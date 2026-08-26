/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server.ursa.sdt;

import kafka.cluster.Partition;
import kafka.server.KafkaConfig;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.metadata.ConfigRepository;
import org.apache.kafka.storage.diskless.handlers.KafkaLogNaming;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.streamnative.ursa.compaction.CompactionManager;
import io.streamnative.ursa.compaction.DynamicConfigs;
import io.streamnative.ursa.compaction.task.PreparedCompactStreamTask;
import scala.Option;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UrsaSDTInterceptorTest {

    @Test
    void shouldCoalesceDisklessHighWatermarksAndReplacePublisherWhenLogIdChanges() {
        var clusterConfig = mock(KafkaConfig.class);
        var topicConfigRepository = mock(ConfigRepository.class);
        var compactionManager = mock(CompactionManager.class);
        var executor = mock(ScheduledExecutorService.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> firstScheduledFuture = mock(ScheduledFuture.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> secondScheduledFuture = mock(ScheduledFuture.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");
        var topicIdPartition = new TopicIdPartition(
                Uuid.randomUuid(), new TopicPartition("diskless-topic", 0));

        doReturn(properties).when(clusterConfig).props();
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executor).execute(any(Runnable.class));
        doReturn(firstScheduledFuture, secondScheduledFuture).when(executor).scheduleAtFixedRate(
                org.mockito.ArgumentMatchers.any(Runnable.class),
                eq(1L),
                eq(1L),
                eq(TimeUnit.SECONDS));
        var interceptor = new UrsaSDTInterceptor(
                clusterConfig,
                topicConfigRepository,
                executor,
                null,
                compactionManager);

        interceptor.onDisklessAppend(topicIdPartition, 11L, 10L);
        interceptor.onDisklessAppend(topicIdPartition, 11L, 20L);
        interceptor.onDisklessAppend(topicIdPartition, 22L, 5L);

        verify(executor, times(2)).scheduleAtFixedRate(
                org.mockito.ArgumentMatchers.any(Runnable.class),
                eq(1L),
                eq(1L),
                eq(TimeUnit.SECONDS));
        verify(firstScheduledFuture).cancel(false);
    }

    @Test
    void shouldFenceOldPublisherWhenLogIdChangesBeforeTaskPublication() throws Exception {
        var clusterConfig = mock(KafkaConfig.class);
        var topicConfigRepository = mock(ConfigRepository.class);
        var compactionManager = mock(CompactionManager.class);
        var configs = mock(DynamicConfigs.class);
        var executor = mock(ScheduledExecutorService.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> firstScheduledFuture = mock(ScheduledFuture.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> secondScheduledFuture = mock(ScheduledFuture.class);
        var scheduledRunnable = ArgumentCaptor.forClass(Runnable.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");
        var topicIdPartition = new TopicIdPartition(
                Uuid.randomUuid(), new TopicPartition("diskless-race-topic", 0));
        var configLookupStarted = new CountDownLatch(1);
        var releaseConfigLookup = new CountDownLatch(1);
        var configLookups = new AtomicInteger();

        doReturn(properties).when(clusterConfig).props();
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executor).execute(any(Runnable.class));
        doReturn(firstScheduledFuture, secondScheduledFuture).when(executor).scheduleAtFixedRate(
                scheduledRunnable.capture(),
                eq(1L),
                eq(1L),
                eq(TimeUnit.SECONDS));
        when(configs.sdtEnabled()).thenReturn(true);
        when(configs.sdtSuspended()).thenReturn(false);
        when(configs.toTaskProperties()).thenReturn(Map.of());
        when(configs.getProperty("clusterTailCompactDataVisibilityIntervalInSeconds"))
                .thenReturn(Optional.empty());
        var interceptor = spy(new UrsaSDTInterceptor(
                clusterConfig,
                topicConfigRepository,
                executor,
                null,
                compactionManager));
        doAnswer(invocation -> {
            if (configLookups.incrementAndGet() != 2) {
                return configs;
            }
            configLookupStarted.countDown();
            assertTrue(releaseConfigLookup.await(10, TimeUnit.SECONDS));
            return configs;
        }).when(interceptor).getDynamicConfigs("diskless-race-topic");

        interceptor.onDisklessAppend(topicIdPartition, 11L, 10L);
        Runnable oldPublisherRun = scheduledRunnable.getValue();
        CompletableFuture<Void> oldPublish = CompletableFuture.runAsync(oldPublisherRun);
        try {
            assertTrue(configLookupStarted.await(10, TimeUnit.SECONDS));

            interceptor.onDisklessAppend(topicIdPartition, 22L, 5L);
            verify(firstScheduledFuture).cancel(false);
        } finally {
            releaseConfigLookup.countDown();
        }
        oldPublish.get(10, TimeUnit.SECONDS);

        verify(compactionManager, never()).publishTask(any());
    }

    @Test
    void shouldRejectOldIncarnationEventsAfterOwnershipLossAndRecreation() throws Exception {
        var clusterConfig = mock(KafkaConfig.class);
        var topicConfigRepository = mock(ConfigRepository.class);
        var compactionManager = mock(CompactionManager.class);
        var configs = mock(DynamicConfigs.class);
        var executor = mock(ScheduledExecutorService.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        var updateCaptor = ArgumentCaptor.forClass(Runnable.class);
        var publishCaptor = ArgumentCaptor.forClass(Runnable.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");
        var oldPartition = new TopicIdPartition(
                Uuid.randomUuid(), new TopicPartition("recreated-topic", 0));
        var newPartition = new TopicIdPartition(
                Uuid.randomUuid(), new TopicPartition("recreated-topic", 0));

        doReturn(properties).when(clusterConfig).props();
        doReturn(scheduledFuture).when(executor).scheduleAtFixedRate(
                publishCaptor.capture(), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        doAnswer(invocation -> null).when(executor).execute(updateCaptor.capture());
        when(configs.sdtEnabled()).thenReturn(true);
        when(configs.sdtSuspended()).thenReturn(false);
        when(configs.toTaskProperties()).thenReturn(Map.of());
        when(configs.getProperty("clusterTailCompactDataVisibilityIntervalInSeconds"))
                .thenReturn(Optional.empty());
        var publication = publication(
                compactionManager, KafkaLogNaming.logName(newPartition), 22L, -1L);
        var interceptor = spy(new UrsaSDTInterceptor(
                clusterConfig,
                topicConfigRepository,
                executor,
                null,
                compactionManager));
        doReturn(configs).when(interceptor).getDynamicConfigs("recreated-topic");

        interceptor.onDisklessAppend(oldPartition, 11L, 10L);
        interceptor.onPartitionOwnershipLost(oldPartition);
        interceptor.onDisklessAppend(newPartition, 22L, 7L);

        // Executor delivery is deliberately inverted: the new incarnation installs first.
        updateCaptor.getAllValues().get(1).run();
        updateCaptor.getAllValues().get(0).run();

        // A callback from the old UUID arriving even later must not replace or close the new one.
        interceptor.onDisklessAppend(oldPartition, 11L, 20L);
        verify(executor, times(2)).execute(any(Runnable.class));
        verify(executor).scheduleAtFixedRate(any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        verify(scheduledFuture, never()).cancel(false);

        publishCaptor.getValue().run();
        assertEquals(22L, publication.tasks().get(0).getStreamId());
        assertEquals(7L, publication.tasks().get(0).getEndOffset());
    }

    @Test
    void shouldAcceptAuthoritativeNewIncarnationBeforeOldLossCallback() throws Exception {
        var clusterConfig = mock(KafkaConfig.class);
        var topicConfigRepository = mock(ConfigRepository.class);
        var compactionManager = mock(CompactionManager.class);
        var configs = mock(DynamicConfigs.class);
        var executor = mock(ScheduledExecutorService.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        var updateCaptor = ArgumentCaptor.forClass(Runnable.class);
        var publishCaptor = ArgumentCaptor.forClass(Runnable.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");
        var oldPartition = new TopicIdPartition(
                Uuid.randomUuid(), new TopicPartition("authoritative-recreated-topic", 0));
        var newPartition = new TopicIdPartition(
                Uuid.randomUuid(), new TopicPartition("authoritative-recreated-topic", 0));

        doReturn(properties).when(clusterConfig).props();
        doReturn(scheduledFuture).when(executor).scheduleAtFixedRate(
                publishCaptor.capture(), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        doAnswer(invocation -> null).when(executor).execute(updateCaptor.capture());
        when(configs.sdtEnabled()).thenReturn(true);
        when(configs.sdtSuspended()).thenReturn(false);
        when(configs.toTaskProperties()).thenReturn(Map.of());
        when(configs.getProperty("clusterTailCompactDataVisibilityIntervalInSeconds"))
                .thenReturn(Optional.empty());
        var publication = publication(
                compactionManager, KafkaLogNaming.logName(newPartition), 22L, -1L);
        var interceptor = spy(new UrsaSDTInterceptor(
                clusterConfig,
                topicConfigRepository,
                executor,
                null,
                compactionManager));
        doReturn(configs).when(interceptor).getDynamicConfigs("authoritative-recreated-topic");

        interceptor.onDisklessAppend(oldPartition, 11L, 10L, 1L);
        interceptor.onDisklessAppend(newPartition, 22L, 7L, 2L);

        // The new metadata image is authoritative before the reconciler reports old ownership loss.
        updateCaptor.getAllValues().get(1).run();
        updateCaptor.getAllValues().get(0).run();
        interceptor.onPartitionOwnershipLost(oldPartition, 3L);
        interceptor.onDisklessAppend(oldPartition, 11L, 20L, 1L);

        verify(executor, times(2)).execute(any(Runnable.class));
        verify(executor).scheduleAtFixedRate(
                any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        verify(scheduledFuture, never()).cancel(false);

        publishCaptor.getValue().run();
        assertEquals(22L, publication.tasks().get(0).getStreamId());
        assertEquals(7L, publication.tasks().get(0).getEndOffset());
    }

    @Test
    void shouldLetNewClassicIncarnationFenceLateOldDisklessAppend() {
        var clusterConfig = mock(KafkaConfig.class);
        var executor = mock(ScheduledExecutorService.class);
        var partition = mock(Partition.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> oldDisklessFuture = mock(ScheduledFuture.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> newClassicFuture = mock(ScheduledFuture.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");
        var oldPartition = new TopicIdPartition(
                Uuid.randomUuid(), new TopicPartition("diskless-to-classic-recreation", 0));
        var newTopicId = Uuid.randomUuid();

        doReturn(properties).when(clusterConfig).props();
        when(partition.topic()).thenReturn("diskless-to-classic-recreation");
        when(partition.partitionId()).thenReturn(0);
        when(partition.topicId()).thenReturn(Option.apply(newTopicId));
        when(partition.isLeader()).thenReturn(true);
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executor).execute(any(Runnable.class));
        doReturn(oldDisklessFuture, newClassicFuture).when(executor).scheduleAtFixedRate(
                any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        var interceptor = new UrsaSDTInterceptor(
                clusterConfig,
                mock(ConfigRepository.class),
                executor,
                null,
                mock(CompactionManager.class));

        interceptor.onDisklessAppend(oldPartition, 11L, 10L, 1L);
        interceptor.onLeadershipAcquired(partition, 2L);
        interceptor.onDisklessAppend(oldPartition, 11L, 20L, 1L);

        verify(executor, times(2)).execute(any(Runnable.class));
        verify(executor, times(2)).scheduleAtFixedRate(
                any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        verify(oldDisklessFuture).cancel(false);
        verify(newClassicFuture, never()).cancel(false);
    }

    @Test
    void shouldPublishExistingClassicTailAfterLeadershipAcquisitionWithoutAppend() throws Exception {
        var clusterConfig = mock(KafkaConfig.class);
        var compactionManager = mock(CompactionManager.class);
        var configs = mock(DynamicConfigs.class);
        var executor = mock(ScheduledExecutorService.class);
        var partition = mock(Partition.class);
        var unifiedLog = mock(UnifiedLog.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        var publishCaptor = ArgumentCaptor.forClass(Runnable.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");
        var topicId = Uuid.randomUuid();
        var topicIdPartition = new TopicIdPartition(
                topicId, new TopicPartition("leadership-tail-topic", 0));

        doReturn(properties).when(clusterConfig).props();
        when(partition.topic()).thenReturn(topicIdPartition.topic());
        when(partition.partitionId()).thenReturn(topicIdPartition.partition());
        when(partition.topicId()).thenReturn(Option.apply(topicId));
        when(partition.isLeader()).thenReturn(true);
        when(partition.unifiedLog()).thenReturn(Optional.of(unifiedLog));
        when(unifiedLog.highWatermark()).thenReturn(12L);
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executor).execute(any(Runnable.class));
        doReturn(scheduledFuture).when(executor).scheduleAtFixedRate(
                publishCaptor.capture(), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        when(configs.sdtEnabled()).thenReturn(true);
        when(configs.sdtSuspended()).thenReturn(false);
        when(configs.toTaskProperties()).thenReturn(Map.of());
        when(configs.getProperty("clusterTailCompactDataVisibilityIntervalInSeconds"))
                .thenReturn(Optional.empty());
        long sourceId = TopicCompactionTaskPublisher.kafkaSourceId(topicId, 0);
        var publication = publication(
                compactionManager, KafkaLogNaming.logName(topicIdPartition), sourceId, 4L);
        var interceptor = spy(new UrsaSDTInterceptor(
                clusterConfig,
                mock(ConfigRepository.class),
                executor,
                null,
                compactionManager));
        doReturn(configs).when(interceptor).getDynamicConfigs(topicIdPartition.topic());

        interceptor.onLeadershipAcquired(partition, 7L);
        publishCaptor.getValue().run();

        assertEquals(1, publication.tasks().size());
        assertEquals(5L, publication.tasks().get(0).getStartOffset());
        assertEquals(12L, publication.tasks().get(0).getEndOffset());
        assertEquals(KafkaLogNaming.logName(topicIdPartition), publication.tasks().get(0).getTopic());
        assertEquals(topicId.toString(),
                publication.tasks().get(0).getProperties().get("sourceTopicId"));
    }

    @Test
    void shouldFenceClassicPublicationImmediatelyWhenLeadershipIsLost() throws Exception {
        var clusterConfig = mock(KafkaConfig.class);
        var compactionManager = mock(CompactionManager.class);
        var configs = mock(DynamicConfigs.class);
        var executor = mock(ScheduledExecutorService.class);
        var partition = mock(Partition.class);
        var unifiedLog = mock(UnifiedLog.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> cleanupFuture = mock(ScheduledFuture.class);
        var publishCaptor = ArgumentCaptor.forClass(Runnable.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");
        var topicId = Uuid.randomUuid();
        var topicIdPartition = new TopicIdPartition(
                topicId, new TopicPartition("leadership-lost-topic", 0));

        doReturn(properties).when(clusterConfig).props();
        when(partition.topic()).thenReturn(topicIdPartition.topic());
        when(partition.partitionId()).thenReturn(topicIdPartition.partition());
        when(partition.topicId()).thenReturn(Option.apply(topicId));
        when(partition.isLeader()).thenReturn(true);
        when(partition.unifiedLog()).thenReturn(Optional.of(unifiedLog));
        when(unifiedLog.highWatermark()).thenReturn(12L);
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executor).execute(any(Runnable.class));
        doReturn(scheduledFuture).when(executor).scheduleAtFixedRate(
                publishCaptor.capture(), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        doReturn(cleanupFuture).when(executor).schedule(
                any(Runnable.class), eq(5L), eq(TimeUnit.MINUTES));
        when(configs.sdtEnabled()).thenReturn(true);
        when(configs.sdtSuspended()).thenReturn(false);
        when(configs.toTaskProperties()).thenReturn(Map.of());
        when(configs.getProperty("clusterTailCompactDataVisibilityIntervalInSeconds"))
                .thenReturn(Optional.empty());
        long sourceId = TopicCompactionTaskPublisher.kafkaSourceId(topicId, 0);
        var publication = publication(
                compactionManager, KafkaLogNaming.logName(topicIdPartition), sourceId, 4L);
        var interceptor = spy(new UrsaSDTInterceptor(
                clusterConfig,
                mock(ConfigRepository.class),
                executor,
                null,
                compactionManager));
        doReturn(configs).when(interceptor).getDynamicConfigs(topicIdPartition.topic());

        interceptor.onLeadershipAcquired(partition, 7L);
        publishCaptor.getValue().run();
        interceptor.onLeadershipLost(topicIdPartition);

        verify(scheduledFuture).cancel(false);
        verify(publication.session()).fence();
        verify(publication.session()).close();
        assertFalse(interceptor.hasClassicPublisher("leadership-lost-topic-partition-0"));
    }

    @Test
    void shouldNotFenceNewIncarnationOrDisklessOwnerOnDelayedClassicLeadershipLoss() {
        var clusterConfig = mock(KafkaConfig.class);
        var executor = mock(ScheduledExecutorService.class);
        var partition = mock(Partition.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> classicFuture = mock(ScheduledFuture.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> disklessFuture = mock(ScheduledFuture.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");
        var oldPartition = new TopicIdPartition(
                Uuid.randomUuid(), new TopicPartition("delayed-leadership-loss-topic", 0));
        var currentPartition = new TopicIdPartition(
                Uuid.randomUuid(), oldPartition.topicPartition());

        doReturn(properties).when(clusterConfig).props();
        when(partition.topic()).thenReturn(currentPartition.topic());
        when(partition.partitionId()).thenReturn(currentPartition.partition());
        when(partition.topicId()).thenReturn(Option.apply(currentPartition.topicId()));
        when(partition.isLeader()).thenReturn(true);
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executor).execute(any(Runnable.class));
        doReturn(classicFuture, disklessFuture).when(executor).scheduleAtFixedRate(
                any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        var interceptor = new UrsaSDTInterceptor(
                clusterConfig,
                mock(ConfigRepository.class),
                executor,
                null,
                mock(CompactionManager.class));

        interceptor.onLeadershipAcquired(partition, 2L);
        interceptor.onLeadershipLost(oldPartition);

        assertTrue(interceptor.hasClassicPublisher("delayed-leadership-loss-topic-partition-0"));
        verify(classicFuture, never()).cancel(false);

        interceptor.onDisklessAppend(currentPartition, 73L, 9L, 3L);
        interceptor.onLeadershipLost(currentPartition);

        assertTrue(interceptor.hasDisklessPublisher("delayed-leadership-loss-topic-partition-0"));
        verify(disklessFuture, never()).cancel(false);
    }

    @Test
    void shouldDiscardQueuedLeadershipEventWhenPartitionIncarnationChanges() {
        var clusterConfig = mock(KafkaConfig.class);
        var executor = mock(ScheduledExecutorService.class);
        var partition = mock(Partition.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> newIncarnationFuture = mock(ScheduledFuture.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> replacementFuture = mock(ScheduledFuture.class);
        var updateCaptor = ArgumentCaptor.forClass(Runnable.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");
        var oldTopicId = Uuid.randomUuid();
        var newTopicId = Uuid.randomUuid();
        var currentTopicId = new AtomicReference<>(oldTopicId);
        var publisherKey = "queued-incarnation-topic-partition-0";

        doReturn(properties).when(clusterConfig).props();
        when(partition.topic()).thenReturn("queued-incarnation-topic");
        when(partition.partitionId()).thenReturn(0);
        when(partition.topicId()).thenAnswer(invocation -> Option.apply(currentTopicId.get()));
        when(partition.isLeader()).thenReturn(true);
        doAnswer(invocation -> null).when(executor).execute(updateCaptor.capture());
        doReturn(newIncarnationFuture, replacementFuture).when(executor).scheduleAtFixedRate(
                any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        var interceptor = new UrsaSDTInterceptor(
                clusterConfig,
                mock(ConfigRepository.class),
                executor,
                null,
                mock(CompactionManager.class));

        interceptor.onLeadershipAcquired(partition, 1L);
        currentTopicId.set(newTopicId);
        updateCaptor.getValue().run();

        assertFalse(interceptor.hasClassicPublisher(publisherKey));
        assertFalse(interceptor.hasPublisherEvent(publisherKey));
        verify(executor, never()).scheduleAtFixedRate(
                any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));

        interceptor.onLeadershipAcquired(partition, 2L);
        updateCaptor.getAllValues().get(1).run();

        assertTrue(interceptor.hasClassicPublisher(publisherKey));
        verify(executor).scheduleAtFixedRate(
                any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));

        // Selecting another UUID must fence the installed publisher before its async
        // replacement update is delivered.
        currentTopicId.set(Uuid.randomUuid());
        interceptor.onLeadershipAcquired(partition, 3L);
        verify(newIncarnationFuture).cancel(false);

        updateCaptor.getAllValues().get(2).run();
        verify(executor, times(2)).scheduleAtFixedRate(
                any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        verify(replacementFuture, never()).cancel(false);
    }

    @Test
    void shouldIgnoreLeadershipAcquisitionWhenPartitionIsNotLeader() {
        var clusterConfig = mock(KafkaConfig.class);
        var executor = mock(ScheduledExecutorService.class);
        var partition = mock(Partition.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");

        doReturn(properties).when(clusterConfig).props();
        when(partition.topic()).thenReturn("follower-topic");
        when(partition.partitionId()).thenReturn(0);
        when(partition.topicId()).thenReturn(Option.apply(Uuid.randomUuid()));
        when(partition.isLeader()).thenReturn(false);
        var interceptor = new UrsaSDTInterceptor(
                clusterConfig,
                mock(ConfigRepository.class),
                executor,
                null,
                mock(CompactionManager.class));

        interceptor.onLeadershipAcquired(partition, 1L);

        verify(executor, never()).execute(any(Runnable.class));
        assertFalse(interceptor.hasPublisherEvent("follower-topic-partition-0"));
    }

    @Test
    void shouldDiscardLeadershipEventWhenExecutorRejectsUpdate() {
        var clusterConfig = mock(KafkaConfig.class);
        var executor = mock(ScheduledExecutorService.class);
        var partition = mock(Partition.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");

        doReturn(properties).when(clusterConfig).props();
        when(partition.topic()).thenReturn("rejected-leadership-topic");
        when(partition.partitionId()).thenReturn(0);
        when(partition.topicId()).thenReturn(Option.apply(Uuid.randomUuid()));
        when(partition.isLeader()).thenReturn(true);
        doThrow(new RejectedExecutionException("executor stopped"))
                .when(executor).execute(any(Runnable.class));
        var interceptor = new UrsaSDTInterceptor(
                clusterConfig,
                mock(ConfigRepository.class),
                executor,
                null,
                mock(CompactionManager.class));

        interceptor.onLeadershipAcquired(partition, 1L);

        assertFalse(interceptor.hasClassicPublisher("rejected-leadership-topic-partition-0"));
        assertFalse(interceptor.hasPublisherEvent("rejected-leadership-topic-partition-0"));
    }

    @Test
    void shouldDiscardClassicPublisherWhenPeriodicSchedulingIsRejected() {
        var clusterConfig = mock(KafkaConfig.class);
        var executor = mock(ScheduledExecutorService.class);
        var partition = mock(Partition.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");

        doReturn(properties).when(clusterConfig).props();
        when(partition.topic()).thenReturn("rejected-periodic-topic");
        when(partition.partitionId()).thenReturn(0);
        when(partition.topicId()).thenReturn(Option.apply(Uuid.randomUuid()));
        when(partition.isLeader()).thenReturn(true);
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executor).execute(any(Runnable.class));
        doThrow(new RejectedExecutionException("executor shutting down"))
                .when(executor).scheduleAtFixedRate(
                        any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        var interceptor = new UrsaSDTInterceptor(
                clusterConfig,
                mock(ConfigRepository.class),
                executor,
                null,
                mock(CompactionManager.class));

        interceptor.onLeadershipAcquired(partition, 1L);

        assertFalse(interceptor.hasClassicPublisher("rejected-periodic-topic-partition-0"));
        assertFalse(interceptor.hasPublisherEvent("rejected-periodic-topic-partition-0"));
    }

    @Test
    void shouldFenceStaleSameIncarnationAppendAcrossOwnershipReacquisition() {
        var clusterConfig = mock(KafkaConfig.class);
        var topicConfigRepository = mock(ConfigRepository.class);
        var compactionManager = mock(CompactionManager.class);
        var executor = mock(ScheduledExecutorService.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> firstFuture = mock(ScheduledFuture.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> reacquiredFuture = mock(ScheduledFuture.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");
        var topicIdPartition = new TopicIdPartition(
                Uuid.randomUuid(), new TopicPartition("reacquired-topic", 0));

        doReturn(properties).when(clusterConfig).props();
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executor).execute(any(Runnable.class));
        doReturn(firstFuture, reacquiredFuture).when(executor).scheduleAtFixedRate(
                any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        var interceptor = new UrsaSDTInterceptor(
                clusterConfig,
                topicConfigRepository,
                executor,
                null,
                compactionManager);

        interceptor.onDisklessAppend(topicIdPartition, 11L, 10L, 1L);
        interceptor.onPartitionOwnershipLost(topicIdPartition, 2L);
        interceptor.onDisklessAppend(topicIdPartition, 11L, 20L, 1L);
        interceptor.onDisklessAppend(topicIdPartition, 22L, 7L, 3L);

        verify(executor, times(2)).execute(any(Runnable.class));
        verify(executor, times(2)).scheduleAtFixedRate(
                any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        verify(firstFuture).cancel(false);
        verify(reacquiredFuture, never()).cancel(false);
    }

    @Test
    void shouldIgnoreLateOwnershipLossAfterSameIncarnationReacquisition() {
        var clusterConfig = mock(KafkaConfig.class);
        var executor = mock(ScheduledExecutorService.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> firstFuture = mock(ScheduledFuture.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> reacquiredFuture = mock(ScheduledFuture.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");
        var topicIdPartition = new TopicIdPartition(
                Uuid.randomUuid(), new TopicPartition("late-loss-topic", 0));

        doReturn(properties).when(clusterConfig).props();
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executor).execute(any(Runnable.class));
        doReturn(firstFuture, reacquiredFuture).when(executor).scheduleAtFixedRate(
                any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        var interceptor = new UrsaSDTInterceptor(
                clusterConfig,
                mock(ConfigRepository.class),
                executor,
                null,
                mock(CompactionManager.class));

        interceptor.onDisklessAppend(topicIdPartition, 11L, 10L, 1L);
        interceptor.onPartitionOwnershipLost(topicIdPartition, 2L);
        interceptor.onDisklessAppend(topicIdPartition, 22L, 20L, 3L);
        interceptor.onPartitionOwnershipLost(topicIdPartition, 2L);

        verify(executor, times(2)).execute(any(Runnable.class));
        verify(executor, times(2)).scheduleAtFixedRate(
                any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        verify(firstFuture).cancel(false);
        verify(reacquiredFuture, never()).cancel(false);
    }

    @Test
    void shouldKeepOwnershipLossTombstoneAtHighestGeneration() {
        var clusterConfig = mock(KafkaConfig.class);
        var executor = mock(ScheduledExecutorService.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");
        var topicIdPartition = new TopicIdPartition(
                Uuid.randomUuid(), new TopicPartition("loss-generation-topic", 0));

        doReturn(properties).when(clusterConfig).props();
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executor).execute(any(Runnable.class));
        doReturn(scheduledFuture).when(executor).scheduleAtFixedRate(
                any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        var interceptor = new UrsaSDTInterceptor(
                clusterConfig,
                mock(ConfigRepository.class),
                executor,
                null,
                mock(CompactionManager.class));

        interceptor.onDisklessAppend(topicIdPartition, 11L, 10L, 1L);
        interceptor.onPartitionOwnershipLost(topicIdPartition, 4L);
        interceptor.onPartitionOwnershipLost(topicIdPartition, 2L);
        interceptor.onDisklessAppend(topicIdPartition, 22L, 20L, 3L);

        verify(executor).execute(any(Runnable.class));
        verify(executor).scheduleAtFixedRate(
                any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        verify(scheduledFuture).cancel(false);
        assertFalse(interceptor.hasDisklessPublisher("loss-generation-topic-partition-0"));
    }

    @Test
    void shouldSerializeEventSelectionAndFencingAcrossModeSwitch() throws Exception {
        var clusterConfig = mock(KafkaConfig.class);
        var executor = mock(ScheduledExecutorService.class);
        var partition = mock(Partition.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> firstFuture = mock(ScheduledFuture.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> secondFuture = mock(ScheduledFuture.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");
        var topicId = Uuid.randomUuid();
        var topicIdPartition = new TopicIdPartition(
                topicId, new TopicPartition("serialized-switch-topic", 0));
        var firstEventSelected = new CountDownLatch(1);
        var releaseFirstEvent = new CountDownLatch(1);
        var blockFirstEvent = new AtomicBoolean(true);

        doReturn(properties).when(clusterConfig).props();
        when(partition.topic()).thenReturn("serialized-switch-topic");
        when(partition.partitionId()).thenReturn(0);
        when(partition.topicId()).thenReturn(Option.apply(topicId));
        when(partition.isLeader()).thenReturn(true);
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executor).execute(any(Runnable.class));
        doReturn(firstFuture, secondFuture).when(executor).scheduleAtFixedRate(
                any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        var interceptor = spy(new UrsaSDTInterceptor(
                clusterConfig,
                mock(ConfigRepository.class),
                executor,
                null,
                mock(CompactionManager.class)));
        doAnswer(invocation -> {
            if (blockFirstEvent.compareAndSet(true, false)) {
                firstEventSelected.countDown();
                assertTrue(releaseFirstEvent.await(10, TimeUnit.SECONDS));
            }
            return null;
        }).when(interceptor).beforePublisherFence("serialized-switch-topic-partition-0");

        CompletableFuture<Void> classic = CompletableFuture.runAsync(
                () -> interceptor.onAppend(null, null, partition, 1L));
        assertTrue(firstEventSelected.await(10, TimeUnit.SECONDS));
        CompletableFuture<Void> diskless = CompletableFuture.runAsync(
                () -> interceptor.onDisklessAppend(topicIdPartition, 73L, 10L, 2L));
        releaseFirstEvent.countDown();
        classic.get(10, TimeUnit.SECONDS);
        diskless.get(10, TimeUnit.SECONDS);

        assertFalse(interceptor.hasClassicPublisher("serialized-switch-topic-partition-0"));
        assertTrue(interceptor.hasDisklessPublisher("serialized-switch-topic-partition-0"));
        verify(executor).scheduleAtFixedRate(
                any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
    }

    @Test
    void shouldFenceBothDirectionsWhenPartitionSwitchesStorageMode() throws Exception {
        var clusterConfig = mock(KafkaConfig.class);
        var topicConfigRepository = mock(ConfigRepository.class);
        var compactionManager = mock(CompactionManager.class);
        var configs = mock(DynamicConfigs.class);
        var executor = mock(ScheduledExecutorService.class);
        var partition = mock(Partition.class);
        var unifiedLog = mock(UnifiedLog.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> firstClassicFuture = mock(ScheduledFuture.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> disklessFuture = mock(ScheduledFuture.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> secondClassicFuture = mock(ScheduledFuture.class);
        var publishCaptor = ArgumentCaptor.forClass(Runnable.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");
        var topicId = Uuid.randomUuid();
        var topicIdPartition = new TopicIdPartition(
                topicId, new TopicPartition("mode-switch-topic", 0));

        doReturn(properties).when(clusterConfig).props();
        when(partition.topic()).thenReturn("mode-switch-topic");
        when(partition.partitionId()).thenReturn(0);
        when(partition.topicId()).thenReturn(Option.apply(topicId));
        when(partition.isLeader()).thenReturn(true);
        when(partition.unifiedLog()).thenReturn(Optional.of(unifiedLog));
        when(unifiedLog.highWatermark()).thenReturn(9L);
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executor).execute(any(Runnable.class));
        doReturn(firstClassicFuture, disklessFuture, secondClassicFuture)
                .when(executor).scheduleAtFixedRate(
                        publishCaptor.capture(), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        when(configs.sdtEnabled()).thenReturn(true);
        when(configs.sdtSuspended()).thenReturn(false);
        when(configs.toTaskProperties()).thenReturn(Map.of());
        when(configs.getProperty("clusterTailCompactDataVisibilityIntervalInSeconds"))
                .thenReturn(Optional.empty());
        var disklessPublication = publication(
                compactionManager, KafkaLogNaming.logName(topicIdPartition), 73L, -1L);
        long classicStreamId = TopicCompactionTaskPublisher.kafkaSourceId(topicId, 0);
        var classicPublication = publication(
                compactionManager, KafkaLogNaming.logName(topicIdPartition), classicStreamId, -1L);
        var interceptor = spy(new UrsaSDTInterceptor(
                clusterConfig,
                topicConfigRepository,
                executor,
                null,
                compactionManager));
        doReturn(configs).when(interceptor).getDynamicConfigs("mode-switch-topic");

        interceptor.onAppend(null, null, partition, 1L);
        Runnable staleClassic = publishCaptor.getAllValues().get(0);
        interceptor.onDisklessAppend(topicIdPartition, 73L, 9L, 2L);
        Runnable diskless = publishCaptor.getAllValues().get(1);

        staleClassic.run();
        diskless.run();

        interceptor.onAppend(null, null, partition, 3L);
        Runnable currentClassic = publishCaptor.getAllValues().get(2);
        interceptor.onDisklessAppend(topicIdPartition, 73L, 10L, 2L);
        diskless.run();
        currentClassic.run();

        verify(firstClassicFuture, atLeastOnce()).cancel(false);
        verify(disklessFuture, atLeastOnce()).cancel(false);
        assertEquals("URSA", disklessPublication.tasks().get(0).getProperties().get("entryFormat"));
        assertEquals(73L, disklessPublication.tasks().get(0).getStreamId());
        assertEquals("KAFKA", classicPublication.tasks().get(0).getProperties().get("entryFormat"));
        assertEquals(classicStreamId, classicPublication.tasks().get(0).getStreamId());
    }

    @Test
    void shouldFenceOwnershipLossWithoutWaitingForBlockedRemoteIo() throws Exception {
        var clusterConfig = mock(KafkaConfig.class);
        var topicConfigRepository = mock(ConfigRepository.class);
        var compactionManager = mock(CompactionManager.class);
        var configs = mock(DynamicConfigs.class);
        var executor = mock(ScheduledExecutorService.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        var publishCaptor = ArgumentCaptor.forClass(Runnable.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");
        var topicIdPartition = new TopicIdPartition(
                Uuid.randomUuid(), new TopicPartition("ownership-topic", 0));
        var remoteCallStarted = new CountDownLatch(1);
        var releaseRemoteCall = new CountDownLatch(1);

        doReturn(properties).when(clusterConfig).props();
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executor).execute(any(Runnable.class));
        doReturn(scheduledFuture).when(executor).scheduleAtFixedRate(
                publishCaptor.capture(), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        when(configs.sdtEnabled()).thenReturn(true);
        when(configs.sdtSuspended()).thenReturn(false);
        when(configs.toTaskProperties()).thenReturn(Map.of());
        when(configs.getProperty("clusterTailCompactDataVisibilityIntervalInSeconds"))
                .thenReturn(Optional.empty());
        var session = mock(CompactionManager.PublicationSession.class);
        when(compactionManager.tryOpenPublicationSession(KafkaLogNaming.logName(topicIdPartition), 73L))
                .thenReturn(Optional.of(session));
        doAnswer(invocation -> {
            remoteCallStarted.countDown();
            assertTrue(releaseRemoteCall.await(10, TimeUnit.SECONDS));
            return CompactionManager.PublicationResult.NO_TASK;
        }).when(session).publishNext(any());
        var interceptor = spy(new UrsaSDTInterceptor(
                clusterConfig,
                topicConfigRepository,
                executor,
                null,
                compactionManager));
        doReturn(configs).when(interceptor).getDynamicConfigs("ownership-topic");

        interceptor.onDisklessAppend(topicIdPartition, 73L, 10L);
        CompletableFuture<Void> publication = CompletableFuture.runAsync(publishCaptor.getValue());
        try {
            assertTrue(remoteCallStarted.await(10, TimeUnit.SECONDS));

            CompletableFuture<Void> ownershipLoss = CompletableFuture.runAsync(
                    () -> interceptor.onPartitionOwnershipLost(topicIdPartition));
            ownershipLoss.get(1, TimeUnit.SECONDS);
            verify(scheduledFuture).cancel(false);
        } finally {
            releaseRemoteCall.countDown();
        }
        publication.get(10, TimeUnit.SECONDS);

        verify(session).publishNext(any());
    }

    @Test
    void shouldRejectNonPositivePublisherIntervalDuringInitialization() {
        var clusterConfig = mock(KafkaConfig.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "0");
        doReturn(properties).when(clusterConfig).props();

        var error = assertThrows(IllegalArgumentException.class, () -> new UrsaSDTInterceptor(
                clusterConfig,
                mock(ConfigRepository.class),
                mock(ScheduledExecutorService.class),
                null,
                mock(CompactionManager.class)));

        assertEquals("clusterTailCompactDataVisibilityIntervalInSeconds must be greater than zero",
                error.getMessage());
    }

    @Test
    void shouldFencePublicationSessionEvenWhenAsyncCloseIsRejected() throws Exception {
        var clusterConfig = mock(KafkaConfig.class);
        var executor = mock(ScheduledExecutorService.class);
        var session = mock(CompactionManager.PublicationSession.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");

        doReturn(properties).when(clusterConfig).props();
        doReturn("rejected-close-topic").when(session).topicName();
        doAnswer(invocation -> {
            throw new RejectedExecutionException("executor closed");
        }).when(executor).execute(any(Runnable.class));
        var interceptor = new UrsaSDTInterceptor(
                clusterConfig,
                mock(ConfigRepository.class),
                executor,
                null,
                mock(CompactionManager.class));

        interceptor.closePublicationSessionAsync(session);

        verify(session).fence();
        verify(session, never()).close();
    }

    @Test
    void shouldRetryTransientPublicationSessionCloseFailure() throws Exception {
        var clusterConfig = mock(KafkaConfig.class);
        var executor = Executors.newSingleThreadScheduledExecutor();
        var session = mock(CompactionManager.PublicationSession.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");

        doReturn(properties).when(clusterConfig).props();
        doReturn("retry-close-topic").when(session).topicName();
        doThrow(new IOException("temporary Oxia failure"))
            .doNothing()
            .when(session).close();
        var interceptor = new UrsaSDTInterceptor(
                clusterConfig,
                mock(ConfigRepository.class),
                executor,
                null,
                mock(CompactionManager.class));

        try {
            interceptor.closePublicationSessionAsync(session);

            verify(session).fence();
            verify(session, timeout(5_000).times(2)).close();
        } finally {
            interceptor.close();
        }
    }

    @Test
    void shouldNotRetainPublishersWhenSdtIsDisabled() {
        var clusterConfig = mock(KafkaConfig.class);
        var topicConfigRepository = mock(ConfigRepository.class);
        var executor = mock(ScheduledExecutorService.class);
        var partition = mock(Partition.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> cleanupFuture = mock(ScheduledFuture.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");

        doReturn(properties).when(clusterConfig).props();
        when(topicConfigRepository.topicConfig(any())).thenReturn(new Properties());
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executor).execute(any(Runnable.class));
        doReturn(cleanupFuture).when(executor).schedule(
                any(Runnable.class), eq(5L), eq(TimeUnit.MINUTES));
        var interceptor = new UrsaSDTInterceptor(
                clusterConfig,
                topicConfigRepository,
                executor,
                null,
                mock(CompactionManager.class));

        for (int index = 0; index < 100; index++) {
            var topicIdPartition = new TopicIdPartition(
                    Uuid.randomUuid(), new TopicPartition("disabled-diskless-" + index, 0));
            interceptor.onDisklessAppend(topicIdPartition, index + 1L, 10L, index + 1L);
            assertFalse(interceptor.hasDisklessPublisher("disabled-diskless-" + index + "-partition-0"));
        }
        var classicTopicId = Uuid.randomUuid();
        when(partition.topic()).thenReturn("disabled-classic");
        when(partition.partitionId()).thenReturn(0);
        when(partition.topicId()).thenReturn(Option.apply(classicTopicId));
        when(partition.isLeader()).thenReturn(true);
        interceptor.onAppend(null, null, partition, 101L);

        assertFalse(interceptor.hasClassicPublisher("disabled-classic-partition-0"));
        verify(executor, never()).scheduleAtFixedRate(
                any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
    }

    @Test
    void shouldReplaceClosedPublisherOnAppend() {
        var clusterConfig = mock(KafkaConfig.class);
        var topicConfigRepository = mock(ConfigRepository.class);
        var compactionManager = mock(CompactionManager.class);
        var executor = mock(ScheduledExecutorService.class);
        var partition = mock(Partition.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> firstScheduledFuture = mock(ScheduledFuture.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> secondScheduledFuture = mock(ScheduledFuture.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> cleanupFuture = mock(ScheduledFuture.class);
        var publishCaptor = ArgumentCaptor.forClass(Runnable.class);
        var cleanupCaptor = ArgumentCaptor.forClass(Runnable.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");

        doReturn(properties).when(clusterConfig).props();
        when(partition.topic()).thenReturn("test-topic");
        when(partition.partitionId()).thenReturn(0);
        when(partition.topicId()).thenReturn(Option.apply(Uuid.randomUuid()));
        var isLeader = new AtomicBoolean(true);
        when(partition.isLeader()).thenAnswer(invocation -> isLeader.get());
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executor).execute(any(Runnable.class));
        doReturn(firstScheduledFuture, secondScheduledFuture).when(executor).scheduleAtFixedRate(
            publishCaptor.capture(),
            eq(1L),
            eq(1L),
            eq(TimeUnit.SECONDS)
        );
        doReturn(cleanupFuture).when(executor).schedule(
            cleanupCaptor.capture(), eq(5L), eq(TimeUnit.MINUTES));

        var interceptor = new UrsaSDTInterceptor(
            clusterConfig,
            topicConfigRepository,
            executor,
            null,
            compactionManager);

        interceptor.onAppend(null, null, partition);
        isLeader.set(false);
        publishCaptor.getValue().run();
        assertFalse(interceptor.hasClassicPublisher("test-topic-partition-0"));
        assertTrue(interceptor.hasPublisherEvent("test-topic-partition-0"));
        cleanupCaptor.getValue().run();
        assertFalse(interceptor.hasPublisherEvent("test-topic-partition-0"));
        isLeader.set(true);
        interceptor.onAppend(null, null, partition);

        verify(executor, times(2)).scheduleAtFixedRate(
            org.mockito.ArgumentMatchers.any(),
            eq(1L),
            eq(1L),
            eq(TimeUnit.SECONDS)
        );
        verify(firstScheduledFuture, atLeastOnce()).cancel(false);
    }

    @Test
    void shouldSkipDefaultInternalTopicsOnAppend() {
        var defaultInternalTopics = List.of(
            "__consumer_offsets",
            "sn.system.__kafka_aliveness",
            "strimzi.cruisecontrol.metrics",
            "strimzi.cruisecontrol.modeltrainingsamples",
            "strimzi.cruisecontrol.partitionmetricsamples"
        );

        for (var internalTopic : defaultInternalTopics) {
            var clusterConfig = mock(KafkaConfig.class);
            var topicConfigRepository = mock(ConfigRepository.class);
            var compactionManager = mock(CompactionManager.class);
            var executor = mock(ScheduledExecutorService.class);
            var partition = mock(Partition.class);
            var properties = new Properties();
            properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
            properties.put("clusterSdtEnabled", "true");

            doReturn(properties).when(clusterConfig).props();
            when(partition.topic()).thenReturn(internalTopic);

            var interceptor = new UrsaSDTInterceptor(
                clusterConfig,
                topicConfigRepository,
                executor,
                null,
                compactionManager);

            interceptor.onAppend(null, null, partition);

            verify(executor, never()).scheduleAtFixedRate(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyLong(),
                org.mockito.ArgumentMatchers.anyLong(),
                org.mockito.ArgumentMatchers.any()
            );
        }
    }

    @Test
    void shouldSkipExtraInternalTopicsOnAppend() {
        var clusterConfig = mock(KafkaConfig.class);
        var topicConfigRepository = mock(ConfigRepository.class);
        var compactionManager = mock(CompactionManager.class);
        var executor = mock(ScheduledExecutorService.class);
        var partition = mock(Partition.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");
        properties.put(UrsaSDTInterceptor.EXTRA_INTERNAL_TOPICS_CONFIG, "my.extra.topic, another.extra.topic");

        doReturn(properties).when(clusterConfig).props();
        when(partition.topic()).thenReturn("my.extra.topic");

        var interceptor = new UrsaSDTInterceptor(
            clusterConfig,
            topicConfigRepository,
            executor,
            null,
            compactionManager);

        interceptor.onAppend(null, null, partition);

        verify(executor, never()).scheduleAtFixedRate(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.any()
        );

        // also verify the second extra topic is skipped
        when(partition.topic()).thenReturn("another.extra.topic");
        interceptor.onAppend(null, null, partition);

        verify(executor, never()).scheduleAtFixedRate(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.any()
        );
    }

    @Test
    void shouldProcessNonInternalTopicOnAppend() {
        var clusterConfig = mock(KafkaConfig.class);
        var topicConfigRepository = mock(ConfigRepository.class);
        var compactionManager = mock(CompactionManager.class);
        var executor = mock(ScheduledExecutorService.class);
        var partition = mock(Partition.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put("clusterSdtEnabled", "true");

        doReturn(properties).when(clusterConfig).props();
        when(partition.topic()).thenReturn("regular-topic");
        when(partition.partitionId()).thenReturn(0);
        when(partition.topicId()).thenReturn(Option.apply(Uuid.randomUuid()));
        when(partition.isLeader()).thenReturn(true);
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executor).execute(any(Runnable.class));
        doReturn(scheduledFuture).when(executor).scheduleAtFixedRate(
            org.mockito.ArgumentMatchers.any(),
            eq(1L),
            eq(1L),
            eq(TimeUnit.SECONDS)
        );

        var interceptor = new UrsaSDTInterceptor(
            clusterConfig,
            topicConfigRepository,
            executor,
            null,
            compactionManager);

        interceptor.onAppend(null, null, partition);

        verify(executor, times(1)).scheduleAtFixedRate(
            org.mockito.ArgumentMatchers.any(),
            eq(1L),
            eq(1L),
            eq(TimeUnit.SECONDS)
        );
    }

    private static PublicationHarness publication(
            CompactionManager compactionManager,
            String topicName,
            long streamId,
            long lastPublishedOffset) throws Exception {
        var session = mock(CompactionManager.PublicationSession.class);
        var tasks = new ArrayList<PreparedCompactStreamTask>();
        when(compactionManager.tryOpenPublicationSession(topicName, streamId))
                .thenReturn(Optional.of(session));
        doAnswer(invocation -> {
            CompactionManager.PublicationTaskFactory factory = invocation.getArgument(0);
            Optional<PreparedCompactStreamTask> task = factory.create(lastPublishedOffset);
            task.ifPresent(tasks::add);
            return task.isPresent()
                    ? CompactionManager.PublicationResult.PUBLISHED
                    : CompactionManager.PublicationResult.NO_TASK;
        }).when(session).publishNext(any());
        return new PublicationHarness(session, tasks);
    }

    private record PublicationHarness(
            CompactionManager.PublicationSession session,
            List<PreparedCompactStreamTask> tasks) {
    }
}
