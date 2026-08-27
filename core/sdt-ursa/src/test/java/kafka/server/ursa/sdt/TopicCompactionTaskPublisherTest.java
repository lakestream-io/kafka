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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.storage.diskless.handlers.KafkaLogNaming;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.lakestream.ursa.compaction.CompactionManager;
import io.lakestream.ursa.compaction.DynamicConfigs;
import io.lakestream.ursa.compaction.PublicationFencedException;
import io.lakestream.ursa.compaction.task.PreparedCompactStreamTask;
import scala.Option;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TopicCompactionTaskPublisherTest {

    @Test
    void shouldPublishEndExclusiveKafkaTaskContract() throws Exception {
        var interceptor = mock(UrsaSDTInterceptor.class);
        var compactionManager = mock(CompactionManager.class);
        var configs = mock(DynamicConfigs.class);
        var partition = mock(Partition.class);
        var unifiedLog = mock(UnifiedLog.class);
        var executor = mock(ScheduledExecutorService.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        var topicId = new Uuid(11L, 29L);
        var expectedStreamId = TopicCompactionTaskPublisher.kafkaSourceId(topicId, 3);
        String taskTopicName = taskTopicName("orders", topicId, 3);
        var publication = publication(compactionManager, taskTopicName, expectedStreamId, -1L, 9L);

        when(partition.topic()).thenReturn("orders");
        when(partition.partitionId()).thenReturn(3);
        when(partition.topicId()).thenReturn(Option.apply(topicId));
        when(partition.isLeader()).thenReturn(true);
        when(partition.unifiedLog()).thenReturn(Optional.of(unifiedLog));
        when(unifiedLog.highWatermark()).thenReturn(10L, 20L);
        when(interceptor.getTopicName(partition)).thenReturn("orders-partition-3");
        when(interceptor.getDynamicConfigs("orders")).thenReturn(configs);
        when(interceptor.brokerId()).thenReturn(7);
        when(configs.sdtEnabled()).thenReturn(true);
        when(configs.sdtSuspended()).thenReturn(false);
        when(configs.toTaskProperties()).thenReturn(Map.of());
        when(configs.getProperty("clusterTailCompactDataVisibilityIntervalInSeconds"))
            .thenReturn(Optional.empty());
        doReturn(scheduledFuture).when(executor).scheduleAtFixedRate(
            org.mockito.ArgumentMatchers.any(Runnable.class),
            eq(1L),
            eq(1L),
            eq(TimeUnit.SECONDS)
        );

        var publisher = new TopicCompactionTaskPublisher(interceptor, compactionManager, partition, executor);
        allowPublication(interceptor, publisher, "orders-partition-3");
        publisher.start(Duration.ofSeconds(1));
        publisher.publish();
        publisher.publish();

        verify(compactionManager).tryOpenPublicationSession(taskTopicName, expectedStreamId);
        verify(publication.session(), times(2)).publishNext(any());
        var firstTask = publication.tasks().get(0);
        assertEquals(expectedStreamId, firstTask.getStreamId());
        assertEquals(0L, firstTask.getStartOffset());
        assertEquals(10L, firstTask.getEndOffset());
        assertEquals(taskTopicName, firstTask.getTopic());
        assertEquals("KAFKA", firstTask.getProperties().get("entryFormat"));
        assertEquals(
            "orders-partition-3",
            firstTask.getProperties().get(TopicCompactionTaskPublisher.SOURCE_TOPIC_PROPERTY));
        assertEquals(
            "orders",
            firstTask.getProperties().get(TopicCompactionTaskPublisher.SOURCE_SCHEMA_TOPIC_PROPERTY));
        assertEquals(
            topicId.toString(),
            firstTask.getProperties().get(TopicCompactionTaskPublisher.SOURCE_TOPIC_ID_PROPERTY));
        assertEquals("7", firstTask.getProperties().get(UrsaSDTInterceptor.PUBLISHER_BROKER_ID_PROPERTY));

        var secondTask = publication.tasks().get(1);
        assertEquals(expectedStreamId, secondTask.getStreamId());
        assertEquals(10L, secondTask.getStartOffset());
        assertEquals(20L, secondTask.getEndOffset());
        assertEquals("KAFKA", secondTask.getProperties().get("entryFormat"));
    }

    @Test
    void shouldUseTopicIncarnationStreamIdWhenTopicIsRecreated() throws Exception {
        var interceptor = mock(UrsaSDTInterceptor.class);
        var compactionManager = mock(CompactionManager.class);
        var configs = mock(DynamicConfigs.class);
        var oldPartition = mock(Partition.class);
        var recreatedPartition = mock(Partition.class);
        var oldLog = mock(UnifiedLog.class);
        var recreatedLog = mock(UnifiedLog.class);
        var executor = mock(ScheduledExecutorService.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        var oldTopicId = new Uuid(101L, 202L);
        var recreatedTopicId = new Uuid(303L, 404L);
        var oldStreamId = TopicCompactionTaskPublisher.kafkaSourceId(oldTopicId, 0);
        var recreatedStreamId = TopicCompactionTaskPublisher.kafkaSourceId(recreatedTopicId, 0);
        String oldTaskTopicName = taskTopicName("orders", oldTopicId, 0);
        String recreatedTaskTopicName = taskTopicName("orders", recreatedTopicId, 0);
        var oldPublication = publication(
            compactionManager, oldTaskTopicName, oldStreamId, -1L);
        var recreatedPublication = publication(
            compactionManager, recreatedTaskTopicName, recreatedStreamId, -1L);

        assertNotEquals(oldStreamId, recreatedStreamId);
        assertNotEquals(oldTaskTopicName, recreatedTaskTopicName);
        when(oldPartition.topic()).thenReturn("orders");
        when(oldPartition.topicId()).thenReturn(Option.apply(oldTopicId));
        when(oldPartition.isLeader()).thenReturn(true);
        when(oldPartition.unifiedLog()).thenReturn(Optional.of(oldLog));
        when(recreatedPartition.topic()).thenReturn("orders");
        when(recreatedPartition.topicId()).thenReturn(Option.apply(recreatedTopicId));
        when(recreatedPartition.isLeader()).thenReturn(true);
        when(recreatedPartition.unifiedLog()).thenReturn(Optional.of(recreatedLog));
        when(oldLog.highWatermark()).thenReturn(10L);
        when(recreatedLog.highWatermark()).thenReturn(10L);
        when(interceptor.getTopicName(oldPartition)).thenReturn("orders-partition-0");
        when(interceptor.getTopicName(recreatedPartition)).thenReturn("orders-partition-0");
        when(interceptor.getDynamicConfigs("orders")).thenReturn(configs);
        when(configs.sdtEnabled()).thenReturn(true);
        when(configs.sdtSuspended()).thenReturn(false);
        when(configs.toTaskProperties()).thenReturn(Map.of());
        when(configs.getProperty("clusterTailCompactDataVisibilityIntervalInSeconds"))
            .thenReturn(Optional.empty());
        doReturn(scheduledFuture).when(executor).scheduleAtFixedRate(
            org.mockito.ArgumentMatchers.any(Runnable.class),
            eq(1L),
            eq(1L),
            eq(TimeUnit.SECONDS)
        );

        var oldPublisher = new TopicCompactionTaskPublisher(
            interceptor, compactionManager, oldPartition, executor);
        allowPublication(interceptor, oldPublisher, "orders-partition-0");
        oldPublisher.start(Duration.ofSeconds(1));
        oldPublisher.publish();
        var recreatedPublisher = new TopicCompactionTaskPublisher(
            interceptor, compactionManager, recreatedPartition, executor);
        allowPublication(interceptor, recreatedPublisher, "orders-partition-0");
        recreatedPublisher.start(Duration.ofSeconds(1));
        recreatedPublisher.publish();

        assertEquals(oldStreamId, oldPublication.tasks().get(0).getStreamId());
        assertEquals(0L, oldPublication.tasks().get(0).getStartOffset());
        assertEquals(10L, oldPublication.tasks().get(0).getEndOffset());
        assertEquals(oldTaskTopicName, oldPublication.tasks().get(0).getTopic());
        assertEquals(recreatedStreamId, recreatedPublication.tasks().get(0).getStreamId());
        assertEquals(0L, recreatedPublication.tasks().get(0).getStartOffset());
        assertEquals(10L, recreatedPublication.tasks().get(0).getEndOffset());
        assertEquals(recreatedTaskTopicName, recreatedPublication.tasks().get(0).getTopic());
    }

    @Test
    void shouldUseDistinctStableKafkaSourceIdsForPartitions() {
        var topicId = new Uuid(11L, 29L);
        var historicalTopicIdentity =
            (topicId.getMostSignificantBits() ^ topicId.getLeastSignificantBits()) & Long.MAX_VALUE;

        assertEquals(historicalTopicIdentity, TopicCompactionTaskPublisher.kafkaSourceId(topicId, 0));
        assertEquals(historicalTopicIdentity ^ 1L, TopicCompactionTaskPublisher.kafkaSourceId(topicId, 1));
        assertNotEquals(
            TopicCompactionTaskPublisher.kafkaSourceId(topicId, 0),
            TopicCompactionTaskPublisher.kafkaSourceId(topicId, 1));
        assertThrows(IllegalArgumentException.class,
            () -> TopicCompactionTaskPublisher.kafkaSourceId(topicId, -1));
    }

    @Test
    void shouldCancelScheduledTaskWhenBrokerIsNotLeader() throws Exception {
        var interceptor = mock(UrsaSDTInterceptor.class);
        var compactionManager = mock(CompactionManager.class);
        var partition = mock(Partition.class);
        var executor = mock(ScheduledExecutorService.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        var publishCaptor = ArgumentCaptor.forClass(Runnable.class);

        when(partition.topic()).thenReturn("test-topic");
        when(partition.partitionId()).thenReturn(0);
        when(partition.topicId()).thenReturn(Option.apply(Uuid.randomUuid()));
        when(partition.isLeader()).thenReturn(false);
        when(interceptor.getTopicName(partition)).thenReturn("test-topic-partition-0");
        doReturn(scheduledFuture).when(executor).scheduleAtFixedRate(
            publishCaptor.capture(),
            eq(1L),
            eq(1L),
            eq(TimeUnit.SECONDS)
        );

        var publisher = new TopicCompactionTaskPublisher(interceptor, compactionManager, partition, executor);
        publisher.start(Duration.ofSeconds(1));

        publishCaptor.getValue().run();

        verify(scheduledFuture).cancel(false);
        verify(partition, never()).unifiedLog();
        verify(compactionManager, never()).tryOpenPublicationSession(anyString(), anyLong());
    }

    @Test
    void shouldIgnoreMalformedDynamicIntervalAndStillPublish() throws Exception {
        var interceptor = mock(UrsaSDTInterceptor.class);
        var compactionManager = mock(CompactionManager.class);
        var configs = mock(DynamicConfigs.class);
        var partition = mock(Partition.class);
        var unifiedLog = mock(UnifiedLog.class);
        var executor = mock(ScheduledExecutorService.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        var topicId = Uuid.randomUuid();
        long streamId = TopicCompactionTaskPublisher.kafkaSourceId(topicId, 0);
        String taskTopicName = taskTopicName("orders", topicId, 0);
        var publication = publication(
            compactionManager, taskTopicName, streamId, -1L);

        when(partition.topic()).thenReturn("orders");
        when(partition.partitionId()).thenReturn(0);
        when(partition.topicId()).thenReturn(Option.apply(topicId));
        when(partition.isLeader()).thenReturn(true);
        when(partition.unifiedLog()).thenReturn(Optional.of(unifiedLog));
        when(unifiedLog.highWatermark()).thenReturn(10L);
        when(interceptor.getTopicName(partition)).thenReturn("orders-partition-0");
        when(interceptor.getDynamicConfigs("orders")).thenReturn(configs);
        when(configs.sdtEnabled()).thenReturn(true);
        when(configs.sdtSuspended()).thenReturn(false);
        when(configs.toTaskProperties()).thenReturn(Map.of());
        when(configs.getProperty("clusterTailCompactDataVisibilityIntervalInSeconds"))
            .thenReturn(Optional.of("not-a-number"));
        doReturn(scheduledFuture).when(executor).scheduleAtFixedRate(
            any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        var publisher = new TopicCompactionTaskPublisher(
            interceptor, compactionManager, partition, executor);
        allowPublication(interceptor, publisher, "orders-partition-0");
        publisher.start(Duration.ofSeconds(1));

        publisher.publish();

        assertEquals(1, publication.tasks().size());
        verify(executor, times(1)).scheduleAtFixedRate(
            any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
    }

    @Test
    void shouldRetryLeaseAcquisitionOnLaterTick() throws Exception {
        var interceptor = mock(UrsaSDTInterceptor.class);
        var compactionManager = mock(CompactionManager.class);
        var configs = mock(DynamicConfigs.class);
        var partition = mock(Partition.class);
        var unifiedLog = mock(UnifiedLog.class);
        var executor = mock(ScheduledExecutorService.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        var topicId = Uuid.randomUuid();
        long streamId = TopicCompactionTaskPublisher.kafkaSourceId(topicId, 0);
        String taskTopicName = taskTopicName("lease-topic", topicId, 0);
        var session = mock(CompactionManager.PublicationSession.class);
        var tasks = new ArrayList<PreparedCompactStreamTask>();

        when(partition.topic()).thenReturn("lease-topic");
        when(partition.partitionId()).thenReturn(0);
        when(partition.topicId()).thenReturn(Option.apply(topicId));
        when(partition.isLeader()).thenReturn(true);
        when(partition.unifiedLog()).thenReturn(Optional.of(unifiedLog));
        when(unifiedLog.highWatermark()).thenReturn(10L);
        when(interceptor.getTopicName(partition)).thenReturn("lease-topic-partition-0");
        when(interceptor.getDynamicConfigs("lease-topic")).thenReturn(configs);
        when(configs.sdtEnabled()).thenReturn(true);
        when(configs.sdtSuspended()).thenReturn(false);
        when(configs.toTaskProperties()).thenReturn(Map.of());
        when(configs.getProperty("clusterTailCompactDataVisibilityIntervalInSeconds"))
            .thenReturn(Optional.empty());
        when(compactionManager.tryOpenPublicationSession(taskTopicName, streamId))
            .thenReturn(Optional.empty(), Optional.of(session));
        answerPublication(session, tasks, -1L);
        doReturn(scheduledFuture).when(executor).scheduleAtFixedRate(
            any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        var publisher = new TopicCompactionTaskPublisher(
            interceptor, compactionManager, partition, executor);
        allowPublication(interceptor, publisher, "lease-topic-partition-0");
        publisher.start(Duration.ofSeconds(1));

        publisher.publish();
        publisher.publish();

        verify(compactionManager, times(2)).tryOpenPublicationSession(
            taskTopicName, streamId);
        verify(session).publishNext(any());
        assertEquals(1, tasks.size());
    }

    @Test
    void shouldTerminatePublisherWhenPublicationSessionIsFenced() throws Exception {
        var interceptor = mock(UrsaSDTInterceptor.class);
        var compactionManager = mock(CompactionManager.class);
        var configs = mock(DynamicConfigs.class);
        var partition = mock(Partition.class);
        var unifiedLog = mock(UnifiedLog.class);
        var executor = mock(ScheduledExecutorService.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        var topicId = Uuid.randomUuid();
        long streamId = TopicCompactionTaskPublisher.kafkaSourceId(topicId, 0);
        String taskTopicName = taskTopicName("fenced-topic", topicId, 0);
        var session = mock(CompactionManager.PublicationSession.class);

        when(partition.topic()).thenReturn("fenced-topic");
        when(partition.partitionId()).thenReturn(0);
        when(partition.topicId()).thenReturn(Option.apply(topicId));
        when(partition.isLeader()).thenReturn(true);
        when(partition.unifiedLog()).thenReturn(Optional.of(unifiedLog));
        when(interceptor.getTopicName(partition)).thenReturn("fenced-topic-partition-0");
        when(interceptor.getDynamicConfigs("fenced-topic")).thenReturn(configs);
        when(configs.sdtEnabled()).thenReturn(true);
        when(configs.sdtSuspended()).thenReturn(false);
        when(configs.getProperty("clusterTailCompactDataVisibilityIntervalInSeconds"))
            .thenReturn(Optional.empty());
        when(compactionManager.tryOpenPublicationSession(taskTopicName, streamId))
            .thenReturn(Optional.of(session));
        when(session.publishNext(any())).thenThrow(new PublicationFencedException("lost lease"));
        doReturn(scheduledFuture).when(executor).scheduleAtFixedRate(
            any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        var publisher = new TopicCompactionTaskPublisher(
            interceptor, compactionManager, partition, executor);
        allowPublication(interceptor, publisher, "fenced-topic-partition-0");
        publisher.start(Duration.ofSeconds(1));

        publisher.publish();
        publisher.publish();

        assertEquals(true, publisher.isPublicationFenced());
        assertEquals(true, publisher.isClosed());
        verify(compactionManager).tryOpenPublicationSession(taskTopicName, streamId);
        verify(session).publishNext(any());
        verify(interceptor).closePublicationSessionAsync(session);
        verify(interceptor).closeClassicPublisherIfCurrent("fenced-topic-partition-0", publisher);
    }

    @Test
    void shouldCloseDisabledPublisherImmediatelyEvenWhenDataIsPending() {
        var interceptor = mock(UrsaSDTInterceptor.class);
        var compactionManager = mock(CompactionManager.class);
        var configs = mock(DynamicConfigs.class);
        var partition = mock(Partition.class);
        var unifiedLog = mock(UnifiedLog.class);
        var executor = mock(ScheduledExecutorService.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);

        when(partition.topic()).thenReturn("disabled-topic");
        when(partition.partitionId()).thenReturn(0);
        when(partition.topicId()).thenReturn(Option.apply(Uuid.randomUuid()));
        when(partition.isLeader()).thenReturn(true);
        when(partition.unifiedLog()).thenReturn(Optional.of(unifiedLog));
        when(unifiedLog.highWatermark()).thenReturn(10L);
        when(interceptor.getTopicName(partition)).thenReturn("disabled-topic-partition-0");
        when(interceptor.getDynamicConfigs("disabled-topic")).thenReturn(configs);
        when(configs.sdtEnabled()).thenReturn(false);
        doReturn(scheduledFuture).when(executor).scheduleAtFixedRate(
            any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        var publisher = new TopicCompactionTaskPublisher(
            interceptor, compactionManager, partition, executor);
        publisher.start(Duration.ofSeconds(1));

        publisher.publish();
        publisher.publish();
        verify(interceptor, never()).closeClassicPublisherIfIdle(
            "disabled-topic-partition-0", publisher);
        verify(interceptor).closeClassicPublisherIfCurrent(
            "disabled-topic-partition-0", publisher);
        verify(scheduledFuture).cancel(false);
    }

    private static void allowPublication(
            UrsaSDTInterceptor interceptor,
            TopicCompactionTaskPublisher publisher,
            String topicName) throws Exception {
        when(interceptor.isCurrentClassicPublisher(topicName, publisher)).thenReturn(true);
        doAnswer(invocation -> {
            invocation.<UrsaSDTInterceptor.ClassicPublisherAction>getArgument(2).run();
            return true;
        }).when(interceptor).runIfCurrentClassicPublisher(
                eq(topicName), eq(publisher),
                org.mockito.ArgumentMatchers.any(UrsaSDTInterceptor.ClassicPublisherAction.class));
    }

    private static PublicationHarness publication(
            CompactionManager compactionManager,
            String topicName,
            long streamId,
            long... lastPublishedOffsets) throws Exception {
        var session = mock(CompactionManager.PublicationSession.class);
        var tasks = new ArrayList<PreparedCompactStreamTask>();
        when(compactionManager.tryOpenPublicationSession(topicName, streamId))
            .thenReturn(Optional.of(session));
        answerPublication(session, tasks, lastPublishedOffsets);
        return new PublicationHarness(session, tasks);
    }

    private static void answerPublication(
            CompactionManager.PublicationSession session,
            List<PreparedCompactStreamTask> tasks,
            long... lastPublishedOffsets) throws Exception {
        var cursor = new AtomicInteger();
        doAnswer(invocation -> {
            int index = Math.min(cursor.getAndIncrement(), lastPublishedOffsets.length - 1);
            CompactionManager.PublicationTaskFactory factory = invocation.getArgument(0);
            Optional<PreparedCompactStreamTask> task = factory.create(lastPublishedOffsets[index]);
            task.ifPresent(tasks::add);
            return task.isPresent()
                ? CompactionManager.PublicationResult.PUBLISHED
                : CompactionManager.PublicationResult.NO_TASK;
        }).when(session).publishNext(any());
    }

    private static String taskTopicName(String topic, Uuid topicId, int partition) {
        return KafkaLogNaming.logName(new TopicIdPartition(
                topicId,
                new TopicPartition(topic, partition)));
    }

    private record PublicationHarness(
            CompactionManager.PublicationSession session,
            List<PreparedCompactStreamTask> tasks) {
    }
}
