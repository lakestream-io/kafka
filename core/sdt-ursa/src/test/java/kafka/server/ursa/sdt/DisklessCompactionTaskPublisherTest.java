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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.storage.diskless.handlers.KafkaLogNaming;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.streamnative.ursa.compaction.CompactionManager;
import io.streamnative.ursa.compaction.DynamicConfigs;
import io.streamnative.ursa.compaction.PublicationFencedException;
import io.streamnative.ursa.compaction.task.PreparedCompactStreamTask;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DisklessCompactionTaskPublisherTest {

    @Test
    void shouldPublishContinuousEndExclusiveUrsaRangesUsingActualLogId() throws Exception {
        var fixture = fixture("orders", 3, 73L, -1L, 9L);

        fixture.publisher.observeHighWatermark(10L);
        fixture.publisher.publish();
        fixture.publisher.observeHighWatermark(20L);
        fixture.publisher.publish();

        verify(fixture.compactionManager).tryOpenPublicationSession(fixture.taskTopicName, 73L);
        verify(fixture.session, times(2)).publishNext(any());
        var firstTask = fixture.tasks.get(0);
        assertTask(firstTask, 73L, 0L, 10L, fixture.taskTopicName);
        var secondTask = fixture.tasks.get(1);
        assertTask(secondTask, 73L, 10L, 20L, fixture.taskTopicName);
    }

    @Test
    void shouldKeepTwoPartitionsOnTheirActualIndependentLogIds() throws Exception {
        var partitionZero = fixture("events", 0, 101L, -1L);
        var partitionOne = fixture("events", 1, 202L, -1L);

        partitionZero.publisher.observeHighWatermark(4L);
        partitionOne.publisher.observeHighWatermark(7L);
        partitionZero.publisher.publish();
        partitionOne.publisher.publish();

        assertTask(partitionZero.tasks.get(0), 101L, 0L, 4L, partitionZero.taskTopicName);
        assertTask(partitionOne.tasks.get(0), 202L, 0L, 7L, partitionOne.taskTopicName);
    }

    @Test
    void shouldRestartRangeAtZeroForRecreatedPartitionWithNewLogId() throws Exception {
        var oldPublisher = fixture("orders", 0, 301L, 8L);
        var recreatedPublisher = fixture("orders", 0, 401L, -1L);

        oldPublisher.publisher.observeHighWatermark(12L);
        recreatedPublisher.publisher.observeHighWatermark(5L);
        oldPublisher.publisher.publish();
        recreatedPublisher.publisher.publish();

        assertTask(oldPublisher.tasks.get(0), 301L, 9L, 12L, oldPublisher.taskTopicName);
        assertTask(recreatedPublisher.tasks.get(0), 401L, 0L, 5L, recreatedPublisher.taskTopicName);
        assertNotEquals(
                oldPublisher.taskTopicName,
                recreatedPublisher.taskTopicName);
    }

    @Test
    void shouldRetainPendingHighWatermarkWhilePublicationIsSuspended() throws Exception {
        var fixture = fixture("orders", 0, 73L, -1L);
        when(fixture.configs.sdtSuspended()).thenReturn(true);

        fixture.publisher.observeHighWatermark(10L);
        fixture.publisher.publish();
        fixture.publisher.publish();
        fixture.publisher.publish();
        fixture.publisher.publish();

        verify(fixture.interceptor, never()).closeDisklessPublisherIfIdle(
                "orders-partition-0", fixture.publisher);
        verify(fixture.compactionManager, never()).tryOpenPublicationSession(
                fixture.taskTopicName, 73L);
    }

    @Test
    void shouldUseDynamicIntervalAndOnlyCloseAfterPublishedDataBecomesIdle() throws Exception {
        var fixture = fixture("orders", 0, 73L, -1L, 9L, 9L);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> dynamicFuture = mock(ScheduledFuture.class);
        when(fixture.configs.getProperty("clusterTailCompactDataVisibilityIntervalInSeconds"))
                .thenReturn(Optional.of("10"), Optional.of("10"), Optional.of("10"));
        doReturn(dynamicFuture).when(fixture.executor).scheduleAtFixedRate(
                any(Runnable.class), eq(10L), eq(10L), eq(TimeUnit.SECONDS));

        fixture.publisher.observeHighWatermark(10L);
        fixture.publisher.publish();
        fixture.publisher.publish();
        verify(fixture.interceptor, never()).closeDisklessPublisherIfIdle(
                "orders-partition-0", fixture.publisher);

        fixture.publisher.publish();

        verify(fixture.executor).scheduleAtFixedRate(
                any(Runnable.class), eq(10L), eq(10L), eq(TimeUnit.SECONDS));
        verify(fixture.interceptor).closeDisklessPublisherIfIdle(
                "orders-partition-0", fixture.publisher);
    }

    @Test
    void shouldIgnoreMalformedDynamicIntervalAndStillPublish() throws Exception {
        var fixture = fixture("orders", 0, 73L, -1L);
        when(fixture.configs.getProperty("clusterTailCompactDataVisibilityIntervalInSeconds"))
                .thenReturn(Optional.of("not-a-number"));

        fixture.publisher.observeHighWatermark(10L);
        fixture.publisher.publish();

        assertEquals(1, fixture.tasks.size());
        verify(fixture.executor, never()).scheduleAtFixedRate(
                any(Runnable.class), eq(10L), eq(10L), eq(TimeUnit.SECONDS));
    }

    @Test
    void shouldRetryLeaseAcquisitionOnLaterTick() throws Exception {
        var fixture = fixture("orders", 0, 73L, -1L);
        when(fixture.compactionManager.tryOpenPublicationSession(fixture.taskTopicName, 73L))
                .thenReturn(Optional.empty(), Optional.of(fixture.session));

        fixture.publisher.observeHighWatermark(10L);
        fixture.publisher.publish();
        fixture.publisher.publish();

        verify(fixture.compactionManager, times(2)).tryOpenPublicationSession(
                fixture.taskTopicName, 73L);
        verify(fixture.session).publishNext(any());
        assertEquals(1, fixture.tasks.size());
    }

    @Test
    void shouldTerminatePublisherWhenPublicationSessionIsFenced() throws Exception {
        var fixture = fixture("orders", 0, 73L, -1L);
        doAnswer(invocation -> {
            throw new PublicationFencedException("lost lease");
        }).when(fixture.session).publishNext(any());

        fixture.publisher.observeHighWatermark(10L);
        fixture.publisher.publish();
        fixture.publisher.publish();

        assertEquals(true, fixture.publisher.isPublicationFenced());
        assertEquals(true, fixture.publisher.isClosed());
        verify(fixture.compactionManager).tryOpenPublicationSession(
                fixture.taskTopicName, 73L);
        verify(fixture.session).publishNext(any());
        verify(fixture.interceptor).closePublicationSessionAsync(fixture.session);
        verify(fixture.interceptor).closeDisklessPublisherIfCurrent(
                "orders-partition-0", fixture.publisher);
    }

    private static Fixture fixture(
            String topic,
            int partition,
            long streamId,
            long... lastPublishedOffsets) {
        var interceptor = mock(UrsaSDTInterceptor.class);
        var compactionManager = mock(CompactionManager.class);
        var configs = mock(DynamicConfigs.class);
        var executor = mock(ScheduledExecutorService.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        var topicIdPartition = new TopicIdPartition(
                Uuid.randomUuid(), new TopicPartition(topic, partition));
        String publisherKey = topic + "-partition-" + partition;
        String taskTopicName = KafkaLogNaming.logName(topicIdPartition);
        var session = mock(CompactionManager.PublicationSession.class);
        var tasks = new ArrayList<PreparedCompactStreamTask>();

        when(interceptor.getTopicName(topicIdPartition)).thenReturn(publisherKey);
        when(interceptor.getDynamicConfigs(topic)).thenReturn(configs);
        when(interceptor.brokerId()).thenReturn(7);
        when(configs.sdtEnabled()).thenReturn(true);
        when(configs.sdtSuspended()).thenReturn(false);
        when(configs.toTaskProperties()).thenReturn(Map.of());
        when(configs.getProperty("clusterTailCompactDataVisibilityIntervalInSeconds"))
                .thenReturn(Optional.empty());
        doReturn(scheduledFuture).when(executor).scheduleAtFixedRate(
                any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
        try {
            when(compactionManager.tryOpenPublicationSession(taskTopicName, streamId))
                    .thenReturn(Optional.of(session));
            answerPublication(session, tasks, lastPublishedOffsets);
        } catch (Exception error) {
            throw new AssertionError(error);
        }

        var publisher = new DisklessCompactionTaskPublisher(
                interceptor, compactionManager, topicIdPartition, streamId, executor);
        when(interceptor.isCurrentDisklessPublisher(publisherKey, publisher)).thenReturn(true);
        try {
            doAnswer(invocation -> {
                invocation.<UrsaSDTInterceptor.DisklessPublisherAction>getArgument(2).run();
                return true;
            }).when(interceptor).runIfCurrentDisklessPublisher(
                    eq(publisherKey), eq(publisher), any(UrsaSDTInterceptor.DisklessPublisherAction.class));
        } catch (Exception error) {
            throw new AssertionError(error);
        }
        publisher.start(Duration.ofSeconds(1));
        return new Fixture(
                interceptor,
                compactionManager,
                configs,
                executor,
                publisher,
                session,
                tasks,
                taskTopicName);
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

    private static void assertTask(
            PreparedCompactStreamTask task,
            long streamId,
            long startOffset,
            long endOffset,
            String topic) {
        assertEquals(streamId, task.getStreamId());
        assertEquals(startOffset, task.getStartOffset());
        assertEquals(endOffset, task.getEndOffset());
        assertEquals(topic, task.getTopic());
        assertEquals("URSA", task.getProperties().get("entryFormat"));
        assertEquals("KAFKA_BATCHED_RAW_PARQUET", task.getProperties().get("entrySerDeType"));
        assertEquals(
                topic.substring("default/".length(), topic.indexOf("-topic-id-")),
                task.getProperties().get(DisklessCompactionTaskPublisher.SOURCE_SCHEMA_TOPIC_PROPERTY));
        assertEquals("7", task.getProperties().get(UrsaSDTInterceptor.PUBLISHER_BROKER_ID_PROPERTY));
    }

    private record Fixture(
            UrsaSDTInterceptor interceptor,
            CompactionManager compactionManager,
            DynamicConfigs configs,
            ScheduledExecutorService executor,
            DisklessCompactionTaskPublisher publisher,
            CompactionManager.PublicationSession session,
            List<PreparedCompactStreamTask> tasks,
            String taskTopicName) {
    }
}
