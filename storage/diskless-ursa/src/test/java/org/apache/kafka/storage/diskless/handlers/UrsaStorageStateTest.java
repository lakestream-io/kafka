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
package org.apache.kafka.storage.diskless.handlers;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.storage.diskless.DisklessClientZone;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateManager;
import org.apache.kafka.storage.internals.log.LogMetricNames;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import io.lakestream.api.Log;
import io.lakestream.api.LogId;
import io.lakestream.api.LogOffset;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamLayout;
import io.lakestream.api.StreamMetadata;
import io.oxia.client.api.AsyncOxiaClient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class UrsaStorageStateTest {
    private static final String LOG_METRIC_GROUP = "kafka.log";
    private static final String LOG_METRIC_TYPE = "Log";

    @Test
    void testCleanupPartitionClosesLogAndClearsProducerState() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 0));
        ProducerStateManager producerStateManager = mockProducerStateManager();
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log log1 = mockLog(5L, 41L, 1, 500L, 100, 1_400L, 200);
        Log log2 = mockLog(5L, 41L, 1, 500L, 100, 1_400L, 200);
        CountDownLatch closeLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            closeLatch.countDown();
            return null;
        }).when(log1).close();

        AtomicInteger openCount = new AtomicInteger();
        stubCatalogLayout(catalog);
        when(catalog.openLog(any(), any())).thenAnswer(invocation ->
                CompletableFuture.completedFuture(openCount.incrementAndGet() == 1 ? log1 : log2));

        try (UrsaStorageState state = newState(catalog)) {
            UrsaPartitionLog partitionLog = state.getOrCreatePartitionLog(tp);
            partitionLog.installProducerStateManager(DisklessClientZone.NO_ZONE, producerStateManager);

            assertTrue(state.cleanupPartition(tp));
            verify(producerStateManager).cleanup(false);
            verify(log1, never()).fence();
            verify(catalog).openLog(any(), any());
            assertTrue(closeLatch.await(5, TimeUnit.SECONDS));
            assertNull(state.partitionLog(tp));

            state.getOrCreatePartitionLog(tp);
            verify(catalog, times(2)).openLog(any(), any());
            assertNotNull(state.partitionLog(tp));
        }
    }

    @Test
    void testTransientLogCloseFailureIsRetainedAndRetriedBeforeReopen() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(
                Uuid.randomUuid(), new TopicPartition("close-retry-topic", 0));
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log firstLog = mockLog(0L, 0L, 1, 10L, 10, 10L, 10);
        Log replacementLog = mockLog(0L, 0L, 1, 10L, 10, 10L, 10);
        doThrow(new IOException("temporary lease release failure"))
                .doNothing()
                .when(firstLog)
                .close();
        stubCatalogLayout(catalog);
        when(catalog.openLog(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(firstLog))
                .thenReturn(CompletableFuture.completedFuture(replacementLog));

        try (UrsaStorageState state = newState(catalog)) {
            UrsaPartitionLog first = state.getOrCreatePartitionLog(tp);

            assertTrue(state.cleanupPartition(tp));
            assertNull(state.partitionLog(tp));
            assertEquals(1, state.retiredPartitionLogCount());

            TestUtils.waitForCondition(
                    () -> state.retiredPartitionLogCount() == 0,
                    5_000,
                    "Expected the failed leased Log close to be retried in the background");
            verify(firstLog, times(2)).close();
            verify(firstLog, never()).fence();

            UrsaPartitionLog replacement = state.getOrCreatePartitionLog(tp);
            assertNotSame(first, replacement);
            assertSame(replacement, state.partitionLog(tp));
            verify(catalog, times(2)).openLog(any(), any());
        }
    }

    @Test
    void testCleanupPartitionNoopWhenNoState() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        try (UrsaStorageState state = newState(catalog)) {
            TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 0));

            assertFalse(state.cleanupPartition(tp));
            verify(catalog, never()).openLog(any(), any());
        }
    }

    @Test
    void testCleanupNonOwnedProducerStatesRemovesOnlyStaleZones() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 1));
        ProducerStateManager noZoneManager = mockProducerStateManager();
        ProducerStateManager zoneAManager = mockProducerStateManager();
        StreamCatalog catalog = mockCatalogWithLog(mockLog(0L, 0L, 1, 10L, 10, 10L, 10));

        try (UrsaStorageState state = newState(catalog)) {
            UrsaPartitionLog partitionLog = state.getOrCreatePartitionLog(tp);
            partitionLog.installProducerStateManager(DisklessClientZone.NO_ZONE, noZoneManager);
            partitionLog.installProducerStateManager("zone-a", zoneAManager);

            assertTrue(state.cleanupNonOwnedProducerStates(tp, Set.of("zone-a"), false));
            verify(noZoneManager).cleanup(false);
            verify(zoneAManager, never()).cleanup(false);
            assertEquals(Set.of(tp), state.snapshotTrackedPartitions());

            assertTrue(state.cleanupPartition(tp, false));
            verify(zoneAManager).cleanup(false);
        }
    }

    @Test
    void testStateCloseWaitsForPreviouslyStartedProducerStateCleanup() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("close-drain-topic", 0));
        CompletableFuture<Void> cleanupGate = new CompletableFuture<>();
        CountDownLatch cleanupStarted = new CountDownLatch(1);
        ProducerStateManager producerStateManager = mock(ProducerStateManager.class);
        when(producerStateManager.cleanup(false)).thenAnswer(invocation -> {
            cleanupStarted.countDown();
            return cleanupGate;
        });
        StreamCatalog catalog = mockCatalogWithLog(mockLog(0L, 0L, 1, 10L, 10, 10L, 10));
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        doAnswer(invocation -> {
            assertTrue(cleanupGate.isDone(), "Oxia client closed before producer-state cleanup completed");
            return null;
        }).when(oxiaClient).close();
        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, oxiaClient);
        UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                holder,
                Map.of(),
                null);

        try {
            UrsaPartitionLog partitionLog = state.getOrCreatePartitionLog(tp);
            partitionLog.installProducerStateManager(DisklessClientZone.NO_ZONE, producerStateManager);
            assertTrue(state.cleanupPartition(tp, false));
            assertTrue(state.snapshotTrackedPartitions().isEmpty());
            assertTrue(cleanupStarted.await(5, TimeUnit.SECONDS));

            CompletableFuture<Void> closeFuture = CompletableFuture.runAsync(() -> {
                try {
                    state.close();
                } catch (Exception error) {
                    throw new RuntimeException(error);
                }
            });

            assertFalse(closeFuture.isDone());
            verify(oxiaClient, never()).close();
            cleanupGate.complete(null);
            closeFuture.get(5, TimeUnit.SECONDS);
            verify(oxiaClient).close();
        } finally {
            cleanupGate.complete(null);
            state.close();
        }
    }

    @Test
    void testStateCloseTimeoutKeepsResourcesForRetry() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("close-timeout-topic", 0));
        CompletableFuture<Void> cleanupGate = new CompletableFuture<>();
        ProducerStateManager producerStateManager = mock(ProducerStateManager.class);
        when(producerStateManager.cleanup(false)).thenReturn(cleanupGate);
        StreamCatalog catalog = mockCatalogWithLog(mockLog(0L, 0L, 1, 10L, 10, 10L, 10));
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, oxiaClient);
        UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                holder,
                Map.of(),
                null);

        UrsaPartitionLog partitionLog = state.getOrCreatePartitionLog(tp);
        partitionLog.installProducerStateManager(DisklessClientZone.NO_ZONE, producerStateManager);

        IOException timeout = assertThrows(IOException.class, () -> state.close(100L));
        assertTrue(timeout.getMessage().contains("Timed out"));
        verify(catalog, never()).close();
        verify(oxiaClient, never()).close();

        cleanupGate.complete(null);
        state.close(5_000L);

        verify(catalog).close();
        verify(oxiaClient).close();
    }

    @Test
    void testStateCloseReleasesResourcesAfterProducerStateCleanupFailure() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("close-failure-topic", 0));
        ProducerStateManager producerStateManager = mock(ProducerStateManager.class);
        when(producerStateManager.cleanup(false))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("cleanup failed")));
        StreamCatalog catalog = mockCatalogWithLog(mockLog(0L, 0L, 1, 10L, 10, 10L, 10));
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, oxiaClient);
        UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                holder,
                Map.of(),
                null);

        UrsaPartitionLog partitionLog = state.getOrCreatePartitionLog(tp);
        partitionLog.installProducerStateManager(DisklessClientZone.NO_ZONE, producerStateManager);

        state.close();

        verify(producerStateManager).cleanup(false);
        verify(oxiaClient).close();
        verify(catalog).close();
    }

    @Test
    void testGetOrCreatePartitionLogIsRejectedAfterStateClose() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("closed-state-topic", 0));
        StreamCatalog catalog = mock(StreamCatalog.class);
        UrsaStorageState state = newState(catalog);

        state.close();

        assertThrows(IllegalStateException.class, () -> state.getOrCreatePartitionLog(tp));
        assertTrue(state.snapshotTrackedPartitions().isEmpty());
        verify(catalog, never()).openLog(any(), any());
    }

    @Test
    void testStateCloseDrainsPartitionLogCreationAlreadyInProgress() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("closing-state-topic", 0));
        CountDownLatch openStarted = new CountDownLatch(1);
        CountDownLatch allowOpen = new CountDownLatch(1);
        CountDownLatch closeStarted = new CountDownLatch(1);
        Log logInstance = mockLog(0L, 0L, 1, 10L, 10, 10L, 10);
        StreamCatalog catalog = mock(StreamCatalog.class);
        stubCatalogLayout(catalog);
        when(catalog.openLog(any(), any())).thenAnswer(invocation -> {
            openStarted.countDown();
            assertTrue(allowOpen.await(5, TimeUnit.SECONDS));
            return CompletableFuture.completedFuture(logInstance);
        });
        UrsaStorageState state = newState(catalog);

        try {
            CompletableFuture<UrsaPartitionLog> createFuture = CompletableFuture.supplyAsync(
                    () -> state.getOrCreatePartitionLog(tp));
            assertTrue(openStarted.await(5, TimeUnit.SECONDS));

            CompletableFuture<Void> closeFuture = CompletableFuture.runAsync(() -> {
                closeStarted.countDown();
                try {
                    state.close();
                } catch (Exception error) {
                    throw new RuntimeException(error);
                }
            });
            assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
            assertFalse(closeFuture.isDone());

            allowOpen.countDown();
            assertNotNull(createFuture.get(5, TimeUnit.SECONDS));
            closeFuture.get(5, TimeUnit.SECONDS);

            assertTrue(state.snapshotTrackedPartitions().isEmpty());
            verify(logInstance).close();
        } finally {
            allowOpen.countDown();
            state.close();
        }
    }

    @Test
    void testFailedLogOpenEvictsPartitionLogAndAllowsRetry() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("retry-topic", 0));
        StreamCatalog catalog = mock(StreamCatalog.class);
        CompletableFuture<Log> firstOpen = new CompletableFuture<>();
        Log secondLog = mockLog(10L, 10L, 1, 100L, 20, 100L, 20);
        AtomicInteger openCount = new AtomicInteger();
        stubCatalogLayout(catalog);
        when(catalog.openLog(any(), any())).thenAnswer(invocation ->
                openCount.getAndIncrement() == 0
                        ? firstOpen
                        : CompletableFuture.completedFuture(secondLog));

        try (UrsaStorageState state = newState(catalog)) {
            UrsaPartitionLog first = state.getOrCreatePartitionLog(tp);
            assertSame(first, state.partitionLog(tp));

            firstOpen.completeExceptionally(new RuntimeException("open failed"));
            assertNull(state.partitionLog(tp));

            UrsaPartitionLog second = state.getOrCreatePartitionLog(tp);
            assertNotNull(second);
            assertSame(second, state.partitionLog(tp));
            verify(catalog, times(2)).openLog(any(), any());
        }
    }

    @Test
    void testImmediatelyFailedLogOpenIsNotCached() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("immediate-retry-topic", 0));
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log secondLog = mockLog(10L, 10L, 1, 100L, 20, 100L, 20);
        stubCatalogLayout(catalog);
        when(catalog.openLog(any(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("open failed")))
                .thenReturn(CompletableFuture.completedFuture(secondLog));

        try (UrsaStorageState state = newState(catalog)) {
            UrsaPartitionLog failed = state.getOrCreatePartitionLog(tp);
            assertTrue(failed.initializationFailed());
            assertNull(state.partitionLog(tp));

            UrsaPartitionLog retried = state.getOrCreatePartitionLog(tp);
            assertNotNull(retried);
            assertFalse(retried.initializationFailed());
            assertSame(retried, state.partitionLog(tp));
            verify(catalog, times(2)).openLog(any(), any());
        }
    }

    @Test
    void testDisklessLogMetricsRegisteredAfterLogOpen() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("metric-open-topic", 0));
        StreamCatalog catalog = mockCatalogWithLog(
                mockLog(5L, 41L, 1, 500L, 100, 1_400L, 200));

        try (UrsaStorageState state = newState(catalog)) {
            assertNull(jmxGaugeLongValue(LogMetricNames.SIZE, tp.topicPartition()));
            assertNull(jmxGaugeLongValue(LogMetricNames.LOG_START_OFFSET, tp.topicPartition()));
            assertNull(jmxGaugeLongValue(LogMetricNames.LOG_END_OFFSET, tp.topicPartition()));

            state.getOrCreatePartitionLog(tp);

            assertEquals(1_000L, jmxGaugeLongValue(LogMetricNames.SIZE, tp.topicPartition()));
            assertEquals(5L, jmxGaugeLongValue(LogMetricNames.LOG_START_OFFSET, tp.topicPartition()));
            assertEquals(42L, jmxGaugeLongValue(LogMetricNames.LOG_END_OFFSET, tp.topicPartition()));
            assertTrue(hasDisklessLogMetric(LogMetricNames.SIZE, tp.topicPartition()));
            assertTrue(hasDisklessLogMetric(LogMetricNames.LOG_START_OFFSET, tp.topicPartition()));
            assertTrue(hasDisklessLogMetric(LogMetricNames.LOG_END_OFFSET, tp.topicPartition()));
        }
    }

    @Test
    void testDisklessLogMetricsDoNotBlockOnPendingOffsetRefresh() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("metric-pending-topic", 0));
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log logInstance = mock(Log.class);
        CompletableFuture<LogOffset> firstOffset = new CompletableFuture<>();
        CompletableFuture<LogOffset> lastOffset = new CompletableFuture<>();
        stubCatalogLayout(catalog);
        when(catalog.openLog(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(logInstance));
        when(logInstance.getFirstOffset()).thenReturn(firstOffset);
        when(logInstance.getLastOffset()).thenReturn(lastOffset);

        try (UrsaStorageState state = newState(catalog)) {
            state.getOrCreatePartitionLog(tp);
            CompletableFuture<Long> gaugeRead = CompletableFuture.supplyAsync(
                    () -> jmxGaugeLongValue(LogMetricNames.SIZE, tp.topicPartition()));
            try {
                assertEquals(0L, gaugeRead.get(1, TimeUnit.SECONDS));
            } finally {
                firstOffset.complete(new LogOffset(5L, 1, -1L, 100, 500L));
                lastOffset.complete(new LogOffset(41L, 1, -1L, 200, 1_400L));
            }

            assertEquals(1_000L, jmxGaugeLongValue(LogMetricNames.SIZE, tp.topicPartition()));
            assertEquals(5L, jmxGaugeLongValue(LogMetricNames.LOG_START_OFFSET, tp.topicPartition()));
            assertEquals(42L, jmxGaugeLongValue(LogMetricNames.LOG_END_OFFSET, tp.topicPartition()));
        }
    }

    @Test
    void testDisklessLogMetricsRemovedOnCleanupPartition() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("metric-cleanup-topic", 0));
        StreamCatalog catalog = mockCatalogWithLog(
                mockLog(5L, 41L, 1, 500L, 100, 1_400L, 200));

        try (UrsaStorageState state = newState(catalog)) {
            state.getOrCreatePartitionLog(tp);
            assertNotNull(jmxGaugeLongValue(LogMetricNames.SIZE, tp.topicPartition()));
            assertNotNull(jmxGaugeLongValue(LogMetricNames.LOG_START_OFFSET, tp.topicPartition()));
            assertNotNull(jmxGaugeLongValue(LogMetricNames.LOG_END_OFFSET, tp.topicPartition()));

            assertTrue(state.cleanupPartition(tp, false));

            assertNull(jmxGaugeLongValue(LogMetricNames.SIZE, tp.topicPartition()));
            assertNull(jmxGaugeLongValue(LogMetricNames.LOG_START_OFFSET, tp.topicPartition()));
            assertNull(jmxGaugeLongValue(LogMetricNames.LOG_END_OFFSET, tp.topicPartition()));
        }
    }

    @Test
    void testLogOpenInitializesPartitionLog() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("read-state-topic", 0));
        StreamCatalog catalog = mockCatalogWithLog(
                mockLog(0L, 0L, 1, 10L, 10, 10L, 10));

        try (UrsaStorageState state = newState(catalog)) {
            assertNull(state.partitionLog(tp));

            UrsaPartitionLog partitionLog = state.getOrCreatePartitionLog(tp);

            assertNotNull(partitionLog);
            assertSame(partitionLog, state.partitionLog(tp));
            verify(catalog).openLog(any(), any());
        }
    }

    @Test
    void testCleanupPartitionSuppressesMetricRegistrationForInFlightLogOpen() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(
                Uuid.randomUuid(),
                new TopicPartition("metric-cleanup-race-topic-" + Uuid.randomUuid(), 0));
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log firstLog = mockLog(10L, 10L, 1, 100L, 20, 100L, 20);
        Log secondLog = mockLog(10L, 10L, 1, 100L, 20, 100L, 20);
        CountDownLatch firstCloseLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            firstCloseLatch.countDown();
            return null;
        }).when(firstLog).close();

        AtomicInteger openCount = new AtomicInteger();
        CompletableFuture<Log> firstOpenFuture = new CompletableFuture<>();
        stubCatalogLayout(catalog);
        when(catalog.openLog(any(), any())).thenAnswer(invocation ->
                openCount.getAndIncrement() == 0
                        ? firstOpenFuture
                        : CompletableFuture.completedFuture(secondLog));

        try (UrsaStorageState state = newState(catalog)) {
            state.getOrCreatePartitionLog(tp);

            assertTrue(state.cleanupPartition(tp, false));
            assertNull(jmxGaugeLongValue(LogMetricNames.SIZE, tp.topicPartition()));
            assertNull(jmxGaugeLongValue(LogMetricNames.LOG_START_OFFSET, tp.topicPartition()));
            assertNull(jmxGaugeLongValue(LogMetricNames.LOG_END_OFFSET, tp.topicPartition()));

            firstOpenFuture.complete(firstLog);
            assertTrue(firstCloseLatch.await(5, TimeUnit.SECONDS));
            assertNull(jmxGaugeLongValue(LogMetricNames.SIZE, tp.topicPartition()));
            assertNull(jmxGaugeLongValue(LogMetricNames.LOG_START_OFFSET, tp.topicPartition()));
            assertNull(jmxGaugeLongValue(LogMetricNames.LOG_END_OFFSET, tp.topicPartition()));

            state.getOrCreatePartitionLog(tp);

            verify(catalog, times(2)).openLog(any(), any());
            assertEquals(20L, jmxGaugeLongValue(LogMetricNames.SIZE, tp.topicPartition()));
        }
    }

    @Test
    void testDisklessLogMetricsRemovedOnStateClose() throws Exception {
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("metric-close-topic", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("metric-close-topic", 1));
        StreamCatalog catalog = mockCatalogWithLog(
                mockLog(10L, 10L, 1, 100L, 20, 100L, 20));
        UrsaStorageState state = newState(catalog);

        state.getOrCreatePartitionLog(tp0);
        state.getOrCreatePartitionLog(tp1);
        assertNotNull(jmxGaugeLongValue(LogMetricNames.SIZE, tp0.topicPartition()));
        assertNotNull(jmxGaugeLongValue(LogMetricNames.SIZE, tp1.topicPartition()));

        state.close();

        assertNull(jmxGaugeLongValue(LogMetricNames.SIZE, tp0.topicPartition()));
        assertNull(jmxGaugeLongValue(LogMetricNames.LOG_START_OFFSET, tp0.topicPartition()));
        assertNull(jmxGaugeLongValue(LogMetricNames.LOG_END_OFFSET, tp0.topicPartition()));
        assertNull(jmxGaugeLongValue(LogMetricNames.SIZE, tp1.topicPartition()));
        assertNull(jmxGaugeLongValue(LogMetricNames.LOG_START_OFFSET, tp1.topicPartition()));
        assertNull(jmxGaugeLongValue(LogMetricNames.LOG_END_OFFSET, tp1.topicPartition()));
    }

    @Test
    void testDisklessLogMetricsCoexistWithClassicLogMetrics() throws Exception {
        TopicPartition topicPartition = new TopicPartition("metric-conflict-topic-" + Uuid.randomUuid(), 0);
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), topicPartition);
        Map<String, String> tags = classicLogMetricTags(topicPartition);
        KafkaMetricsGroup externalMetricsGroup = new KafkaMetricsGroup(LOG_METRIC_GROUP, LOG_METRIC_TYPE);
        externalMetricsGroup.newGauge(LogMetricNames.SIZE, () -> 777L, tags);
        externalMetricsGroup.newGauge(LogMetricNames.LOG_START_OFFSET, () -> 11L, tags);
        externalMetricsGroup.newGauge(LogMetricNames.LOG_END_OFFSET, () -> 22L, tags);
        StreamCatalog catalog = mockCatalogWithLog(
                mockLog(5L, 41L, 1, 500L, 100, 1_400L, 200));

        try (UrsaStorageState state = newState(catalog)) {
            assertEquals(777L, classicJmxGaugeLongValue(LogMetricNames.SIZE, topicPartition));
            assertNull(jmxGaugeLongValue(LogMetricNames.SIZE, topicPartition));

            state.getOrCreatePartitionLog(tp);

            assertEquals(1_000L, jmxGaugeLongValue(LogMetricNames.SIZE, topicPartition));
            assertEquals(5L, jmxGaugeLongValue(LogMetricNames.LOG_START_OFFSET, topicPartition));
            assertEquals(42L, jmxGaugeLongValue(LogMetricNames.LOG_END_OFFSET, topicPartition));
            assertEquals(777L, classicJmxGaugeLongValue(LogMetricNames.SIZE, topicPartition));
            assertTrue(hasDisklessLogMetric(LogMetricNames.SIZE, topicPartition));
            assertTrue(hasClassicLogMetric(LogMetricNames.SIZE, topicPartition));
        } finally {
            externalMetricsGroup.removeMetric(LogMetricNames.SIZE, tags);
            externalMetricsGroup.removeMetric(LogMetricNames.LOG_START_OFFSET, tags);
            externalMetricsGroup.removeMetric(LogMetricNames.LOG_END_OFFSET, tags);
        }
    }

    @Test
    void testDisklessLogMetricsSkipWhenDisklessMetricAlreadyExists() throws Exception {
        TopicPartition topicPartition = new TopicPartition("metric-diskless-conflict-topic-" + Uuid.randomUuid(), 0);
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), topicPartition);
        Map<String, String> tags = disklessLogMetricTags(topicPartition);
        KafkaMetricsGroup externalMetricsGroup = new KafkaMetricsGroup(LOG_METRIC_GROUP, LOG_METRIC_TYPE);
        externalMetricsGroup.newGauge(LogMetricNames.SIZE, () -> 777L, tags);
        externalMetricsGroup.newGauge(LogMetricNames.LOG_START_OFFSET, () -> 11L, tags);
        externalMetricsGroup.newGauge(LogMetricNames.LOG_END_OFFSET, () -> 22L, tags);
        StreamCatalog catalog = mockCatalogWithLog(
                mockLog(5L, 41L, 1, 500L, 100, 1_400L, 200));

        try (UrsaStorageState state = newState(catalog)) {
            state.getOrCreatePartitionLog(tp);

            assertEquals(777L, jmxGaugeLongValue(LogMetricNames.SIZE, topicPartition));
            assertEquals(11L, jmxGaugeLongValue(LogMetricNames.LOG_START_OFFSET, topicPartition));
            assertEquals(22L, jmxGaugeLongValue(LogMetricNames.LOG_END_OFFSET, topicPartition));
            assertTrue(hasDisklessLogMetric(LogMetricNames.SIZE, topicPartition));
        } finally {
            externalMetricsGroup.removeMetric(LogMetricNames.SIZE, tags);
            externalMetricsGroup.removeMetric(LogMetricNames.LOG_START_OFFSET, tags);
            externalMetricsGroup.removeMetric(LogMetricNames.LOG_END_OFFSET, tags);
        }
    }

    @Test
    void testBrokerTopicConfigUpdateDoesNotMutateCatalog() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("config-race-topic", 0));
        Map<String, String> latestConfig = Map.of("retention.ms", "2000");
        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null);
        try (UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                holder,
                Map.of(),
                ignored -> Map.of())) {
            state.applyTopicConfigAsync(tp.topic(), tp.topicId(), latestConfig).get();

            verifyNoInteractions(catalog);
        }
    }

    private static UrsaStorageState newState(StreamCatalog catalog) {
        return new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                catalog);
    }

    private static StreamCatalog mockCatalogWithLog(Log logInstance) {
        StreamCatalog catalog = mock(StreamCatalog.class);
        stubCatalogLayout(catalog);
        when(catalog.openLog(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(logInstance));
        return catalog;
    }

    private static void stubCatalogLayout(StreamCatalog catalog) {
        StreamMetadata metadata = mock(StreamMetadata.class);
        StreamLayout layout = mock(StreamLayout.class);
        when(catalog.loadStream(any())).thenReturn(CompletableFuture.completedFuture(metadata));
        when(metadata.layout()).thenReturn(layout);
        when(layout.logIds()).thenReturn(CompletableFuture.completedFuture(
                LongStream.range(0, 100)
                        .mapToObj(LogId::of)
                        .toList()));
    }

    private static ProducerStateManager mockProducerStateManager() {
        ProducerStateManager producerStateManager = mock(ProducerStateManager.class);
        when(producerStateManager.cleanup(false)).thenReturn(CompletableFuture.completedFuture(null));
        return producerStateManager;
    }

    private static Log mockLog(
            long firstOffset,
            long lastOffset,
            int lastNumberOfRecords,
            long firstCumulativeSize,
            int firstEntrySize,
            long lastCumulativeSize,
            int lastEntrySize) {
        Log logInstance = mock(Log.class);
        LogOffset first = new LogOffset(firstOffset, 1, -1L, firstEntrySize, firstCumulativeSize);
        LogOffset last = new LogOffset(
                lastOffset,
                lastNumberOfRecords,
                -1L,
                lastEntrySize,
                lastCumulativeSize);
        when(logInstance.getFirstOffset()).thenReturn(CompletableFuture.completedFuture(first));
        when(logInstance.getLastOffset()).thenReturn(CompletableFuture.completedFuture(last));
        return logInstance;
    }

    private static Map<String, String> classicLogMetricTags(TopicPartition topicPartition) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("topic", topicPartition.topic());
        tags.put("partition", String.valueOf(topicPartition.partition()));
        return tags;
    }

    private static Map<String, String> disklessLogMetricTags(TopicPartition topicPartition) {
        Map<String, String> tags = classicLogMetricTags(topicPartition);
        tags.put("storage", "diskless");
        return tags;
    }

    private static boolean hasDisklessLogMetric(String metricName, TopicPartition topicPartition) {
        return hasLogMetric(metricName, topicPartition, true);
    }

    private static boolean hasClassicLogMetric(String metricName, TopicPartition topicPartition) {
        return hasLogMetric(metricName, topicPartition, false);
    }

    private static boolean hasLogMetric(String metricName, TopicPartition topicPartition, boolean diskless) {
        String storageTag = ",storage=diskless";
        for (var entry : KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet()) {
            var name = entry.getKey();
            if (!LOG_METRIC_GROUP.equals(name.getGroup()) || !LOG_METRIC_TYPE.equals(name.getType())) {
                continue;
            }
            String mBeanName = name.getMBeanName();
            if (!mBeanName.contains("type=Log,name=" + metricName)
                    || !mBeanName.contains(",topic=" + topicPartition.topic())
                    || !mBeanName.contains(",partition=" + topicPartition.partition())) {
                continue;
            }
            if (diskless == mBeanName.contains(storageTag)) {
                return true;
            }
        }
        return false;
    }

    private static Long jmxGaugeLongValue(String metricName, TopicPartition topicPartition) {
        return jmxGaugeLongValue(metricName, topicPartition, true);
    }

    private static Long classicJmxGaugeLongValue(String metricName, TopicPartition topicPartition) {
        return jmxGaugeLongValue(metricName, topicPartition, false);
    }

    private static Long jmxGaugeLongValue(String metricName, TopicPartition topicPartition, boolean diskless) {
        for (var entry : KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet()) {
            var name = entry.getKey();
            if (!LOG_METRIC_GROUP.equals(name.getGroup()) || !LOG_METRIC_TYPE.equals(name.getType())) {
                continue;
            }
            String mBeanName = name.getMBeanName();
            if (!mBeanName.contains("type=Log,name=" + metricName)
                    || !mBeanName.contains(",topic=" + topicPartition.topic())
                    || !mBeanName.contains(",partition=" + topicPartition.partition())) {
                continue;
            }
            if (diskless != mBeanName.contains(",storage=diskless")) {
                continue;
            }
            Object metric = entry.getValue();
            if (metric instanceof com.yammer.metrics.core.Gauge<?> gauge) {
                Object value = gauge.value();
                if (value instanceof Number numberValue) {
                    return numberValue.longValue();
                }
            }
        }
        return null;
    }
}
