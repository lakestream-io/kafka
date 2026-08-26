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
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import io.streamnative.lakestream.api.Log;
import io.streamnative.lakestream.api.LogId;
import io.streamnative.lakestream.api.LogOffset;
import io.streamnative.lakestream.api.Stream;
import io.streamnative.lakestream.api.StreamIdentifier;
import io.streamnative.ursa.lakestream.impl.IndexedStreamCatalog;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class UrsaStorageStateRetentionTest {

    @Test
    void testLogOpenAppliesDefaultDisabledRetention() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("retention-test-topic", 0));
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        Log logInstance = mock(Log.class);
        when(catalog.generateStreamId(Optional.of(KafkaLogNaming.logName(tp))))
                .thenReturn(CompletableFuture.completedFuture(1L));
        when(catalog.createLog(KafkaLogNaming.logName(tp), LogId.of(1L)))
                .thenReturn(logInstance);

        try (UrsaStorageState state = newState(catalog)) {
            Log result = state.openLog(tp).get(5, TimeUnit.SECONDS);

            assertSame(logInstance, result);
            verify(logInstance, never()).getLastOffset();
            verify(logInstance, never()).computeRetentionTrimOffset(anyLong(), anyLong(), anyLong());
            verify(logInstance, never()).softTrim(anyLong());
        }
    }

    @Test
    void testDisabledRetentionDoesNotInspectLog() throws Exception {
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        Log logInstance = mock(Log.class);

        try (UrsaStorageState state = newState(catalog)) {
            assertSame(logInstance, state.maybeApplyRetention(logInstance, -1L, -1L).get(5, TimeUnit.SECONDS));
            verifyNoInteractions(logInstance);
        }
    }

    @Test
    void testRetentionTrimAppliedAtComputedOffset() throws Exception {
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        Log logInstance = mock(Log.class);
        LogOffset lastOffset = mockOffset(42L);
        LogOffset firstOffset = mockOffset(5L);
        long retentionMs = 120_000L;
        long retentionBytes = 2L * 1024 * 1024;

        when(logInstance.getLastOffset()).thenReturn(CompletableFuture.completedFuture(lastOffset));
        when(logInstance.computeRetentionTrimOffset(42L, retentionMs, retentionBytes))
                .thenReturn(CompletableFuture.completedFuture(10L));
        when(logInstance.getFirstOffset()).thenReturn(CompletableFuture.completedFuture(firstOffset));
        when(logInstance.softTrim(10L)).thenReturn(CompletableFuture.completedFuture(null));

        try (UrsaStorageState state = newState(catalog)) {
            assertSame(
                    logInstance,
                    state.maybeApplyRetention(logInstance, retentionMs, retentionBytes).get(5, TimeUnit.SECONDS));
            verify(logInstance).softTrim(10L);
        }
    }

    @Test
    void testRetentionTrimBeforeCurrentStartIsSkipped() throws Exception {
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        Log logInstance = mock(Log.class);
        LogOffset lastOffset = mockOffset(42L);
        LogOffset firstOffset = mockOffset(5L);

        when(logInstance.getLastOffset()).thenReturn(CompletableFuture.completedFuture(lastOffset));
        when(logInstance.computeRetentionTrimOffset(42L, 120_000L, -1L))
                .thenReturn(CompletableFuture.completedFuture(4L));
        when(logInstance.getFirstOffset()).thenReturn(CompletableFuture.completedFuture(firstOffset));

        try (UrsaStorageState state = newState(catalog)) {
            assertSame(
                    logInstance,
                    state.maybeApplyRetention(logInstance, 120_000L, -1L).get(5, TimeUnit.SECONDS));
            verify(logInstance, never()).softTrim(4L);
        }
    }

    @Test
    void testRetentionFailureDoesNotFailLogOpen() throws Exception {
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        Log logInstance = mock(Log.class);
        when(logInstance.getLastOffset()).thenReturn(CompletableFuture.failedFuture(new RuntimeException("boom")));

        try (UrsaStorageState state = newState(catalog)) {
            assertSame(
                    logInstance,
                    state.maybeApplyRetention(logInstance, 120_000L, -1L).get(5, TimeUnit.SECONDS));
            verify(logInstance, never()).computeRetentionTrimOffset(anyLong(), anyLong(), anyLong());
            verify(logInstance, never()).softTrim(anyLong());
        }
    }

    @Test
    void testEmptyLogSkipsRetentionComputation() throws Exception {
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        Log logInstance = mock(Log.class);
        when(logInstance.getLastOffset()).thenReturn(CompletableFuture.completedFuture(LogOffset.NOT_FOUND));

        try (UrsaStorageState state = newState(catalog)) {
            assertSame(
                    logInstance,
                    state.maybeApplyRetention(logInstance, 120_000L, -1L).get(5, TimeUnit.SECONDS));
            verify(logInstance, never()).computeRetentionTrimOffset(anyLong(), anyLong(), anyLong());
            verify(logInstance, never()).getFirstOffset();
            verify(logInstance, never()).softTrim(anyLong());
        }
    }

    @Test
    void testMissingFirstOffsetSkipsTrim() throws Exception {
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        Log logInstance = mock(Log.class);
        LogOffset lastOffset = mockOffset(42L);

        when(logInstance.getLastOffset()).thenReturn(CompletableFuture.completedFuture(lastOffset));
        when(logInstance.computeRetentionTrimOffset(42L, 120_000L, -1L))
                .thenReturn(CompletableFuture.completedFuture(10L));
        when(logInstance.getFirstOffset()).thenReturn(CompletableFuture.completedFuture(null));

        try (UrsaStorageState state = newState(catalog)) {
            assertSame(
                    logInstance,
                    state.maybeApplyRetention(logInstance, 120_000L, -1L).get(5, TimeUnit.SECONDS));
            verify(logInstance, never()).softTrim(anyLong());
        }
    }

    @Test
    void testPeriodicRetentionTrimsWritesAppendedAfterEmptyOpen() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("periodic-retention", 0));
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        Log logInstance = mock(Log.class);
        LogOffset firstOffset = mockOffset(5L);
        LogOffset lastOffset = mockOffset(42L);
        when(catalog.generateStreamId(Optional.of(KafkaLogNaming.logName(tp))))
                .thenReturn(CompletableFuture.completedFuture(1L));
        when(catalog.createLog(KafkaLogNaming.logName(tp), LogId.of(1L)))
                .thenReturn(logInstance);
        when(logInstance.getFirstOffset()).thenReturn(CompletableFuture.completedFuture(firstOffset));
        when(logInstance.getLastOffset())
                .thenReturn(CompletableFuture.completedFuture(LogOffset.NOT_FOUND))
                .thenReturn(CompletableFuture.completedFuture(LogOffset.NOT_FOUND))
                .thenReturn(CompletableFuture.completedFuture(lastOffset));
        when(logInstance.computeRetentionTrimOffset(42L, 120_000L, -1L))
                .thenReturn(CompletableFuture.completedFuture(10L));
        when(logInstance.softTrim(10L)).thenReturn(CompletableFuture.completedFuture(null));

        Map<String, Object> defaults = Map.of(
                TopicConfig.RETENTION_MS_CONFIG, 120_000L,
                TopicConfig.RETENTION_BYTES_CONFIG, -1L,
                ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_CONFIG, 60_000L);
        try (UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                catalog,
                defaults,
                ignored -> Map.of())) {
            state.getOrCreatePartitionLog(tp);
            verify(logInstance, never()).softTrim(10L);

            state.triggerPeriodicRetention();

            verify(logInstance).softTrim(10L);
        }
    }

    @Test
    void testTopicConfigUpdateTriggersRetentionForOpenPartition() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("dynamic-retention", 0));
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        Stream stream = mock(Stream.class);
        Log logInstance = mock(Log.class);
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);
        Map<String, String> updatedConfig = Map.of(TopicConfig.RETENTION_MS_CONFIG, "120000");
        LogOffset firstOffset = mockOffset(5L);
        LogOffset lastOffset = mockOffset(42L);

        when(catalog.generateStreamId(Optional.of(KafkaLogNaming.logName(tp))))
                .thenReturn(CompletableFuture.completedFuture(1L));
        when(catalog.registerExternalPartition(identifier, 0, 1L, Map.of()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(catalog.streamExists(identifier)).thenReturn(CompletableFuture.completedFuture(true));
        when(catalog.loadStream(identifier)).thenReturn(CompletableFuture.completedFuture(stream));
        when(stream.properties()).thenReturn(Map.of());
        when(catalog.setStreamProperties(identifier, updatedConfig))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(catalog.createLog(KafkaLogNaming.logName(tp), LogId.of(1L)))
                .thenReturn(logInstance);
        when(logInstance.getFirstOffset()).thenReturn(CompletableFuture.completedFuture(firstOffset));
        when(logInstance.getLastOffset()).thenReturn(CompletableFuture.completedFuture(lastOffset));
        when(logInstance.computeRetentionTrimOffset(42L, 120_000L, -1L))
                .thenReturn(CompletableFuture.completedFuture(10L));
        when(logInstance.softTrim(10L)).thenReturn(CompletableFuture.completedFuture(null));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null, null);
        try (UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                holder,
                Map.of(
                        TopicConfig.RETENTION_MS_CONFIG, -1L,
                        TopicConfig.RETENTION_BYTES_CONFIG, -1L,
                        ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_CONFIG, 60_000L),
                ignored -> Map.of())) {
            state.getOrCreatePartitionLog(tp);
            state.updateTopicConfigAsync(tp, updatedConfig).get(5, TimeUnit.SECONDS);

            verify(logInstance).softTrim(10L);
        }
    }

    @Test
    void testTopicConfigPersistenceFailureDoesNotSuppressRetention() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("retention-config-failure", 0));
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        Stream stream = mock(Stream.class);
        Log logInstance = mock(Log.class);
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);
        Map<String, String> updatedConfig = Map.of(TopicConfig.RETENTION_MS_CONFIG, "120000");
        LogOffset firstOffset = mockOffset(5L);
        LogOffset lastOffset = mockOffset(42L);

        when(catalog.generateStreamId(Optional.of(KafkaLogNaming.logName(tp))))
                .thenReturn(CompletableFuture.completedFuture(1L));
        when(catalog.registerExternalPartition(identifier, 0, 1L, Map.of()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(catalog.streamExists(identifier))
                .thenReturn(CompletableFuture.completedFuture(true))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("catalog unavailable")));
        when(catalog.loadStream(identifier)).thenReturn(CompletableFuture.completedFuture(stream));
        when(stream.properties()).thenReturn(Map.of());
        when(catalog.createLog(KafkaLogNaming.logName(tp), LogId.of(1L)))
                .thenReturn(logInstance);
        when(logInstance.getFirstOffset()).thenReturn(CompletableFuture.completedFuture(firstOffset));
        when(logInstance.getLastOffset()).thenReturn(CompletableFuture.completedFuture(lastOffset));
        when(logInstance.computeRetentionTrimOffset(42L, 120_000L, -1L))
                .thenReturn(CompletableFuture.completedFuture(10L));
        when(logInstance.softTrim(10L)).thenReturn(CompletableFuture.completedFuture(11L));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null, null);
        try (UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                holder,
                Map.of(
                        TopicConfig.RETENTION_MS_CONFIG, -1L,
                        TopicConfig.RETENTION_BYTES_CONFIG, -1L,
                        ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_CONFIG, 60_000L),
                ignored -> Map.of())) {
            state.getOrCreatePartitionLog(tp);

            assertThrows(
                    ExecutionException.class,
                    () -> state.updateTopicConfigAsync(tp, updatedConfig).get(5, TimeUnit.SECONDS));
            verify(logInstance).softTrim(10L);
        }
    }

    @Test
    void testCleanupWaitsForSubmittedTrimBeforeFencingLog() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("in-flight-trim", 0));
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        Log logInstance = mock(Log.class);
        LogOffset firstOffset = mockOffset(5L);
        LogOffset lastOffset = mockOffset(42L);
        CountDownLatch trimAwaited = new CountDownLatch(1);
        CompletableFuture<Long> trimResult = new CompletableFuture<>() {
            @Override
            public <U> CompletableFuture<U> handle(
                    BiFunction<? super Long, Throwable, ? extends U> function) {
                trimAwaited.countDown();
                return super.handle(function);
            }
        };

        when(catalog.generateStreamId(Optional.of(KafkaLogNaming.logName(tp))))
                .thenReturn(CompletableFuture.completedFuture(1L));
        when(catalog.createLog(KafkaLogNaming.logName(tp), LogId.of(1L)))
                .thenReturn(logInstance);
        when(logInstance.getFirstOffset()).thenReturn(CompletableFuture.completedFuture(firstOffset));
        when(logInstance.getLastOffset()).thenReturn(CompletableFuture.completedFuture(lastOffset));
        when(logInstance.computeRetentionTrimOffset(42L, 120_000L, -1L))
                .thenReturn(CompletableFuture.completedFuture(10L));
        when(logInstance.softTrim(10L)).thenReturn(trimResult);

        Map<String, Object> defaults = Map.of(
                TopicConfig.RETENTION_MS_CONFIG, 120_000L,
                TopicConfig.RETENTION_BYTES_CONFIG, -1L,
                ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_CONFIG, 60_000L);
        try (UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                catalog,
                defaults,
                ignored -> Map.of())) {
            state.getOrCreatePartitionLog(tp);
            verify(logInstance).softTrim(10L);

            CompletableFuture<Boolean> cleanupFuture = CompletableFuture.supplyAsync(
                    () -> state.cleanupPartition(tp, false));
            assertTrue(trimAwaited.await(5, TimeUnit.SECONDS));
            assertFalse(cleanupFuture.isDone());
            verify(logInstance, never()).fence();
            verify(logInstance, never()).close();

            trimResult.complete(11L);
            assertTrue(cleanupFuture.get(5, TimeUnit.SECONDS));

            org.mockito.InOrder closeOrder = org.mockito.Mockito.inOrder(logInstance);
            closeOrder.verify(logInstance).softTrim(10L);
            closeOrder.verify(logInstance).fence();
            closeOrder.verify(logInstance).close();
        }
    }

    @Test
    void testCleanupPreventsLateRetentionTrim() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("late-retention", 0));
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        Log logInstance = mock(Log.class);
        LogOffset firstOffset = mockOffset(5L);
        LogOffset lastOffset = mockOffset(42L);
        CompletableFuture<Long> trimOffset = new CompletableFuture<>();

        when(catalog.generateStreamId(Optional.of(KafkaLogNaming.logName(tp))))
                .thenReturn(CompletableFuture.completedFuture(1L));
        when(catalog.createLog(KafkaLogNaming.logName(tp), LogId.of(1L)))
                .thenReturn(logInstance);
        when(logInstance.getFirstOffset()).thenReturn(CompletableFuture.completedFuture(firstOffset));
        when(logInstance.getLastOffset()).thenReturn(CompletableFuture.completedFuture(lastOffset));
        when(logInstance.computeRetentionTrimOffset(42L, 120_000L, -1L)).thenReturn(trimOffset);

        Map<String, Object> defaults = Map.of(
                TopicConfig.RETENTION_MS_CONFIG, 120_000L,
                TopicConfig.RETENTION_BYTES_CONFIG, -1L,
                ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_CONFIG, 60_000L);
        try (UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                catalog,
                defaults,
                ignored -> Map.of())) {
            state.getOrCreatePartitionLog(tp);
            verify(logInstance).computeRetentionTrimOffset(42L, 120_000L, -1L);

            state.cleanupPartition(tp, false);
            trimOffset.complete(10L);

            verify(logInstance, never()).softTrim(10L);
        }
    }

    private static UrsaStorageState newState(IndexedStreamCatalog catalog) {
        return new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                catalog);
    }

    private static LogOffset mockOffset(long offset) {
        LogOffset logOffset = mock(LogOffset.class);
        when(logOffset.offset()).thenReturn(offset);
        return logOffset;
    }
}
