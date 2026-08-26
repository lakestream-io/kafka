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
package org.apache.kafka.storage.diskless.idempotent;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.storage.diskless.handlers.KafkaRecordsPayload;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.streamnative.lakestream.api.Log;
import io.streamnative.lakestream.api.LogCursor;
import io.streamnative.lakestream.api.LogEntry;
import io.streamnative.lakestream.api.LogOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ProducerStateManagerTest {

    @Test
    void testFirstWriteMustStartAtSequenceZero() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        ProducerStateManager manager = newManager(tp, () -> null, emptyLogSupplier());
        try {
            ProducerStateManager.PrepareResult result = manager.prepareAppend(List.of(
                batch(1L, (short) 0, 5, 9, 5, 1000L)
            )).get();

            assertInstanceOf(ProducerStateManager.OutOfOrderSequence.class, result);
            ProducerStateManager.OutOfOrderSequence outOfOrder = (ProducerStateManager.OutOfOrderSequence) result;
            assertEquals(0, outOfOrder.expectedSequence());
            assertEquals(5, outOfOrder.actualSequence());
        } finally {
            manager.close();
        }
    }

    @Test
    void testStaleEpochRejected() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        ProducerStateManager manager = newManager(tp, () -> null, emptyLogSupplier());
        try {
            ProducerStateManager.PendingAppend firstPending =
                ready(manager.prepareAppend(List.of(batch(1L, (short) 5, 0, 9, 10, 1000L))).get());
            manager.completeAppend(firstPending, 0L, 1000L);

            ProducerStateManager.PrepareResult staleEpoch = manager.prepareAppend(List.of(
                batch(1L, (short) 3, 10, 19, 10, 2000L)
            )).get();

            assertInstanceOf(ProducerStateManager.InvalidEpoch.class, staleEpoch);
            ProducerStateManager.InvalidEpoch invalidEpoch = (ProducerStateManager.InvalidEpoch) staleEpoch;
            assertEquals((short) 5, invalidEpoch.currentEpoch());
            assertEquals((short) 3, invalidEpoch.requestEpoch());
        } finally {
            manager.close();
        }
    }

    @Test
    void testOutOfOrderSequenceRejected() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        ProducerStateManager manager = newManager(tp, () -> null, emptyLogSupplier());
        try {
            ProducerStateManager.PendingAppend firstPending =
                ready(manager.prepareAppend(List.of(batch(1L, (short) 0, 0, 9, 10, 1000L))).get());
            manager.completeAppend(firstPending, 0L, 1000L);

            ProducerStateManager.PrepareResult outOfOrder = manager.prepareAppend(List.of(
                batch(1L, (short) 0, 20, 29, 10, 2000L)
            )).get();

            assertInstanceOf(ProducerStateManager.OutOfOrderSequence.class, outOfOrder);
            ProducerStateManager.OutOfOrderSequence result = (ProducerStateManager.OutOfOrderSequence) outOfOrder;
            assertEquals(10, result.expectedSequence());
            assertEquals(20, result.actualSequence());
        } finally {
            manager.close();
        }
    }

    @Test
    void testDuplicateReturnsCachedOffset() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        ProducerStateManager manager = newManager(tp, () -> null, emptyLogSupplier());
        try {
            ProducerStateManager.PendingAppend firstPending =
                ready(manager.prepareAppend(List.of(batch(1L, (short) 0, 0, 9, 10, 1000L))).get());
            manager.completeAppend(firstPending, 100L, 777L);

            ProducerStateManager.PrepareResult duplicate = manager.prepareAppend(List.of(
                batch(1L, (short) 0, 0, 9, 10, 2000L)
            )).get();

            assertInstanceOf(ProducerStateManager.Duplicate.class, duplicate);
            ProducerStateManager.AppendResult appendResult =
                ((ProducerStateManager.Duplicate) duplicate).appendResultFuture().get();
            assertEquals(100L, appendResult.baseOffset());
            assertEquals(777L, appendResult.timestamp());
        } finally {
            manager.close();
        }
    }

    @Test
    void testInFlightDuplicateReusesFuture() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        ProducerStateManager manager = newManager(tp, () -> null, emptyLogSupplier());
        try {
            ProducerStateManager.PendingAppend firstPending =
                ready(manager.prepareAppend(List.of(batch(1L, (short) 0, 0, 9, 10, 1000L))).get());

            ProducerStateManager.PrepareResult duplicate = manager.prepareAppend(List.of(
                batch(1L, (short) 0, 0, 9, 10, 2000L)
            )).get();
            assertInstanceOf(ProducerStateManager.Duplicate.class, duplicate);

            CompletableFuture<ProducerStateManager.AppendResult> duplicateFuture =
                ((ProducerStateManager.Duplicate) duplicate).appendResultFuture();
            assertFalse(duplicateFuture.isDone());

            manager.completeAppend(firstPending, 50L, 555L);
            ProducerStateManager.AppendResult appendResult = duplicateFuture.get();
            assertEquals(50L, appendResult.baseOffset());
            assertEquals(555L, appendResult.timestamp());
        } finally {
            manager.close();
        }
    }

    @Test
    void testSnapshotAndReplayRecovery() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();

        ProducerStateManager manager1 = newManager(tp, snapshotStore::client, emptyLogSupplier());
        ProducerStateManager manager2 = null;
        try {
            ProducerStateManager.PendingAppend firstPending =
                ready(manager1.prepareAppend(List.of(batch(1L, (short) 0, 0, 1, 2, 1000L))).get());
            manager1.completeAppend(firstPending, 0L, 1000L);
            manager1.takeSnapshot("test").get();

            LogEntry firstReplayEntry = newReplayEntry(1L, (short) 0, 2, "r2", 2L, 1);
            LogEntry secondReplayEntry = newReplayEntry(1L, (short) 0, 3, "r3", 3L, 1);
            Log replayLog = replayLog(List.of(firstReplayEntry, secondReplayEntry), 3L, 1);
            manager2 = newManager(tp, snapshotStore::client, () -> CompletableFuture.completedFuture(replayLog));

            ProducerStateManager.PrepareResult recovered = manager2.prepareAppend(List.of(
                batch(1L, (short) 0, 4, 4, 1, 3000L)
            )).get();
            assertInstanceOf(ProducerStateManager.Ready.class, recovered);
            verify(firstReplayEntry).close();
            verify(secondReplayEntry).close();
        } finally {
            manager1.close();
            if (manager2 != null) {
                manager2.close();
            }
        }
    }

    @Test
    void testReplayRebasesEveryBatchInRawMemoryRecords() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        LogEntry replayEntry = newMultiBatchReplayEntry(10L);
        Log replayLog = replayLog(replayEntry, 10L, 3);
        ProducerStateManager manager = newManager(
                tp, () -> null, () -> CompletableFuture.completedFuture(replayLog));

        try {
            ProducerStateManager.PrepareResult duplicate = manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 2, 2, 1, 3000L)
            )).get();
            assertInstanceOf(ProducerStateManager.Duplicate.class, duplicate);
            ProducerStateManager.AppendResult duplicateResult =
                    ((ProducerStateManager.Duplicate) duplicate).appendResultFuture().get();
            assertEquals(12L, duplicateResult.baseOffset());
            verify(replayEntry).close();
        } finally {
            manager.close();
        }
    }

    @Test
    void testCorruptedSnapshotIgnoredAndOverwritten() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();
        String snapshotKey = ProducerStateSnapshotKeys.snapshotKey(tp.topicId().toString(), tp.partition());
        snapshotStore.data().put(snapshotKey, "bad-snapshot".getBytes(StandardCharsets.UTF_8));

        ProducerStateManager manager = newManager(tp, snapshotStore::client, emptyLogSupplier());
        try {
            ProducerStateManager.PendingAppend pending =
                ready(manager.prepareAppend(List.of(batch(1L, (short) 0, 0, 0, 1, 1000L))).get());
            manager.completeAppend(pending, 0L, 1000L);
            manager.takeSnapshot("overwrite").get();

            byte[] updated = snapshotStore.data().get(snapshotKey);
            Map<Long, ProducerStateEntry> snapshot = ProducerStateSerDes.deserialize(updated);
            assertEquals(1, snapshot.size());
        } finally {
            manager.close();
        }
    }

    @Test
    void testSnapshotRecoveryIsScopedByZone() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();
        SupplierWithException<CompletableFuture<Log>> logSupplier = emptyLogSupplier();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        ProducerStateManager zoneAManager = null;
        ProducerStateManager zoneBManager = null;
        ProducerStateManager zoneAReloadedManager = null;
        try {
            zoneAManager = new ProducerStateManager(
                tp,
                snapshotStore::client,
                logSupplier::get,
                " zone-a ",
                0L,
                ProducerStateManager.DEFAULT_SNAPSHOT_RECORD_THRESHOLD,
                scheduler);
            ProducerStateManager.PendingAppend initialAppend = ready(zoneAManager.prepareAppend(List.of(
                batch(1L, (short) 0, 0, 0, 1, 1000L)
            )).get());
            zoneAManager.completeAppend(initialAppend, 0L, 1000L);
            zoneAManager.takeSnapshot("zone-a").get();
            zoneAManager.close();
            zoneAManager = null;

            String zoneAKey = ProducerStateSnapshotKeys.snapshotKey(
                tp.topicId().toString(), tp.partition(), "zone-a");
            assertTrue(snapshotStore.data().containsKey(zoneAKey));

            zoneBManager = new ProducerStateManager(
                tp,
                snapshotStore::client,
                logSupplier::get,
                "zone-b",
                0L,
                ProducerStateManager.DEFAULT_SNAPSHOT_RECORD_THRESHOLD,
                scheduler);
            ProducerStateManager.PrepareResult zoneBResult = zoneBManager.prepareAppend(List.of(
                batch(1L, (short) 0, 1, 1, 1, 2000L)
            )).get();
            assertInstanceOf(ProducerStateManager.OutOfOrderSequence.class, zoneBResult);
            ProducerStateManager.OutOfOrderSequence zoneBOutOfOrder =
                (ProducerStateManager.OutOfOrderSequence) zoneBResult;
            assertEquals(0, zoneBOutOfOrder.expectedSequence());
            zoneBManager.close();
            zoneBManager = null;

            zoneAReloadedManager = new ProducerStateManager(
                tp,
                snapshotStore::client,
                logSupplier::get,
                "zone-a",
                0L,
                ProducerStateManager.DEFAULT_SNAPSHOT_RECORD_THRESHOLD,
                scheduler);
            ProducerStateManager.PendingAppend recoveredAppend = ready(zoneAReloadedManager.prepareAppend(List.of(
                batch(1L, (short) 0, 1, 1, 1, 3000L)
            )).get());
            zoneAReloadedManager.completeAppend(recoveredAppend, 1L, 3000L);
        } finally {
            if (zoneAManager != null) {
                zoneAManager.close();
            }
            if (zoneBManager != null) {
                zoneBManager.close();
            }
            if (zoneAReloadedManager != null) {
                zoneAReloadedManager.close();
            }
            scheduler.shutdownNow();
        }
    }

    @Test
    void testReplayStopsWhenFirstEntryHasNoProducerAndNoSnapshot() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        LogEntry nonIdempotentReplayEntry = newReplayEntryWithoutProducer("legacy", 0L, 1);
        LogEntry idempotentReplayEntry = newReplayEntry(1L, (short) 0, 0, "idempotent", 1L, 1);
        Log replayLog = replayLog(List.of(nonIdempotentReplayEntry, idempotentReplayEntry), 1L, 1);

        ProducerStateManager manager = newManager(tp, () -> null, () -> CompletableFuture.completedFuture(replayLog));
        try {
            ProducerStateManager.PrepareResult result = manager.prepareAppend(List.of(
                batch(1L, (short) 0, 0, 0, 1, 3000L)
            )).get();
            assertInstanceOf(ProducerStateManager.Ready.class, result);
            verify(nonIdempotentReplayEntry).close();
            verify(idempotentReplayEntry).close();
        } finally {
            manager.close();
        }
    }

    @Test
    void testReplayFailureClosesEntireEntryBatch() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        RuntimeException entryFailure = new RuntimeException("entry offset unavailable");
        LogEntry failingEntry = mock(LogEntry.class);
        when(failingEntry.offset()).thenThrow(entryFailure);
        LogEntry unvisitedEntry = newReplayEntry(1L, (short) 0, 0, "unvisited", 1L, 1);
        Log replayLog = replayLog(List.of(failingEntry, unvisitedEntry), 1L, 1);

        ProducerStateManager manager = newManager(
                tp, () -> null, () -> CompletableFuture.completedFuture(replayLog));
        try {
            ExecutionException prepareError = assertThrows(ExecutionException.class, () -> manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 0, 0, 1, 3000L)
            )).get());
            Throwable rootCause = prepareError;
            while (rootCause.getCause() != null) {
                rootCause = rootCause.getCause();
            }

            assertSame(entryFailure, rootCause);
            verify(failingEntry).close();
            verify(unvisitedEntry).close();
        } finally {
            manager.close();
        }
    }

    @Test
    void testCorruptReplayEntryFailsRecoveryAndClosesEntireEntryBatch() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        LogEntry corruptEntry = newCorruptReplayEntry(0L);
        LogEntry unvisitedEntry = newReplayEntry(1L, (short) 0, 0, "unvisited", 1L, 1);
        Log replayLog = replayLog(List.of(corruptEntry, unvisitedEntry), 1L, 1);

        ProducerStateManager manager = newManager(
                tp, () -> null, () -> CompletableFuture.completedFuture(replayLog));
        try {
            assertThrows(ExecutionException.class, () -> manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 1, 1, 1, 3000L)
            )).get());
            verify(corruptEntry).close();
            verify(unvisitedEntry).close();
            verify(unvisitedEntry, never()).payload();
        } finally {
            manager.close();
        }
    }

    @Test
    void testLateReplayReadAfterManagerCloseClosesEntriesAndCursor() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        LogOffset lastOffset = mockLogOffset(0L, 1);
        CompletableFuture<List<LogEntry>> readFuture = new CompletableFuture<>();
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(lastOffset));
        when(logInstance.openEphemeralCursor(anyString(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(cursor));
        when(cursor.readEntries(anyInt(), anyLong(), any(), anyLong())).thenReturn(readFuture);

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        ProducerStateManager manager = new ProducerStateManager(
                tp,
                () -> null,
                () -> CompletableFuture.completedFuture(logInstance),
                "test-zone",
                0L,
                ProducerStateManager.DEFAULT_SNAPSHOT_RECORD_THRESHOLD,
                scheduler);
        try {
            CompletableFuture<ProducerStateManager.PrepareResult> prepareFuture = manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 0, 0, 1, 3000L)
            ));
            verify(cursor, timeout(5_000)).readEntries(anyInt(), anyLong(), any(), anyLong());
            assertFalse(prepareFuture.isDone());

            manager.close();
            assertThrows(ExecutionException.class, prepareFuture::get);

            LogEntry lateEntry = newReplayEntry(1L, (short) 0, 0, "late", 0L, 1);
            readFuture.complete(List.of(lateEntry));

            verify(lateEntry, timeout(5_000)).close();
            verify(lateEntry, never()).payload();
            verify(cursor, timeout(5_000)).close();
            verify(cursor).readEntries(anyInt(), anyLong(), any(), anyLong());
        } finally {
            manager.close();
            scheduler.shutdownNow();
        }
    }

    @Test
    void testSynchronousReplayReadFailureCompletesPreparationAndClosesCursor() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        RuntimeException readFailure = new RuntimeException("synchronous replay read failure");
        LogOffset lastOffset = mockLogOffset(0L, 1);
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(lastOffset));
        when(logInstance.openEphemeralCursor(anyString(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(cursor));
        when(cursor.readEntries(anyInt(), anyLong(), any(), anyLong())).thenThrow(readFailure);

        ProducerStateManager manager = newManager(
                tp, () -> null, () -> CompletableFuture.completedFuture(logInstance));
        try {
            CompletableFuture<ProducerStateManager.PrepareResult> prepareFuture = manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 0, 0, 1, 3000L)
            ));

            ExecutionException prepareError = assertThrows(
                    ExecutionException.class,
                    () -> prepareFuture.get(5, TimeUnit.SECONDS));
            Throwable rootCause = prepareError;
            while (rootCause.getCause() != null) {
                rootCause = rootCause.getCause();
            }
            assertSame(readFailure, rootCause);
            assertTrue(prepareFuture.isCompletedExceptionally());
            verify(cursor, timeout(5_000)).close();
        } finally {
            manager.close();
        }
    }

    @Test
    void testReplaySkippedWhenNoSnapshotAndMessagesExceedThreshold() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        LogOffset lastOffset = mockLogOffset(0L, 10_001);
        when(logInstance.getLastOffset()).thenReturn(CompletableFuture.completedFuture(lastOffset));

        ProducerStateManager manager = newManager(tp, () -> null, () -> CompletableFuture.completedFuture(logInstance));
        try {
            // Producer 1: first append bypasses sequence check once after recovery is skipped.
            ProducerStateManager.PrepareResult firstProducerFirst = manager.prepareAppend(List.of(
                batch(1L, (short) 0, 5, 5, 1, 3000L)
            )).get();
            assertInstanceOf(ProducerStateManager.Ready.class, firstProducerFirst);
            manager.completeAppend(((ProducerStateManager.Ready) firstProducerFirst).pendingAppend(), 0L, 3000L);

            // Producer 2: also gets one bypass because recovery-skip bypass is tracked per producer.
            ProducerStateManager.PrepareResult secondProducerFirst = manager.prepareAppend(List.of(
                batch(2L, (short) 0, 7, 7, 1, 3001L)
            )).get();
            assertInstanceOf(ProducerStateManager.Ready.class, secondProducerFirst);
            manager.completeAppend(((ProducerStateManager.Ready) secondProducerFirst).pendingAppend(), 1L, 3001L);

            // Producer 1: after its first bypass, validation should be strict again.
            ProducerStateManager.PrepareResult firstProducerSecond = manager.prepareAppend(List.of(
                batch(1L, (short) 0, 7, 7, 1, 3002L)
            )).get();
            assertInstanceOf(ProducerStateManager.OutOfOrderSequence.class, firstProducerSecond);
            verify(logInstance, never()).openEphemeralCursor(anyString(), anyLong());
        } finally {
            manager.close();
        }
    }

    @Test
    void testBypassedPrepareAbortDoesNotGrantSecondBypassForSameProducer() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        LogOffset lastOffset = mockLogOffset(0L, 10_001);
        when(logInstance.getLastOffset()).thenReturn(CompletableFuture.completedFuture(lastOffset));

        try (ProducerStateManager manager = newManager(
                tp, () -> null, () -> CompletableFuture.completedFuture(logInstance))) {
            ProducerStateManager.PrepareResult first = manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 5, 5, 1, 3000L)
            )).get();
            assertInstanceOf(ProducerStateManager.Ready.class, first);

            ProducerStateManager.PendingAppend pendingAppend = ((ProducerStateManager.Ready) first).pendingAppend();
            // Simulate append failure and rollback to verify boundary behavior after a bypassed prepare.
            manager.abortAppend(pendingAppend, new RuntimeException("abort first bypassed append"));

            ProducerStateManager.PrepareResult second = manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 5, 5, 1, 3001L)
            )).get();
            assertInstanceOf(ProducerStateManager.OutOfOrderSequence.class, second);

            ProducerStateManager.PrepareResult third = manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 0, 0, 1, 3002L)
            )).get();
            assertInstanceOf(ProducerStateManager.Ready.class, third);
            manager.completeAppend(((ProducerStateManager.Ready) third).pendingAppend(), 42L, 3002L);

            Optional<ProducerStateManager.ProducerState> producerState = manager.producerState(1L);
            assertTrue(producerState.isPresent());
            assertEquals((short) 0, producerState.get().producerEpoch());
            assertEquals(0, producerState.get().lastSequence());
            assertEquals(42L, producerState.get().lastOffset());
            assertEquals(1, producerState.get().retainedBatchCount());

            verify(logInstance, never()).openEphemeralCursor(anyString(), anyLong());
        }
    }

    private static ProducerStateManager newManager(
            TopicIdPartition tp,
            SupplierWithException<AsyncOxiaClient> oxiaClientSupplier,
            SupplierWithException<CompletableFuture<Log>> logSupplier) {
        return new ProducerStateManager(tp, oxiaClientSupplier::get, logSupplier::get);
    }

    private static ProducerStateManager.AppendBatch batch(
            long producerId,
            short producerEpoch,
            int firstSeq,
            int lastSeq,
            int recordCount,
            long timestamp) {
        return new ProducerStateManager.AppendBatch(
            producerId,
            producerEpoch,
            firstSeq,
            lastSeq,
            recordCount,
            timestamp);
    }

    private static ProducerStateManager.PendingAppend ready(ProducerStateManager.PrepareResult result) {
        return ((ProducerStateManager.Ready) result).pendingAppend();
    }

    private static TopicIdPartition testTopicPartition() {
        return new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 0));
    }

    private static SupplierWithException<CompletableFuture<Log>> emptyLogSupplier() {
        return () -> {
            Log logInstance = mock(Log.class);
            LogOffset emptyOffset = mockLogOffset(-1L, 0);
            when(logInstance.getLastOffset()).thenReturn(CompletableFuture.completedFuture(emptyOffset));
            return CompletableFuture.completedFuture(logInstance);
        };
    }

    private static LogOffset mockLogOffset(long offset, int numberOfRecords) {
        LogOffset logOffset = mock(LogOffset.class);
        when(logOffset.offset()).thenReturn(offset);
        when(logOffset.numberOfRecords()).thenReturn(numberOfRecords);
        when(logOffset.timestamp()).thenReturn(-1L);
        return logOffset;
    }

    private static Log replayLog(LogEntry replayEntry, long baseOffset, int numMessages) throws Exception {
        return replayLog(List.of(replayEntry), baseOffset, numMessages);
    }

    private static Log replayLog(List<LogEntry> replayEntries, long baseOffset, int numMessages) throws Exception {
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        LogOffset lastOffset = mockLogOffset(baseOffset, numMessages);

        when(logInstance.getLastOffset()).thenReturn(CompletableFuture.completedFuture(lastOffset));
        when(logInstance.openEphemeralCursor(anyString(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(cursor));

        AtomicInteger readCount = new AtomicInteger(0);
        when(cursor.readEntries(anyInt(), anyLong(), any(), anyLong())).thenAnswer(invocation -> {
            if (readCount.getAndIncrement() == 0) {
                return CompletableFuture.completedFuture(replayEntries);
            } else {
                return CompletableFuture.completedFuture(List.of());
            }
        });

        return logInstance;
    }

    private static LogEntry newReplayEntry(
            long producerId,
            short producerEpoch,
            int firstSeq,
            String value,
            long baseOffset,
            int numMessages) {
        MemoryRecords memoryRecords = MemoryRecords.withIdempotentRecords(
            Compression.NONE,
            producerId,
            producerEpoch,
            firstSeq,
            new SimpleRecord(value.getBytes(StandardCharsets.UTF_8)));

        ByteBuf data = KafkaRecordsPayload.copyForAppend(memoryRecords);

        LogEntry entry = mock(LogEntry.class);
        AtomicBoolean closed = new AtomicBoolean();
        when(entry.offset()).thenReturn(baseOffset);
        when(entry.numberOfRecords()).thenReturn(numMessages);
        when(entry.size()).thenReturn(data.readableBytes());
        when(entry.payload()).thenReturn(data.asReadOnly());
        doAnswer(invocation -> {
            if (closed.compareAndSet(false, true)) {
                data.release();
            }
            return null;
        }).when(entry).close();
        return entry;
    }

    private static LogEntry newCorruptReplayEntry(long baseOffset) {
        ByteBuf data = KafkaRecordsPayload.copyForAppend(MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord("corrupt".getBytes(StandardCharsets.UTF_8))));
        data.setByte(data.writerIndex() - 1, data.getByte(data.writerIndex() - 1) ^ 1);

        LogEntry entry = mock(LogEntry.class);
        AtomicBoolean closed = new AtomicBoolean();
        when(entry.offset()).thenReturn(baseOffset);
        when(entry.numberOfRecords()).thenReturn(1);
        when(entry.size()).thenReturn(data.readableBytes());
        when(entry.payload()).thenReturn(data.asReadOnly());
        doAnswer(invocation -> {
            if (closed.compareAndSet(false, true)) {
                data.release();
            }
            return null;
        }).when(entry).close();
        return entry;
    }

    private static LogEntry newMultiBatchReplayEntry(long baseOffset) {
        MemoryRecords firstBatch = MemoryRecords.withIdempotentRecords(
                Compression.NONE,
                1L,
                (short) 0,
                0,
                new SimpleRecord("first".getBytes(StandardCharsets.UTF_8)),
                new SimpleRecord("second".getBytes(StandardCharsets.UTF_8)));
        MemoryRecords secondBatch = MemoryRecords.withIdempotentRecords(
                Compression.NONE,
                1L,
                (short) 0,
                2,
                new SimpleRecord("third".getBytes(StandardCharsets.UTF_8)));
        ByteBuffer combined = ByteBuffer.allocate(
                firstBatch.buffer().remaining() + secondBatch.buffer().remaining());
        combined.put(firstBatch.buffer()).put(secondBatch.buffer()).flip();
        ByteBuf data = KafkaRecordsPayload.copyForAppend(MemoryRecords.readableRecords(combined));

        LogEntry entry = mock(LogEntry.class);
        AtomicBoolean closed = new AtomicBoolean();
        when(entry.offset()).thenReturn(baseOffset);
        when(entry.numberOfRecords()).thenReturn(3);
        when(entry.size()).thenReturn(data.readableBytes());
        when(entry.payload()).thenReturn(data.asReadOnly());
        doAnswer(invocation -> {
            if (closed.compareAndSet(false, true)) {
                data.release();
            }
            return null;
        }).when(entry).close();
        return entry;
    }

    private static LogEntry newReplayEntryWithoutProducer(
            String value,
            long baseOffset,
            int numMessages) {
        MemoryRecords memoryRecords = MemoryRecords.withRecords(
            Compression.NONE,
            new SimpleRecord(value.getBytes(StandardCharsets.UTF_8)));

        ByteBuf data = KafkaRecordsPayload.copyForAppend(memoryRecords);

        LogEntry entry = mock(LogEntry.class);
        AtomicBoolean closed = new AtomicBoolean();
        when(entry.offset()).thenReturn(baseOffset);
        when(entry.numberOfRecords()).thenReturn(numMessages);
        when(entry.size()).thenReturn(data.readableBytes());
        when(entry.payload()).thenReturn(data.asReadOnly());
        doAnswer(invocation -> {
            if (closed.compareAndSet(false, true)) {
                data.release();
            }
            return null;
        }).when(entry).close();
        return entry;
    }

    @FunctionalInterface
    private interface SupplierWithException<T> {
        T get();
    }

    private static final class InMemorySnapshotStore {
        private static final Version VERSION = new Version(
            1L, 0L, 0L, 0L, Optional.empty(), Optional.empty());

        private final Map<String, byte[]> data = new ConcurrentHashMap<>();
        private final AsyncOxiaClient client = mock(AsyncOxiaClient.class);

        InMemorySnapshotStore() throws Exception {
            when(client.get(anyString())).thenAnswer(invocation -> {
                String key = invocation.getArgument(0);
                byte[] value = data.get(key);
                if (value == null) {
                    return CompletableFuture.completedFuture(null);
                }
                return CompletableFuture.completedFuture(new GetResult(key, value, VERSION));
            });
            when(client.put(anyString(), any(byte[].class))).thenAnswer(invocation -> {
                String key = invocation.getArgument(0);
                byte[] value = invocation.getArgument(1);
                data.put(key, value);
                return CompletableFuture.completedFuture(new PutResult(key, VERSION));
            });
            when(client.delete(anyString())).thenAnswer(invocation -> {
                data.remove(invocation.getArgument(0));
                return CompletableFuture.completedFuture(true);
            });
        }

        Map<String, byte[]> data() {
            return data;
        }

        AsyncOxiaClient client() {
            return client;
        }
    }
}
