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
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.diskless.DisklessClientZone;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.streamnative.lakestream.api.Log;
import io.streamnative.lakestream.api.LogEntryHeader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UrsaLakestreamWriterNonIdempotentTest {

    @Test
    void testNewPartitionLogActivatesThenFencesBeforeClose() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        UrsaStorageState state = mock(UrsaStorageState.class);

        UrsaPartitionLog partitionLog = newPartitionLog(
                state,
                tp,
                CompletableFuture.completedFuture(logInstance));

        InOrder lifecycle = inOrder(logInstance);
        lifecycle.verify(logInstance).activate();

        partitionLog.close();

        lifecycle.verify(logInstance).fence();
        lifecycle.verify(logInstance).close();
    }

    @Test
    void testWriteAfterCloseReturnsNotLeaderWithoutAppending() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        UrsaStorageState state = mock(UrsaStorageState.class);
        UrsaPartitionLog partitionLog = attachPartitionLog(state, tp, logInstance);
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord("after-close".getBytes(StandardCharsets.UTF_8)));

        partitionLog.close();

        PartitionResponse response = partitionLog.write(
                records,
                DisklessClientZone.NO_ZONE,
                "test").get(5, TimeUnit.SECONDS);

        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, response.error);
        verify(logInstance, never()).append(anyInt(), any(ByteBuf.class));
    }

    @Test
    void testLateStaleLogOpenClosesWithoutFencingActiveReplacement() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        AtomicReference<UrsaPartitionLog> activePartitionLog = new AtomicReference<>();
        when(state.partitionLog(tp)).thenAnswer(invocation -> activePartitionLog.get());
        when(state.time()).thenReturn(Time.SYSTEM);

        CompletableFuture<Log> staleOpenFuture = new CompletableFuture<>();
        Log staleLog = mock(Log.class);
        UrsaPartitionLog stalePartitionLog = newPartitionLog(state, tp, staleOpenFuture);
        activePartitionLog.set(stalePartitionLog);

        stalePartitionLog.invalidate();
        activePartitionLog.compareAndSet(stalePartitionLog, null);

        Log replacementLog = mock(Log.class);
        UrsaPartitionLog replacementPartitionLog = newPartitionLog(
                state,
                tp,
                CompletableFuture.completedFuture(replacementLog));
        activePartitionLog.set(replacementPartitionLog);

        staleOpenFuture.complete(staleLog);

        verify(replacementLog).activate();
        verify(replacementLog, never()).fence();
        verify(staleLog, never()).activate();
        verify(staleLog, never()).fence();
        verify(staleLog).close();

        replacementPartitionLog.close();
    }

    @Test
    void testNonIdempotentWriteAppendsValidRecordsAndReleasesBuffer() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        CompletableFuture<LogEntryHeader> appendFuture = new CompletableFuture<>();
        AtomicReference<ByteBuf> appendedBuffer = new AtomicReference<>();

        when(logInstance.append(anyInt(), any(ByteBuf.class))).thenAnswer(invocation -> {
            int numberOfMessages = invocation.getArgument(0);
            ByteBuf buffer = invocation.getArgument(1);
            appendedBuffer.set(buffer);

            MemoryRecords appendedRecords = decodeKafkaRecords(buffer);
            RecordBatch batch = appendedRecords.batches().iterator().next();
            assertEquals(2, numberOfMessages);
            assertTrue(batch.isValid());
            assertFalse(batch.hasProducerId());
            assertEquals(1, buffer.refCnt());
            return appendFuture;
        });

        UrsaLakestreamWriter writer = new UrsaLakestreamWriter(newWriterState(tp, logInstance));
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord("a".getBytes(StandardCharsets.UTF_8)),
                new SimpleRecord("b".getBytes(StandardCharsets.UTF_8)));

        try {
            CompletableFuture<Map<TopicIdPartition, PartitionResponse>> writeFuture =
                    writer.write(Map.of(tp, records), DisklessClientZone.NO_ZONE);
            assertFalse(writeFuture.isDone());
            TestUtils.waitForCondition(
                    () -> appendedBuffer.get() != null,
                    5_000L,
                    "Timed out waiting for the append call");
            assertEquals(1, appendedBuffer.get().refCnt());

            appendFuture.complete(mockLogEntryHeader(0L));

            PartitionResponse response = writeFuture.get(5, TimeUnit.SECONDS).get(tp);
            assertEquals(Errors.NONE, response.error);
            assertEquals(0L, response.baseOffset);
            assertEquals(0, appendedBuffer.get().refCnt());
        } finally {
            writer.close();
        }
    }

    @Test
    void testNonIdempotentWritesSamePartitionCanSubmitThroughSequencedWrites() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        CountDownLatch firstAppendSubmitted = new CountDownLatch(1);
        CountDownLatch secondAppendSubmitted = new CountDownLatch(1);
        AtomicReference<CompletableFuture<LogEntryHeader>> firstAppendFuture = new AtomicReference<>();
        AtomicReference<CompletableFuture<LogEntryHeader>> secondAppendFuture = new AtomicReference<>();
        AtomicInteger appendCalls = new AtomicInteger(0);

        when(logInstance.append(anyInt(), any(ByteBuf.class))).thenAnswer(invocation -> {
            ByteBuf buffer = invocation.getArgument(1);
            MemoryRecords appendedRecords = decodeKafkaRecords(buffer);
            RecordBatch batch = appendedRecords.batches().iterator().next();
            assertTrue(batch.isValid());
            assertFalse(batch.hasProducerId());

            CompletableFuture<LogEntryHeader> future = new CompletableFuture<>();
            int appendIndex = appendCalls.getAndIncrement();
            if (appendIndex == 0) {
                firstAppendFuture.set(future);
                firstAppendSubmitted.countDown();
            } else if (appendIndex == 1) {
                secondAppendFuture.set(future);
                secondAppendSubmitted.countDown();
            } else {
                fail("Unexpected append index " + appendIndex);
            }
            return future;
        });

        UrsaLakestreamWriter writer = new UrsaLakestreamWriter(newWriterState(tp, logInstance));
        MemoryRecords records1 = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord("first".getBytes(StandardCharsets.UTF_8)));
        MemoryRecords records2 = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord("second".getBytes(StandardCharsets.UTF_8)));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            CompletableFuture<Map<TopicIdPartition, PartitionResponse>> firstWrite =
                    CompletableFuture.supplyAsync(
                            () -> writer.write(Map.of(tp, records1), DisklessClientZone.NO_ZONE), executor)
                            .thenCompose(future -> future);

            assertTrue(firstAppendSubmitted.await(5, TimeUnit.SECONDS),
                    "Timed out waiting for first non-idempotent append submission");

            CompletableFuture<Map<TopicIdPartition, PartitionResponse>> secondWrite =
                    CompletableFuture.supplyAsync(
                            () -> writer.write(Map.of(tp, records2), DisklessClientZone.NO_ZONE), executor)
                            .thenCompose(future -> future);

            assertTrue(secondAppendSubmitted.await(5, TimeUnit.SECONDS),
                    "Timed out waiting for second non-idempotent append submission");

            firstAppendFuture.get().complete(mockLogEntryHeader(0L));
            secondAppendFuture.get().complete(mockLogEntryHeader(1L));

            PartitionResponse first = firstWrite.get(5, TimeUnit.SECONDS).get(tp);
            PartitionResponse second = secondWrite.get(5, TimeUnit.SECONDS).get(tp);
            assertEquals(Errors.NONE, first.error);
            assertEquals(Errors.NONE, second.error);
            assertEquals(0L, first.baseOffset);
            assertEquals(1L, second.baseOffset);
        } finally {
            executor.shutdownNow();
            writer.close();
        }
    }

    @Test
    void testFencedLogReturnsNotLeaderAndInvalidatesPartitionLog() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        when(logInstance.append(anyInt(), any(ByteBuf.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("stream 17 is fenced")));

        UrsaStorageState state = mock(UrsaStorageState.class);
        UrsaPartitionLog partitionLog = attachPartitionLog(state, tp, logInstance);
        UrsaLakestreamWriter writer = new UrsaLakestreamWriter(state);
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord("a".getBytes(StandardCharsets.UTF_8)));

        try {
            PartitionResponse response = writer.write(Map.of(tp, records), DisklessClientZone.NO_ZONE)
                    .get(5, TimeUnit.SECONDS).get(tp);
            assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, response.error);
            verify(state).removePartitionLog(eq(tp), same(partitionLog));
            verify(logInstance).fence();
            verify(logInstance).close();
        } finally {
            writer.close();
        }
    }

    @Test
    void testAppendFailureReturnsStorageErrorWithoutInvalidationAndReleasesBuffer() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        CompletableFuture<LogEntryHeader> appendFuture = new CompletableFuture<>();
        AtomicReference<ByteBuf> appendedBuffer = new AtomicReference<>();
        when(logInstance.append(anyInt(), any(ByteBuf.class))).thenAnswer(invocation -> {
            appendedBuffer.set(invocation.getArgument(1));
            return appendFuture;
        });

        UrsaStorageState state = mock(UrsaStorageState.class);
        UrsaPartitionLog partitionLog = attachPartitionLog(state, tp, logInstance);
        UrsaLakestreamWriter writer = new UrsaLakestreamWriter(state);
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord("a".getBytes(StandardCharsets.UTF_8)));

        try {
            CompletableFuture<Map<TopicIdPartition, PartitionResponse>> writeFuture =
                    writer.write(Map.of(tp, records), DisklessClientZone.NO_ZONE);
            TestUtils.waitForCondition(
                    () -> appendedBuffer.get() != null,
                    5_000L,
                    "Timed out waiting for the append call");
            assertEquals(1, appendedBuffer.get().refCnt());

            appendFuture.completeExceptionally(new IOException("temporary storage failure"));

            PartitionResponse response = writeFuture.get(5, TimeUnit.SECONDS).get(tp);
            assertEquals(Errors.KAFKA_STORAGE_ERROR, response.error);
            assertEquals(0, appendedBuffer.get().refCnt());
            verify(state, never()).removePartitionLog(eq(tp), same(partitionLog));
        } finally {
            writer.close();
        }
    }

    private static UrsaStorageState newWriterState(TopicIdPartition tp, Log logInstance) {
        UrsaStorageState state = mock(UrsaStorageState.class);
        attachPartitionLog(state, tp, logInstance);
        return state;
    }

    private static UrsaPartitionLog attachPartitionLog(
            UrsaStorageState state,
            TopicIdPartition tp,
            Log logInstance) {
        when(state.time()).thenReturn(Time.SYSTEM);
        UrsaPartitionLog partitionLog = newPartitionLog(
                state,
                tp,
                CompletableFuture.completedFuture(logInstance));
        when(state.getOrCreatePartitionLog(tp)).thenReturn(partitionLog);
        return partitionLog;
    }

    private static UrsaPartitionLog newPartitionLog(
            UrsaStorageState state,
            TopicIdPartition tp,
            CompletableFuture<Log> logFuture) {
        return new UrsaPartitionLog(
                tp,
                state,
                mock(DisklessLogMetrics.class),
                logFuture,
                null,
                0L,
                0,
                null);
    }

    private static TopicIdPartition testTopicPartition() {
        return new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 0));
    }

    private static LogEntryHeader mockLogEntryHeader(long offset) {
        LogEntryHeader header = mock(LogEntryHeader.class);
        when(header.offset()).thenReturn(offset);
        return header;
    }

    private static MemoryRecords decodeKafkaRecords(ByteBuf buffer) {
        return MemoryRecords.readableRecords(KafkaEntryFormatter.decode(buffer.duplicate()));
    }
}
