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
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionRequest;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionResponse;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.streamnative.lakestream.api.Log;
import io.streamnative.lakestream.api.LogCursor;
import io.streamnative.lakestream.api.LogEntry;
import io.streamnative.lakestream.api.LogOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UrsaLakestreamReaderTest {

    @Test
    void testListOffsetsEarliestUsesLogBoundaries() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(5L, 1)));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        ListOffsetsPartitionRequest request = new ListOffsetsPartitionRequest(
                tp, ListOffsetsPartitionRequest.EARLIEST_TIMESTAMP, Optional.empty());

        ListOffsetsPartitionResponse response = reader.listOffsets(Map.of(tp, request)).get().get(tp);

        assertNotNull(response);
        assertEquals(Errors.NONE, response.error());
        assertEquals(5L, response.offset());
        assertEquals(-1L, response.timestamp());
    }

    @Test
    void testListOffsetsLatestUsesLogHighWatermark() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(5L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 5)));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        ListOffsetsPartitionRequest request = new ListOffsetsPartitionRequest(
                tp, ListOffsetsPartitionRequest.LATEST_TIMESTAMP, Optional.empty());

        ListOffsetsPartitionResponse response = reader.listOffsets(Map.of(tp, request)).get().get(tp);

        assertNotNull(response);
        assertEquals(Errors.NONE, response.error());
        assertEquals(15L, response.offset());
        assertEquals(-1L, response.timestamp());
    }

    @Test
    void testFetchAtHighWatermarkReturnsEmptyRecords() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(5L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 5)));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        FetchRequest.PartitionData partitionData = fetchPartitionData(15L);

        FetchPartitionData response = reader.fetch(
                createFetchParams(), Map.of(tp, partitionData)).get().get(tp);

        assertNotNull(response);
        assertEquals(Errors.NONE, response.error);
        assertEquals(15L, response.highWatermark);
        assertEquals(5L, response.logStartOffset);
        assertEquals(MemoryRecords.EMPTY, response.records);
        verify(logInstance, never()).openEphemeralCursor(anyString(), anyLong());
    }

    @Test
    void testFetchBelowLogStartOffsetReturnsOffsetOutOfRange() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(5L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 5)));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);

        FetchPartitionData response = reader.fetch(
                createFetchParams(), Map.of(tp, fetchPartitionData(4L))).get().get(tp);

        assertNotNull(response);
        assertEquals(Errors.OFFSET_OUT_OF_RANGE, response.error);
        assertEquals(15L, response.highWatermark);
        assertEquals(5L, response.logStartOffset);
        assertEquals(MemoryRecords.EMPTY, response.records);
        verify(logInstance, never()).openEphemeralCursor(anyString(), anyLong());
    }

    @Test
    void testFetchAboveHighWatermarkReturnsOffsetOutOfRange() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(5L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 5)));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);

        FetchPartitionData response = reader.fetch(
                createFetchParams(), Map.of(tp, fetchPartitionData(16L))).get().get(tp);

        assertNotNull(response);
        assertEquals(Errors.OFFSET_OUT_OF_RANGE, response.error);
        assertEquals(15L, response.highWatermark);
        assertEquals(5L, response.logStartOffset);
        assertEquals(MemoryRecords.EMPTY, response.records);
        verify(logInstance, never()).openEphemeralCursor(anyString(), anyLong());
    }

    @Test
    void testFetchCopiesReadOnlyPayloadAndRebasesKafkaRecordOffsets() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 3)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 3)));
        when(logInstance.openEphemeralCursor(anyString(), eq(10L)))
                .thenReturn(CompletableFuture.completedFuture(cursor));
        LogEntry entry = createKafkaRecordsEntry(10L, new long[]{1000L, 1200L, 1500L});
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(13L)))
                .thenReturn(CompletableFuture.completedFuture(List.of(entry)));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);

        FetchPartitionData response = reader.fetch(
                createFetchParams(), Map.of(tp, fetchPartitionData(10L))).get().get(tp);

        assertNotNull(response);
        assertEquals(Errors.NONE, response.error);
        assertEquals(13L, response.highWatermark);
        assertEquals(10L, response.logStartOffset);
        List<Long> offsets = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        for (Record record : response.records.records()) {
            offsets.add(record.offset());
            timestamps.add(record.timestamp());
        }
        assertEquals(List.of(10L, 11L, 12L), offsets);
        assertEquals(List.of(1000L, 1200L, 1500L), timestamps);
        verify(entry).close();
    }

    @Test
    void testCancelledFetchClosesLateEntriesBeforeReusingCursorLease() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(0L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(100L, 1)));

        List<CompletableFuture<List<LogEntry>>> readFutures = new ArrayList<>();
        when(logInstance.openEphemeralCursor(anyString(), anyLong())).thenAnswer(invocation -> {
            LogCursor cursor = mock(LogCursor.class);
            when(cursor.readOffset()).thenReturn(-1L);
            when(cursor.seek(anyLong())).thenReturn(CompletableFuture.completedFuture(null));
            when(cursor.readEntries(anyInt(), anyLong(), isNull(), anyLong())).thenAnswer(readInvocation -> {
                CompletableFuture<List<LogEntry>> readFuture = new CompletableFuture<>();
                readFutures.add(readFuture);
                return readFuture;
            });
            return CompletableFuture.completedFuture(cursor);
        });
        UrsaPartitionLog partitionLog = attachReaderPartitionLog(state, tp, logInstance);

        try {
            List<CompletableFuture<FetchPartitionData>> fetchFutures = new ArrayList<>();
            for (long fetchOffset = 0; fetchOffset < 5; fetchOffset++) {
                fetchFutures.add(partitionLog.fetch(fetchPartitionData(fetchOffset)));
            }

            assertEquals(4, readFutures.size());
            assertTrue(fetchFutures.get(0).cancel(false));

            LogEntry lateEntry = createKafkaRecordsEntry(0L, new long[]{1000L});
            readFutures.get(0).complete(List.of(lateEntry));

            verify(lateEntry, timeout(5_000)).close();
            assertEquals(5, readFutures.size());

            for (int i = 1; i < readFutures.size(); i++) {
                readFutures.get(i).complete(List.of());
            }
            CompletableFuture.allOf(fetchFutures.subList(1, fetchFutures.size())
                    .toArray(new CompletableFuture<?>[0])).get(5, TimeUnit.SECONDS);
            verify(logInstance, times(4)).openEphemeralCursor(anyString(), anyLong());
        } finally {
            partitionLog.close();
        }
    }

    @Test
    void testFetchConversionFailureClosesEntireEntryBatch() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(0L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(1L, 1)));
        when(logInstance.openEphemeralCursor(anyString(), eq(0L)))
                .thenReturn(CompletableFuture.completedFuture(cursor));
        LogEntry failingEntry = mock(LogEntry.class);
        when(failingEntry.payload()).thenThrow(new IllegalStateException("payload unavailable"));
        LogEntry unvisitedEntry = createKafkaRecordsEntry(1L, new long[]{1200L});
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(2L)))
                .thenReturn(CompletableFuture.completedFuture(List.of(failingEntry, unvisitedEntry)));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        FetchPartitionData response = reader.fetch(
                createFetchParams(), Map.of(tp, fetchPartitionData(0L))).get().get(tp);

        assertEquals(Errors.KAFKA_STORAGE_ERROR, response.error);
        verify(failingEntry).close();
        verify(unvisitedEntry).close();
    }

    @Test
    void testFetchInvalidatesPartitionLogWhenLogIsMissing() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(0L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.failedFuture(new NoSuchStreamException("missing")));
        UrsaPartitionLog partitionLog = attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        FetchPartitionData response = reader.fetch(
                createFetchParams(), Map.of(tp, fetchPartitionData(0L))).get().get(tp);

        assertNotNull(response);
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response.error);
        verify(state, atLeastOnce()).removePartitionLog(eq(tp), same(partitionLog));
    }

    @Test
    void testSynchronousCursorReadFailureReleasesFetchPoolSlotForReuse() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        RuntimeException readFailure = new RuntimeException("synchronous read failure");
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(0L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 1)));
        when(logInstance.openEphemeralCursor(anyString(), eq(0L)))
                .thenReturn(CompletableFuture.completedFuture(cursor));
        when(cursor.readOffset()).thenReturn(0L);
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(11L)))
                .thenThrow(readFailure)
                .thenReturn(CompletableFuture.completedFuture(List.of()));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);

        FetchPartitionData failedResponse = reader.fetch(
                createFetchParams(), Map.of(tp, fetchPartitionData(0L))).get().get(tp);
        FetchPartitionData retryResponse = null;
        for (int i = 0; i < 4; i++) {
            retryResponse = reader.fetch(
                    createFetchParams(), Map.of(tp, fetchPartitionData(0L))).get().get(tp);
        }

        assertEquals(Errors.KAFKA_STORAGE_ERROR, failedResponse.error);
        assertNotNull(retryResponse);
        assertEquals(Errors.NONE, retryResponse.error);
        verify(logInstance, times(4)).openEphemeralCursor(anyString(), eq(0L));
        verify(cursor, times(5)).readEntries(anyInt(), anyLong(), isNull(), eq(11L));
    }

    @Test
    void testFetchReusesLogCursorsFromBoundedPoolWithQueueing() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(0L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(100L, 1)));

        AtomicInteger seekCalls = new AtomicInteger();
        AtomicReference<Long> lastSeekOffset = new AtomicReference<>();
        ConcurrentHashMap<String, LogCursor> cursorByName = new ConcurrentHashMap<>();
        HashSet<String> cursorNames = new HashSet<>();
        CountDownLatch startedFourReads = new CountDownLatch(4);
        CountDownLatch startedFiveReads = new CountDownLatch(5);
        ArrayDeque<ReadCompletion> readCompletions = new ArrayDeque<>();

        when(logInstance.openEphemeralCursor(anyString(), anyLong())).thenAnswer(invocation -> {
            String cursorName = invocation.getArgument(0);
            synchronized (cursorNames) {
                cursorNames.add(cursorName);
            }
            return CompletableFuture.completedFuture(cursorByName.computeIfAbsent(cursorName, ignored -> {
                LogCursor cursor = mock(LogCursor.class);
                when(cursor.readOffset()).thenReturn(-1L);
                when(cursor.seek(anyLong())).thenAnswer(seekInvocation -> {
                    long offset = seekInvocation.getArgument(0);
                    lastSeekOffset.set(offset);
                    seekCalls.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                });
                when(cursor.readEntries(anyInt(), anyLong(), isNull(), anyLong())).thenAnswer(readInvocation -> {
                    CompletableFuture<List<LogEntry>> readFuture = new CompletableFuture<>();
                    synchronized (readCompletions) {
                        readCompletions.add(new ReadCompletion(() -> readFuture.complete(List.of())));
                    }
                    startedFourReads.countDown();
                    startedFiveReads.countDown();
                    return readFuture;
                });
                return cursor;
            }));
        });
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        long[] fetchOffsets = new long[]{0L, 1L, 2L, 3L, 7L};
        List<CompletableFuture<Map<TopicIdPartition, FetchPartitionData>>> futures = new ArrayList<>();
        for (long fetchOffset : fetchOffsets) {
            futures.add(reader.fetch(
                    createFetchParams(), Map.of(tp, fetchPartitionData(fetchOffset))));
        }

        assertTrue(startedFourReads.await(5, TimeUnit.SECONDS));
        verify(logInstance, times(4)).openEphemeralCursor(anyString(), anyLong());

        ReadCompletion firstRead;
        synchronized (readCompletions) {
            firstRead = readCompletions.poll();
        }
        assertNotNull(firstRead);
        firstRead.complete.run();
        assertTrue(startedFiveReads.await(5, TimeUnit.SECONDS));

        String expectedPrefix = "kafka-fetch-" + tp.topic()
                + "-partition-" + tp.partition() + "-cursor-";
        synchronized (cursorNames) {
            assertEquals(4, cursorNames.size());
            for (String name : cursorNames) {
                assertTrue(name.startsWith(expectedPrefix));
            }
        }
        assertEquals(1, seekCalls.get());
        assertEquals(7L, lastSeekOffset.get());

        while (true) {
            ReadCompletion completion;
            synchronized (readCompletions) {
                completion = readCompletions.poll();
            }
            if (completion == null) {
                break;
            }
            completion.complete.run();
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).get(5, TimeUnit.SECONDS);
        verify(logInstance, times(4)).openEphemeralCursor(anyString(), anyLong());
        for (LogCursor cursor : cursorByName.values()) {
            verify(cursor, never()).close();
        }
    }

    @Test
    void testListOffsetsTimestampSearchUsesKafkaRecordTimestampAndPreciseOffset() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        LogCursor scanCursor = mock(LogCursor.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 5)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 5)));
        when(logInstance.openEphemeralCursor(anyString(), eq(10L)))
                .thenReturn(CompletableFuture.completedFuture(scanCursor));
        LogEntry entry = createKafkaRecordsEntry(10L, new long[]{1000L, 1200L, 1500L});
        LogEntry trailingEntry = createKafkaRecordsEntry(13L, new long[]{1800L, 2000L});
        when(scanCursor.readEntries(anyInt(), eq(Long.MAX_VALUE), isNull(), eq(15L)))
                .thenReturn(CompletableFuture.completedFuture(List.of(entry, trailingEntry)));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        ListOffsetsPartitionRequest request = new ListOffsetsPartitionRequest(tp, 1500L, Optional.empty());

        ListOffsetsPartitionResponse response = reader.listOffsets(Map.of(tp, request)).get().get(tp);

        assertNotNull(response);
        assertEquals(Errors.NONE, response.error());
        assertEquals(12L, response.offset());
        assertEquals(1500L, response.timestamp());
        verify(entry).close();
        verify(trailingEntry).close();
        verify(scanCursor).close();
    }

    @Test
    void testCancelledTimestampSearchClosesLateEntriesAndCursor() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        LogCursor scanCursor = mock(LogCursor.class);
        CompletableFuture<List<LogEntry>> readFuture = new CompletableFuture<>();
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 1)));
        when(logInstance.openEphemeralCursor(anyString(), eq(10L)))
                .thenReturn(CompletableFuture.completedFuture(scanCursor));
        when(scanCursor.readEntries(anyInt(), eq(Long.MAX_VALUE), isNull(), eq(11L)))
                .thenReturn(readFuture);
        UrsaPartitionLog partitionLog = attachReaderPartitionLog(state, tp, logInstance);

        CompletableFuture<ListOffsetsPartitionResponse> responseFuture = partitionLog.listOffsets(
                new ListOffsetsPartitionRequest(tp, 1000L, Optional.empty()));
        assertTrue(responseFuture.cancel(false));

        LogEntry lateEntry = createKafkaRecordsEntry(10L, new long[]{1000L});
        readFuture.complete(List.of(lateEntry));

        verify(lateEntry, timeout(5_000)).close();
        verify(scanCursor, timeout(5_000)).close();
    }

    @Test
    void testTimestampScanClosesCursorWhenReadThrowsSynchronously() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        LogCursor scanCursor = mock(LogCursor.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 5)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 5)));
        when(logInstance.openEphemeralCursor(anyString(), eq(10L)))
                .thenReturn(CompletableFuture.completedFuture(scanCursor));
        when(scanCursor.readEntries(anyInt(), eq(Long.MAX_VALUE), isNull(), eq(15L)))
                .thenThrow(new RuntimeException("synchronous timestamp read failure"));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        ListOffsetsPartitionRequest request = new ListOffsetsPartitionRequest(tp, 1500L, Optional.empty());

        ListOffsetsPartitionResponse response = reader.listOffsets(Map.of(tp, request)).get().get(tp);

        assertEquals(Errors.KAFKA_STORAGE_ERROR, response.error());
        verify(scanCursor).close();
    }

    @Test
    void testListOffsetsMaxTimestampScansKafkaRecordsAndKeepsEarliestMaximum() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        LogCursor scanCursor = mock(LogCursor.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 2)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(12L, 3)));
        when(logInstance.openEphemeralCursor(anyString(), eq(10L)))
                .thenReturn(CompletableFuture.completedFuture(scanCursor));
        LogEntry firstEntry = createKafkaRecordsEntry(10L, new long[]{1000L, 3000L});
        LogEntry secondEntry = createKafkaRecordsEntry(12L, new long[]{3500L, 3500L, 2000L});
        CompletableFuture<List<LogEntry>> entriesFuture =
                CompletableFuture.completedFuture(List.of(firstEntry, secondEntry));
        CompletableFuture<List<LogEntry>> endOfLogFuture =
                CompletableFuture.completedFuture(List.of());
        when(scanCursor.readEntries(anyInt(), eq(Long.MAX_VALUE), isNull(), eq(15L)))
                .thenReturn(entriesFuture)
                .thenReturn(endOfLogFuture);
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        ListOffsetsPartitionRequest request = new ListOffsetsPartitionRequest(
                tp, ListOffsetsPartitionRequest.MAX_TIMESTAMP, Optional.empty());

        ListOffsetsPartitionResponse response = reader.listOffsets(Map.of(tp, request)).get().get(tp);

        assertNotNull(response);
        assertEquals(Errors.NONE, response.error());
        assertEquals(12L, response.offset());
        assertEquals(3500L, response.timestamp());
        verify(firstEntry).close();
        verify(secondEntry).close();
        verify(scanCursor).close();
    }

    @Test
    void testMaxTimestampScanClosesCursorWhenReadThrowsSynchronously() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        LogCursor scanCursor = mock(LogCursor.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 5)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 5)));
        when(logInstance.openEphemeralCursor(anyString(), eq(10L)))
                .thenReturn(CompletableFuture.completedFuture(scanCursor));
        when(scanCursor.readEntries(anyInt(), eq(Long.MAX_VALUE), isNull(), eq(15L)))
                .thenThrow(new RuntimeException("synchronous max-timestamp read failure"));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        ListOffsetsPartitionRequest request = new ListOffsetsPartitionRequest(
                tp, ListOffsetsPartitionRequest.MAX_TIMESTAMP, Optional.empty());

        ListOffsetsPartitionResponse response = reader.listOffsets(Map.of(tp, request)).get().get(tp);

        assertEquals(Errors.KAFKA_STORAGE_ERROR, response.error());
        verify(scanCursor).close();
    }

    private static FetchParams createFetchParams() {
        return new FetchParams(
                -1,
                -1,
                500,
                1,
                1024 * 1024,
                FetchIsolation.HIGH_WATERMARK,
                Optional.empty()
        );
    }

    private static FetchRequest.PartitionData fetchPartitionData(long fetchOffset) {
        return new FetchRequest.PartitionData(
                Uuid.ZERO_UUID, fetchOffset, 0, 1024 * 1024, Optional.empty());
    }

    private static TopicIdPartition createTestPartition() {
        return new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 0));
    }

    private static LogOffset logOffset(long offset, int numberOfRecords) {
        return new LogOffset(offset, numberOfRecords, -1L);
    }

    private static LogEntry createKafkaRecordsEntry(long baseOffset, long[] timestamps) {
        SimpleRecord[] records = new SimpleRecord[timestamps.length];
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < timestamps.length; i++) {
            records[i] = new SimpleRecord(timestamps[i], key, value);
        }

        MemoryRecords memoryRecords = MemoryRecords.withRecords(Compression.NONE, records);
        RecordAnalyzer.RecordAnalysisResult analysisResult = RecordAnalyzer.analyzeAndValidateRecords(
                memoryRecords,
                new TopicPartition("test-topic", 0),
                0
        );
        ByteBuf encoded = KafkaEntryFormatter.encode(memoryRecords, analysisResult);

        LogEntry entry = mock(LogEntry.class);
        AtomicBoolean closed = new AtomicBoolean();
        when(entry.offset()).thenReturn(baseOffset);
        when(entry.numberOfRecords()).thenReturn(timestamps.length);
        when(entry.size()).thenReturn(encoded.readableBytes());
        when(entry.payload()).thenReturn(encoded.asReadOnly());
        doAnswer(invocation -> {
            if (closed.compareAndSet(false, true)) {
                encoded.release();
            }
            return null;
        }).when(entry).close();
        return entry;
    }

    private static UrsaPartitionLog attachReaderPartitionLog(
            UrsaStorageState state,
            TopicIdPartition tp,
            Log logInstance) {
        UrsaPartitionLog partitionLog = new UrsaPartitionLog(
                tp,
                state,
                mock(DisklessLogMetrics.class),
                CompletableFuture.completedFuture(logInstance),
                null,
                0L,
                0,
                null);
        when(state.getOrCreatePartitionLog(tp)).thenReturn(partitionLog);
        return partitionLog;
    }

    private static final class ReadCompletion {
        private final Runnable complete;

        private ReadCompletion(Runnable complete) {
            this.complete = complete;
        }
    }

    private static final class NoSuchStreamException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private NoSuchStreamException(String message) {
            super(message);
        }
    }
}
