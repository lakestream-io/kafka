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
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionRequest;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionResponse;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.lakestream.api.Log;
import io.lakestream.api.LogCursor;
import io.lakestream.api.LogEntry;
import io.lakestream.api.LogOffset;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PartitionReaderTest {

    private static final TopicIdPartition TP =
            new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 0));

    @Test
    void fetchAssemblesEntriesAndReusesCursor() throws Exception {
        Log log = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(2L, 1, 1L)));   // hwm = 3
        when(log.openEphemeralCursor(anyString(), eq(0L))).thenReturn(completedFuture(cursor));
        // The cursor has consumed offsets 0 and 1 by the time the second fetch acquires it.
        when(cursor.readOffset()).thenReturn(2L);
        List<LogEntry> firstBatch = List.of(entry(0L, records("a")), entry(1L, records("b")));
        List<LogEntry> secondBatch = List.of(entry(2L, records("c")));
        when(cursor.readEntries(eq(10), eq(1024L), isNull(), eq(3L)))
                .thenReturn(completedFuture(firstBatch))
                .thenReturn(completedFuture(secondBatch));

        try (PartitionReader reader = new PartitionReader(TP, log, 10)) {
            FetchPartitionData first = reader.fetch(fetchRequest(0L)).get();
            assertEquals(Errors.NONE, first.error);
            assertEquals(3L, first.highWatermark);
            assertEquals(0L, first.logStartOffset);
            assertEquals(2, count(first.records));

            FetchPartitionData second = reader.fetch(fetchRequest(2L)).get();
            assertEquals(Errors.NONE, second.error);
            assertEquals(1, count(second.records));
            verify(log, times(1)).openEphemeralCursor(anyString(), anyLong());   // cursor reused
            verify(cursor, never()).seek(anyLong());                             // position matched
        }
    }

    @Test
    void fetchOutOfRangeReturnsOffsetOutOfRange() throws Exception {
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(5L, 1, 1L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(9L, 1, 1L)));

        try (PartitionReader reader = new PartitionReader(TP, log, 10)) {
            FetchPartitionData data = reader.fetch(fetchRequest(2L)).get();
            assertEquals(Errors.OFFSET_OUT_OF_RANGE, data.error);
            assertEquals(5L, data.logStartOffset);
            assertEquals(10L, data.highWatermark);
            verify(log, never()).openEphemeralCursor(anyString(), anyLong());
        }
    }

    @Test
    void listOffsetsByTimestampUsesBinarySearchThenDecodesOneEntry() throws Exception {
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 2, 100L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(4L, 2, 300L)));
        when(log.binarySearchOffset(eq(0L), eq(4L), any())).thenReturn(completedFuture(2L));
        LogEntry found = entry(2L, MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(150L, "a".getBytes(StandardCharsets.UTF_8)),
                new SimpleRecord(210L, "b".getBytes(StandardCharsets.UTF_8))));
        when(log.readEntry(2L)).thenReturn(completedFuture(found));

        try (PartitionReader reader = new PartitionReader(TP, log, 10)) {
            ListOffsetsPartitionResponse response = reader.listOffsets(listOffsetsRequest(200L)).get();
            assertEquals(Errors.NONE, response.error());
            assertEquals(3L, response.offset());
            assertEquals(210L, response.timestamp());
            verify(log, never()).openEphemeralCursor(anyString(), anyLong());
            verify(found).close();
        }
    }

    @Test
    void maxTimestampAnswersFromLastEntryHeader() throws Exception {
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 100L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(2L, 1, 300L)));

        try (PartitionReader reader = new PartitionReader(TP, log, 10)) {
            ListOffsetsPartitionResponse response =
                    reader.listOffsets(listOffsetsRequest(ListOffsetsPartitionRequest.MAX_TIMESTAMP)).get();
            assertEquals(Errors.NONE, response.error());
            assertEquals(2L, response.offset());
            assertEquals(300L, response.timestamp());
            // Entries are appended in write-timestamp order, so no index walk is needed.
            verify(log, never()).getEntryMetadataRange(anyLong(), anyLong());
            verify(log, never()).readEntry(anyLong());
            verify(log, never()).openEphemeralCursor(anyString(), anyLong());
        }
    }

    @Test
    void listOffsetsOnAnEmptyLogReportsNoOffsets() throws Exception {
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(LogOffset.NOT_FOUND));
        when(log.getLastOffset()).thenReturn(completedFuture(LogOffset.NOT_FOUND));

        try (PartitionReader reader = new PartitionReader(TP, log, 10)) {
            assertEquals(0L, reader.highWatermark().get());
            assertEquals(0L, reader.listOffsets(
                    listOffsetsRequest(ListOffsetsPartitionRequest.EARLIEST_TIMESTAMP)).get().offset());
            assertEquals(0L, reader.listOffsets(
                    listOffsetsRequest(ListOffsetsPartitionRequest.LATEST_TIMESTAMP)).get().offset());
            assertEquals(-1L, reader.listOffsets(listOffsetsRequest(1234L)).get().offset());
            assertEquals(-1L, reader.listOffsets(
                    listOffsetsRequest(ListOffsetsPartitionRequest.MAX_TIMESTAMP)).get().offset());
            assertEquals(-1L, reader.listOffsets(
                    listOffsetsRequest(ListOffsetsPartitionRequest.LATEST_TIERED_TIMESTAMP)).get().offset());
            verify(log, never()).binarySearchOffset(anyLong(), anyLong(), any());
            verify(log, never()).getEntryMetadataRange(anyLong(), anyLong());
        }
    }

    @Test
    void concurrentFetchUsesAThrowAwayCursorAndClosesIt() throws Exception {
        Log log = mock(Log.class);
        LogCursor firstCursor = mock(LogCursor.class);
        LogCursor secondCursor = mock(LogCursor.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(9L, 1, 1L)));
        when(log.openEphemeralCursor(anyString(), anyLong()))
                .thenReturn(completedFuture(firstCursor))
                .thenReturn(completedFuture(secondCursor));
        CompletableFuture<List<LogEntry>> firstRead = new CompletableFuture<>();
        CompletableFuture<List<LogEntry>> secondRead = new CompletableFuture<>();
        when(firstCursor.readEntries(anyInt(), anyLong(), isNull(), anyLong())).thenReturn(firstRead);
        when(secondCursor.readEntries(anyInt(), anyLong(), isNull(), anyLong())).thenReturn(secondRead);

        try (PartitionReader reader = new PartitionReader(TP, log, 10)) {
            CompletableFuture<FetchPartitionData> firstFetch = reader.fetch(fetchRequest(0L));
            // The second fetch must not queue behind the first: it opens its own throw-away cursor.
            CompletableFuture<FetchPartitionData> secondFetch = reader.fetch(fetchRequest(4L));
            verify(log, times(2)).openEphemeralCursor(anyString(), anyLong());

            secondRead.complete(List.of());
            assertEquals(Errors.NONE, secondFetch.get(5, TimeUnit.SECONDS).error);
            firstRead.complete(List.of());
            assertEquals(Errors.NONE, firstFetch.get(5, TimeUnit.SECONDS).error);

            // Only one cursor is cached; the loser of the race is closed right away.
            verify(secondCursor, never()).close();
            verify(firstCursor).close();
        }
        verify(secondCursor).close();
    }

    @Test
    void closeClosesTheCachedCursor() throws Exception {
        Log log = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.openEphemeralCursor(anyString(), eq(0L))).thenReturn(completedFuture(cursor));
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(1L)))
                .thenReturn(completedFuture(List.of()));

        PartitionReader reader = new PartitionReader(TP, log, 10);
        reader.fetch(fetchRequest(0L)).get();
        verify(cursor, never()).close();

        reader.close();
        verify(cursor).close();

        reader.close();
        verify(cursor, times(1)).close();
    }

    @Test
    void fetchClosesEveryEntryWhenAssemblyFails() {
        Log log = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(1L, 1, 1L)));
        when(log.openEphemeralCursor(anyString(), eq(0L))).thenReturn(completedFuture(cursor));
        LogEntry failing = mock(LogEntry.class);
        when(failing.payload()).thenThrow(new IllegalStateException("payload unavailable"));
        LogEntry unvisited = entry(1L, records("b"));
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(2L)))
                .thenReturn(completedFuture(List.of(failing, unvisited)));

        try (PartitionReader reader = new PartitionReader(TP, log, 10)) {
            ExecutionException error = assertThrows(
                    ExecutionException.class, () -> reader.fetch(fetchRequest(0L)).get());
            assertInstanceOf(IllegalStateException.class, error.getCause());
            verify(failing).close();
            verify(unvisited).close();
        }
    }

    @Test
    void fetchReleasesTheCursorWhenTheReadThrowsSynchronously() throws Exception {
        Log log = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(9L, 1, 1L)));
        when(log.openEphemeralCursor(anyString(), eq(0L))).thenReturn(completedFuture(cursor));
        when(cursor.readOffset()).thenReturn(0L);
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(10L)))
                .thenThrow(new RuntimeException("synchronous read failure"))
                .thenReturn(completedFuture(List.of()));

        try (PartitionReader reader = new PartitionReader(TP, log, 10)) {
            assertThrows(ExecutionException.class, () -> reader.fetch(fetchRequest(0L)).get());
            // The failed read must not leak the cursor: the retry reuses it instead of opening another.
            assertEquals(Errors.NONE, reader.fetch(fetchRequest(0L)).get().error);
            verify(log, times(1)).openEphemeralCursor(anyString(), anyLong());
        }
    }

    @Test
    void timestampSearchFallsBackToTheFollowingEntryOnce() throws Exception {
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 2, 100L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(2L, 2, 300L)));
        when(log.binarySearchOffset(eq(0L), eq(2L), any())).thenReturn(completedFuture(0L));
        LogEntry stale = entry(0L, MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(100L, "a".getBytes(StandardCharsets.UTF_8)),
                new SimpleRecord(110L, "b".getBytes(StandardCharsets.UTF_8))));
        LogEntry match = entry(2L, MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(150L, "c".getBytes(StandardCharsets.UTF_8)),
                new SimpleRecord(260L, "d".getBytes(StandardCharsets.UTF_8))));
        when(log.readEntry(0L)).thenReturn(completedFuture(stale));
        when(log.readEntry(2L)).thenReturn(completedFuture(match));

        try (PartitionReader reader = new PartitionReader(TP, log, 10)) {
            ListOffsetsPartitionResponse response = reader.listOffsets(listOffsetsRequest(120L)).get();
            assertEquals(2L, response.offset());
            assertEquals(150L, response.timestamp());
            verify(stale).close();
            verify(match).close();
            verify(log, times(2)).readEntry(anyLong());
        }
    }

    @Test
    void timestampSearchStopsAfterOneFallbackEntry() throws Exception {
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 100L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(2L, 1, 300L)));
        when(log.binarySearchOffset(eq(0L), eq(2L), any())).thenReturn(completedFuture(0L));
        LogEntry first = entry(0L, records("a"));
        LogEntry second = entry(1L, records("b"));
        when(log.readEntry(0L)).thenReturn(completedFuture(first));
        when(log.readEntry(1L)).thenReturn(completedFuture(second));

        try (PartitionReader reader = new PartitionReader(TP, log, 10)) {
            ListOffsetsPartitionResponse response = reader.listOffsets(listOffsetsRequest(Long.MAX_VALUE)).get();
            assertEquals(-1L, response.offset());
            assertEquals(-1L, response.timestamp());
            verify(first).close();
            verify(second).close();
            verify(log, times(2)).readEntry(anyLong());
        }
    }

    @Test
    void highWatermarkIsTheOffsetPastTheLastRecord() throws Exception {
        Log log = mock(Log.class);
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(10L, 5, 1L)));

        try (PartitionReader reader = new PartitionReader(TP, log, 10)) {
            assertEquals(15L, reader.highWatermark().get());
        }
    }

    private static FetchRequest.PartitionData fetchRequest(long fetchOffset) {
        return new FetchRequest.PartitionData(TP.topicId(), fetchOffset, -1L, 1024, Optional.empty());
    }

    private static ListOffsetsPartitionRequest listOffsetsRequest(long timestamp) {
        return new ListOffsetsPartitionRequest(TP, timestamp, Optional.empty());
    }

    private static MemoryRecords records(String value) {
        return MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(1L, value.getBytes(StandardCharsets.UTF_8)));
    }

    private static int count(Records records) {
        int count = 0;
        for (Iterator<Record> it = records.records().iterator(); it.hasNext(); it.next()) {
            count++;
        }
        return count;
    }

    /** Mocks a storage entry whose payload is read-only, exactly as the Lakestream implementation returns it. */
    private static LogEntry entry(long offset, MemoryRecords records) {
        int numberOfRecords = count(records);
        ByteBuf payload = Unpooled.buffer(records.buffer().remaining());
        payload.writeBytes(records.buffer().duplicate());

        LogEntry entry = mock(LogEntry.class);
        AtomicBoolean closed = new AtomicBoolean();
        when(entry.offset()).thenReturn(offset);
        when(entry.numberOfRecords()).thenReturn(numberOfRecords);
        when(entry.size()).thenReturn(payload.readableBytes());
        when(entry.payload()).thenReturn(payload.asReadOnly());
        doAnswer(invocation -> {
            if (closed.compareAndSet(false, true)) {
                payload.release();
            }
            return null;
        }).when(entry).close();
        return entry;
    }
}
