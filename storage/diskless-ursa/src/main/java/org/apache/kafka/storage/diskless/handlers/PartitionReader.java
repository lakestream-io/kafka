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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.DisklessFutures;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionRequest;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionResponse;
import org.apache.kafka.storage.diskless.LogEntryUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.lakestream.api.Log;
import io.lakestream.api.LogCursor;
import io.lakestream.api.LogEntry;
import io.lakestream.api.LogOffset;

/**
 * The read side of one diskless partition: fetch, ListOffsets, and the high watermark.
 *
 * <p>Fetches share a single cached {@link LogCursor}. A sequential consumer keeps finding that cursor
 * exactly where its previous fetch left it, so the common case costs neither a cursor open nor a
 * seek. A concurrent second fetch never queues behind the first: it opens a throw-away cursor, and
 * whichever read releases first back into the empty cache keeps its cursor while the other is closed.
 *
 * <p>ListOffsets never opens a cursor and decodes at most two entries. A timestamp lookup binary
 * searches entry headers and then reads the one entry that can hold the answer; MAX_TIMESTAMP is
 * answered from the last entry's header alone.
 */
final class PartitionReader implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PartitionReader.class);

    private static final long UNKNOWN_TIMESTAMP = -1L;
    private static final long NOT_FOUND_OFFSET = -1L;

    private final TopicIdPartition topicIdPartition;
    private final Log logInstance;
    private final int maxEntriesPerFetch;
    private final String cursorName;
    private final AtomicReference<LogCursor> cachedCursor = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    PartitionReader(TopicIdPartition topicIdPartition, Log logInstance, int maxEntriesPerFetch) {
        this.topicIdPartition = topicIdPartition;
        this.logInstance = logInstance;
        this.maxEntriesPerFetch = maxEntriesPerFetch;
        this.cursorName = "kafka-fetch-" + topicIdPartition.topic() + "-" + topicIdPartition.partition();
    }

    /** The offset just past the last durable record, or 0 for an empty log. */
    CompletableFuture<Long> highWatermark() {
        return call(logInstance::getLastOffset, "Log.getLastOffset").thenApply(PartitionReader::highWatermarkOf);
    }

    CompletableFuture<FetchPartitionData> fetch(FetchRequest.PartitionData request) {
        long fetchOffset = request.fetchOffset;
        int maxBytes = request.maxBytes;
        log.debug("Fetching from partition {} at offset {} with maxBytes {}",
                topicIdPartition, fetchOffset, maxBytes);

        return range().thenCompose(range -> {
            if (fetchOffset < range.start() || fetchOffset > range.highWatermark()) {
                log.debug("fetchOffset {} outside range [{}, {}] for partition {}",
                        fetchOffset, range.start(), range.highWatermark(), topicIdPartition);
                return CompletableFuture.completedFuture(
                        fetchData(Errors.OFFSET_OUT_OF_RANGE, range, MemoryRecords.EMPTY));
            }
            if (fetchOffset == range.highWatermark()) {
                return CompletableFuture.completedFuture(fetchData(Errors.NONE, range, MemoryRecords.EMPTY));
            }
            return readEntries(fetchOffset, range.highWatermark(), maxBytes)
                    .thenApply(records -> fetchData(Errors.NONE, range, records));
        });
    }

    CompletableFuture<ListOffsetsPartitionResponse> listOffsets(ListOffsetsPartitionRequest request) {
        long timestamp = request.timestamp();
        log.debug("ListOffsets for partition {} with timestamp {}", topicIdPartition, timestamp);

        if (timestamp == ListOffsetsPartitionRequest.LATEST_TIERED_TIMESTAMP) {
            return CompletableFuture.completedFuture(notFound());
        }
        return range().thenCompose(range -> {
            if (timestamp == ListOffsetsPartitionRequest.EARLIEST_TIMESTAMP
                    || timestamp == ListOffsetsPartitionRequest.EARLIEST_LOCAL_TIMESTAMP) {
                return CompletableFuture.completedFuture(success(range.start(), UNKNOWN_TIMESTAMP));
            }
            if (timestamp == ListOffsetsPartitionRequest.LATEST_TIMESTAMP) {
                return CompletableFuture.completedFuture(success(range.highWatermark(), UNKNOWN_TIMESTAMP));
            }
            if (range.isEmpty()) {
                return CompletableFuture.completedFuture(notFound());
            }
            if (timestamp == ListOffsetsPartitionRequest.MAX_TIMESTAMP) {
                // Entries are appended in write-timestamp order, so the last one carries the maximum.
                return CompletableFuture.completedFuture(
                        success(range.lastEntryOffset(), range.lastEntryTimestamp()));
            }
            return firstAtOrAfter(range, timestamp);
        });
    }

    /** Closes the cached cursor. In-flight reads close their own cursor when they release it. */
    @Override
    public void close() {
        closed.set(true);
        closeQuietly(cachedCursor.getAndSet(null));
    }

    private CompletableFuture<MemoryRecords> readEntries(long fetchOffset, long maxOffsetExclusive, int maxBytes) {
        return acquireCursor(fetchOffset).thenCompose(lease -> {
            CompletableFuture<List<LogEntry>> read;
            try {
                read = lease.cursor().readEntries(maxEntriesPerFetch, maxBytes, null, maxOffsetExclusive);
                if (read == null) {
                    throw new IllegalStateException(
                            "LogCursor.readEntries returned a null future for " + topicIdPartition);
                }
            } catch (Throwable readError) {
                lease.release();
                return CompletableFuture.<MemoryRecords>failedFuture(readError);
            }
            return read.handle((entries, error) -> {
                MemoryRecords records = null;
                Throwable failure = error == null ? null : DisklessFutures.unwrap(error);
                try {
                    if (failure == null) {
                        records = KafkaRecordsPayload.assemble(entries);
                    }
                } catch (Throwable assembleError) {
                    failure = assembleError;
                } finally {
                    failure = LogEntryUtils.closeAll(entries, failure);
                    lease.release();
                }
                if (failure != null) {
                    throw new CompletionException(failure);
                }
                return records;
            });
        });
    }

    /**
     * Leases the cached cursor, seeking it only when it is not already at {@code startOffset}. When the
     * cache is empty — the reader has never read, or another read holds the cursor — a throw-away cursor
     * is opened instead so that reads never queue behind each other.
     */
    private CompletableFuture<CursorLease> acquireCursor(long startOffset) {
        LogCursor cached = cachedCursor.getAndSet(null);
        if (cached == null) {
            return call(() -> logInstance.openEphemeralCursor(cursorName, startOffset), "Log.openEphemeralCursor")
                    .thenApply(cursor -> {
                        if (cursor == null) {
                            throw new IllegalStateException(
                                    "Log.openEphemeralCursor returned a null cursor for " + topicIdPartition);
                        }
                        return new CursorLease(cursor);
                    });
        }
        try {
            if (cached.readOffset() == startOffset) {
                return CompletableFuture.completedFuture(new CursorLease(cached));
            }
            CompletableFuture<Void> positioned = cached.seek(startOffset);
            if (positioned == null) {
                throw new IllegalStateException("LogCursor.seek returned a null future for " + topicIdPartition);
            }
            return positioned.handle((ignored, seekError) -> {
                if (seekError != null) {
                    closeQuietly(cached);
                    throw new CompletionException(DisklessFutures.unwrap(seekError));
                }
                return new CursorLease(cached);
            });
        } catch (Throwable seekError) {
            closeQuietly(cached);
            return CompletableFuture.failedFuture(seekError);
        }
    }

    /**
     * Binary searches entry headers for the last entry written before {@code target}, then decodes that
     * single entry. A header timestamp is the entry's write time while a record timestamp is its create
     * time, so the first matching record can sit in the entry that follows; that one entry is read as a
     * fallback before reporting no match.
     */
    private CompletableFuture<ListOffsetsPartitionResponse> firstAtOrAfter(Range range, long target) {
        return call(() -> logInstance.binarySearchOffset(
                        range.start(), range.lastEntryOffset(), header -> header.timestamp() < target),
                "Log.binarySearchOffset")
                .thenCompose(found -> searchEntryAt(
                        found == null || found < range.start() ? range.start() : found,
                        target,
                        range.highWatermark(),
                        true));
    }

    private CompletableFuture<ListOffsetsPartitionResponse> searchEntryAt(
            long entryOffset,
            long target,
            long endOffsetExclusive,
            boolean readFollowingEntry) {
        if (entryOffset < 0 || entryOffset >= endOffsetExclusive) {
            return CompletableFuture.completedFuture(notFound());
        }
        return call(() -> logInstance.readEntry(entryOffset), "Log.readEntry")
                .thenApply(entry -> probe(entry, target))
                .thenCompose(probe -> {
                    if (probe.match() != null) {
                        return CompletableFuture.completedFuture(probe.match());
                    }
                    if (!readFollowingEntry) {
                        return CompletableFuture.completedFuture(notFound());
                    }
                    return searchEntryAt(probe.nextOffset(), target, endOffsetExclusive, false);
                });
    }

    /** Decodes one entry, always closing it, and reports the first record at or after {@code target}. */
    private EntryProbe probe(LogEntry entry, long target) {
        if (entry == null) {
            throw new CompletionException(
                    new IllegalStateException("Log.readEntry returned a null entry for " + topicIdPartition));
        }
        ListOffsetsPartitionResponse match = null;
        long nextOffset = NOT_FOUND_OFFSET;
        Throwable failure = null;
        try {
            MemoryRecords records = KafkaRecordsPayload.readableRecords(
                    entry.payload(), entry.offset(), entry.numberOfRecords());
            nextOffset = entry.offset() + entry.numberOfRecords();
            for (Record record : records.records()) {
                if (record.timestamp() >= target) {
                    match = success(record.offset(), record.timestamp());
                    break;
                }
            }
        } catch (Throwable decodeError) {
            failure = decodeError;
        } finally {
            failure = LogEntryUtils.closeAll(List.of(entry), failure);
        }
        if (failure != null) {
            throw new CompletionException(failure);
        }
        return new EntryProbe(match, nextOffset);
    }

    private CompletableFuture<Range> range() {
        return call(logInstance::getFirstOffset, "Log.getFirstOffset")
                .thenCombine(call(logInstance::getLastOffset, "Log.getLastOffset"), Range::of);
    }

    private FetchPartitionData fetchData(Errors error, Range range, MemoryRecords records) {
        return new FetchPartitionData(
                error,
                range.highWatermark(),
                range.start(),
                records,
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                false);
    }

    private ListOffsetsPartitionResponse success(long offset, long timestamp) {
        return ListOffsetsPartitionResponse.success(topicIdPartition, offset, timestamp);
    }

    private ListOffsetsPartitionResponse notFound() {
        return ListOffsetsPartitionResponse.success(topicIdPartition, NOT_FOUND_OFFSET, UNKNOWN_TIMESTAMP);
    }

    private void closeQuietly(LogCursor cursor) {
        if (cursor == null) {
            return;
        }
        try {
            cursor.close();
        } catch (Throwable closeError) {
            log.warn("Failed to close the fetch cursor for partition {}", topicIdPartition, closeError);
        }
    }

    private static long highWatermarkOf(LogOffset lastOffset) {
        return lastOffset == null || lastOffset.offset() < 0
                ? 0L
                : lastOffset.offset() + lastOffset.numberOfRecords();
    }

    /** Runs a storage call so that a synchronous throw or a null future becomes a failed future. */
    private static <T> CompletableFuture<T> call(Supplier<CompletableFuture<T>> operation, String description) {
        try {
            CompletableFuture<T> result = operation.get();
            if (result == null) {
                throw new IllegalStateException(description + " returned a null future");
            }
            return result;
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    /** Exclusive use of one cursor until {@link #release()} caches it again or closes it. */
    private final class CursorLease {

        private final LogCursor cursor;
        private final AtomicBoolean released = new AtomicBoolean();

        private CursorLease(LogCursor cursor) {
            this.cursor = cursor;
        }

        private LogCursor cursor() {
            return cursor;
        }

        private void release() {
            if (!released.compareAndSet(false, true)) {
                return;
            }
            if (!closed.get() && cachedCursor.compareAndSet(null, cursor)) {
                // close() may have emptied the cache between the check and the swap above.
                if (closed.get() && cachedCursor.compareAndSet(cursor, null)) {
                    closeQuietly(cursor);
                }
                return;
            }
            closeQuietly(cursor);
        }
    }

    /** The readable offset window of the log: {@code [start, highWatermark)}, plus the last entry's header. */
    private record Range(long start, long highWatermark, long lastEntryOffset, long lastEntryTimestamp) {

        static Range of(LogOffset first, LogOffset last) {
            long highWatermark = highWatermarkOf(last);
            long start = first == null || first.offset() < 0 ? highWatermark : first.offset();
            return new Range(
                    start,
                    highWatermark,
                    last == null ? NOT_FOUND_OFFSET : last.offset(),
                    last == null ? UNKNOWN_TIMESTAMP : last.timestamp());
        }

        boolean isEmpty() {
            return lastEntryOffset < 0 || start >= highWatermark;
        }
    }

    /** The outcome of decoding one entry: a matching record, or the offset of the next entry. */
    private record EntryProbe(ListOffsetsPartitionResponse match, long nextOffset) {
    }
}
