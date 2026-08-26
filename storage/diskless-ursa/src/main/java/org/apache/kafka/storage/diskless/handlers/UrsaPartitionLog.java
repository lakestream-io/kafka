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
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.DisklessLogMetadata;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionRequest;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionResponse;
import org.apache.kafka.storage.diskless.handlers.RecordAnalyzer.RecordAnalysisResult;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import io.netty.buffer.ByteBuf;
import io.oxia.client.api.AsyncOxiaClient;
import io.streamnative.lakestream.api.Log;
import io.streamnative.lakestream.api.LogCursor;
import io.streamnative.lakestream.api.LogEntry;
import io.streamnative.lakestream.api.LogEntryHeader;
import io.streamnative.lakestream.api.LogOffset;
import io.streamnative.ursa.storage.OwnedResultFutures;

final class UrsaPartitionLog {

    private static final Logger log = LoggerFactory.getLogger(UrsaPartitionLog.class);
    private static final long UNKNOWN_TIMESTAMP = -1L;
    private static final int MAX_ENTRIES_PER_FETCH = 10;
    private static final int FETCH_CURSOR_POOL_SIZE = 4;

    private final TopicIdPartition topicIdPartition;
    private final UrsaStorageState state;
    private final Supplier<AsyncOxiaClient> oxiaClientSupplier;
    private final long producerStateSnapshotIntervalMs;
    private final int producerStateSnapshotRecordThreshold;
    private final ScheduledExecutorService producerStateScheduler;
    private final DisklessLogMetrics logMetrics;
    private final CompletableFuture<Log> initFuture;
    private final PartitionWriteSequencer writeSequencer;
    private final AtomicBoolean initialRetentionTriggered = new AtomicBoolean();
    private final AtomicBoolean retentionWorkerRunning = new AtomicBoolean();
    private final AtomicReference<RetentionRequest> pendingRetention = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<Log>> inFlightRetention = new AtomicReference<>();
    private CompletableFuture<Long> activeTrimFuture;
    private volatile LogCursorPool fetchCursorPool;
    private volatile boolean closed;
    private final ConcurrentHashMap<String, ProducerStateManager> producerStateManagers = new ConcurrentHashMap<>();

    UrsaPartitionLog(TopicIdPartition topicIdPartition,
                     UrsaStorageState state,
                     DisklessLogMetrics logMetrics,
                     CompletableFuture<Log> logFuture,
                     Supplier<AsyncOxiaClient> oxiaClientSupplier,
                     long producerStateSnapshotIntervalMs,
                     int producerStateSnapshotRecordThreshold,
                     ScheduledExecutorService producerStateScheduler) {
        this.topicIdPartition = topicIdPartition;
        this.state = state;
        this.logMetrics = logMetrics;
        this.oxiaClientSupplier = oxiaClientSupplier;
        this.producerStateSnapshotIntervalMs = producerStateSnapshotIntervalMs;
        this.producerStateSnapshotRecordThreshold = producerStateSnapshotRecordThreshold;
        this.producerStateScheduler = producerStateScheduler;
        this.writeSequencer = new PartitionWriteSequencer(topicIdPartition.toString());
        this.closed = false;
        this.initFuture = createInitFuture(logFuture);
    }

    ProducerStateManager getOrCreateProducerStateManager(String zone) {
        return producerStateManagers.computeIfAbsent(zone, zoneId -> new ProducerStateManager(
                topicIdPartition,
                oxiaClientSupplier,
                () -> initFuture,
                zoneId,
                producerStateSnapshotIntervalMs,
                producerStateSnapshotRecordThreshold,
                producerStateScheduler));
    }

    void installProducerStateManager(String zone, ProducerStateManager producerStateManager) {
        producerStateManagers.put(zone, producerStateManager);
    }

    CompletableFuture<PartitionResponse> write(MemoryRecords records, String zone, String writerName) {
        log.debug("Writing {} bytes to partition {} via {}", records.sizeInBytes(), topicIdPartition, writerName);

        if (closed) {
            return CompletableFuture.completedFuture(notLeaderResponse());
        }

        if (records.sizeInBytes() == 0) {
            return CompletableFuture.completedFuture(new PartitionResponse(Errors.NONE));
        }

        boolean hasProducerId = false;
        for (RecordBatch batch : records.batches()) {
            if (batch.isTransactional()) {
                log.warn("Transactional produce rejected for partition {}", topicIdPartition);
                return CompletableFuture.completedFuture(new PartitionResponse(Errors.INVALID_REQUEST));
            }
            if (batch.hasProducerId()) {
                hasProducerId = true;
            }
        }

        RecordAnalysisResult analysisResult;
        try {
            analysisResult = RecordAnalyzer.analyzeAndValidateRecords(
                    records,
                    new TopicPartition(topicIdPartition.topic(), topicIdPartition.partition()),
                    0
            );
        } catch (Exception e) {
            log.warn("Record validation failed for partition {}: {}", topicIdPartition, e.getMessage());
            return CompletableFuture.completedFuture(new PartitionResponse(Errors.INVALID_RECORD));
        }

        if (analysisResult.validBytes() <= 0) {
            return CompletableFuture.completedFuture(new PartitionResponse(Errors.NONE));
        }

        final boolean idempotent = hasProducerId;
        return writeSequencer.submit(() -> writeValidated(records, analysisResult, idempotent, zone));
    }

    private PartitionWriteSequencer.WriteTask<PartitionResponse> writeValidated(
            MemoryRecords records,
            RecordAnalysisResult analysisResult,
            boolean idempotent,
            String zone) {
        if (closed) {
            return closedWriteTask();
        }

        CompletableFuture<Void> submissionFuture = new CompletableFuture<>();
        CompletableFuture<PartitionResponse> result = idempotent
                ? appendIdempotentRecords(records, analysisResult, zone, submissionFuture)
                : appendNonIdempotentRecords(records, analysisResult, submissionFuture);
        CompletableFuture<PartitionResponse> mappedResult = result.exceptionally(this::writeErrorResponse);
        return new PartitionWriteSequencer.WriteTask<>(submissionFuture, mappedResult);
    }

    private PartitionWriteSequencer.WriteTask<PartitionResponse> closedWriteTask() {
        return new PartitionWriteSequencer.WriteTask<>(
                CompletableFuture.completedFuture(null),
                CompletableFuture.completedFuture(notLeaderResponse()));
    }

    private PartitionResponse notLeaderResponse() {
        return new PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER);
    }

    private NotLeaderOrFollowerException ownershipLostException() {
        return new NotLeaderOrFollowerException("Partition log is closed for " + topicIdPartition);
    }

    void invalidate() {
        close(false);
        state.removePartitionLog(topicIdPartition, this);
    }

    CompletableFuture<FetchPartitionData> fetch(FetchRequest.PartitionData partitionData) {
        long fetchOffset = partitionData.fetchOffset;
        int maxBytes = partitionData.maxBytes;

        log.debug("Fetching from partition {} at offset {} with maxBytes {}",
                topicIdPartition, fetchOffset, maxBytes);

        return initialized().thenCompose(logInstance -> getFetchOffsetRange(logInstance)
                        .thenCompose(offsetRange -> {
                            if (fetchOffset < offsetRange.logStartOffset()
                                    || fetchOffset > offsetRange.highWatermark()) {
                                log.debug("fetchOffset {} outside range [{}, {}] for partition {}",
                                        fetchOffset,
                                        offsetRange.logStartOffset(),
                                        offsetRange.highWatermark(),
                                        topicIdPartition);
                                return CompletableFuture.completedFuture(new FetchPartitionData(
                                        Errors.OFFSET_OUT_OF_RANGE,
                                        offsetRange.highWatermark(),
                                        offsetRange.logStartOffset(),
                                        MemoryRecords.EMPTY,
                                        Optional.empty(),
                                        OptionalLong.empty(),
                                        Optional.empty(),
                                        OptionalInt.empty(),
                                        false
                                ));
                            }

                            if (fetchOffset == offsetRange.highWatermark()) {
                                log.debug("fetchOffset {} == hwm {}, returning empty for partition {}",
                                        fetchOffset, offsetRange.highWatermark(), topicIdPartition);
                                return CompletableFuture.completedFuture(new FetchPartitionData(
                                        Errors.NONE,
                                        offsetRange.highWatermark(),
                                        offsetRange.logStartOffset(),
                                        MemoryRecords.EMPTY,
                                        Optional.empty(),
                                        OptionalLong.empty(),
                                        Optional.empty(),
                                        OptionalInt.empty(),
                                        false
                                ));
                            }

                            return readRecords(
                                    fetchOffset,
                                    offsetRange.highWatermark(),
                                    MAX_ENTRIES_PER_FETCH,
                                    maxBytes)
                                    .thenApply(records -> new FetchPartitionData(
                                            Errors.NONE,
                                            offsetRange.highWatermark(),
                                            offsetRange.logStartOffset(),
                                            records,
                                            Optional.empty(),
                                            OptionalLong.empty(),
                                            Optional.empty(),
                                            OptionalInt.empty(),
                                            false
                                    ));
                        }))
                .exceptionally(error -> createFetchErrorResponse(mapException(error)));
    }

    CompletableFuture<ListOffsetsPartitionResponse> listOffsets(ListOffsetsPartitionRequest request) {
        long timestamp = request.timestamp();
        log.debug("ListOffsets for partition {} with timestamp {}", topicIdPartition, timestamp);

        if (timestamp == ListOffsetsPartitionRequest.LATEST_TIERED_TIMESTAMP) {
            return CompletableFuture.completedFuture(ListOffsetsPartitionResponse.success(topicIdPartition, -1L, -1L));
        }

        return initialized().thenCompose(logInstance -> {
            if (timestamp == ListOffsetsPartitionRequest.EARLIEST_TIMESTAMP
                    || timestamp == ListOffsetsPartitionRequest.EARLIEST_LOCAL_TIMESTAMP) {
                return handleEarliestTimestamp(logInstance);
            }
            if (timestamp == ListOffsetsPartitionRequest.LATEST_TIMESTAMP) {
                return handleLatestTimestamp(logInstance);
            }
            if (timestamp == ListOffsetsPartitionRequest.MAX_TIMESTAMP) {
                return handleMaxTimestamp(logInstance);
            }
            return handleTimestampSearch(logInstance, timestamp);
        }).exceptionally(error -> ListOffsetsPartitionResponse.error(topicIdPartition, mapException(error)));
    }

    CompletableFuture<DisklessLogMetadata> logMetadata() {
        return initialized().thenCompose(logInstance -> getHighWatermark(logInstance)
                .thenApply(highWatermark -> new DisklessLogMetadata(logInstance.id().id(), highWatermark)));
    }

    CompletableFuture<PartitionResponse> appendIdempotentRecords(
            MemoryRecords records,
            RecordAnalysisResult analysisResult,
            String zone,
            CompletableFuture<Void> submissionFuture) {
        if (closed) {
            submissionFuture.complete(null);
            return CompletableFuture.completedFuture(notLeaderResponse());
        }

        ProducerStateManager producerStateManager = getOrCreateProducerStateManager(zone);
        List<ProducerStateManager.AppendBatch> appendBatches = buildAppendBatches(records);
        if (appendBatches.isEmpty()) {
            submissionFuture.complete(null);
            return CompletableFuture.completedFuture(new PartitionResponse(Errors.NONE));
        }

        return producerStateManager.prepareAppend(appendBatches)
                .thenCompose(prepareResult -> {
                    if (closed) {
                        NotLeaderOrFollowerException ownershipError = ownershipLostException();
                        if (prepareResult instanceof ProducerStateManager.Ready ready) {
                            producerStateManager.abortAppend(ready.pendingAppend(), ownershipError);
                        }
                        submissionFuture.complete(null);
                        return CompletableFuture.completedFuture(notLeaderResponse());
                    }

                    if (prepareResult instanceof ProducerStateManager.InvalidEpoch invalidEpoch) {
                        submissionFuture.complete(null);
                        return CompletableFuture.completedFuture(
                                new PartitionResponse(Errors.INVALID_PRODUCER_EPOCH, invalidEpoch.message()));
                    }
                    if (prepareResult instanceof ProducerStateManager.OutOfOrderSequence outOfOrderSequence) {
                        submissionFuture.complete(null);
                        return CompletableFuture.completedFuture(
                                new PartitionResponse(
                                        Errors.OUT_OF_ORDER_SEQUENCE_NUMBER,
                                        outOfOrderSequence.message()));
                    }
                    if (prepareResult instanceof ProducerStateManager.Duplicate duplicate) {
                        submissionFuture.complete(null);
                        return duplicate.appendResultFuture()
                                .thenApply(appendResult ->
                                        new PartitionResponse(
                                                Errors.NONE,
                                                appendResult.baseOffset(),
                                                appendResult.timestamp(),
                                                0L))
                                .exceptionally(this::writeErrorResponse);
                    }
                    if (prepareResult instanceof ProducerStateManager.Ready ready) {
                        return appendPreparedBatches(
                                records, analysisResult, producerStateManager, ready.pendingAppend(), submissionFuture);
                    }
                    submissionFuture.complete(null);
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("Unexpected prepare result: " + prepareResult));
                })
                .whenComplete((response, error) -> {
                    if (error != null) {
                        submissionFuture.complete(null);
                    }
                });
    }

    private CompletableFuture<PartitionResponse> appendPreparedBatches(
            MemoryRecords records,
            RecordAnalysisResult analysisResult,
            ProducerStateManager producerStateManager,
            ProducerStateManager.PendingAppend pendingAppend,
            CompletableFuture<Void> submissionFuture) {
        CompletableFuture<PartitionResponse> result = new CompletableFuture<>();

        initialized().whenComplete((logInstance, logError) -> {
            if (logError != null) {
                producerStateManager.abortAppend(pendingAppend, logError);
                submissionFuture.complete(null);
                result.completeExceptionally(logError);
                return;
            }

            if (closed) {
                NotLeaderOrFollowerException ownershipError = ownershipLostException();
                producerStateManager.abortAppend(pendingAppend, ownershipError);
                submissionFuture.complete(null);
                result.completeExceptionally(ownershipError);
                return;
            }

            final ByteBuf data;
            try {
                data = KafkaEntryFormatter.encode(records);
            } catch (Throwable encodeError) {
                producerStateManager.abortAppend(pendingAppend, encodeError);
                submissionFuture.complete(null);
                result.completeExceptionally(encodeError);
                return;
            }

            CompletableFuture<LogEntryHeader> appendFuture;
            try {
                log.debug("Appending {} records ({} bytes) to log {} for partition {}, analysisResult: {}",
                        analysisResult.recordCount(), data.readableBytes(), logInstance.id(),
                        topicIdPartition, analysisResult);
                appendFuture = logInstance.append(analysisResult.recordCount(), data);
                if (appendFuture == null) {
                    throw new IllegalStateException("Log.append returned null future");
                }
            } catch (Throwable appendInitError) {
                data.release();
                producerStateManager.abortAppend(pendingAppend, appendInitError);
                submissionFuture.complete(null);
                result.completeExceptionally(appendInitError);
                return;
            }

            submissionFuture.complete(null);
            appendFuture.whenComplete((entryHeader, appendError) -> {
                try {
                    if (appendError != null) {
                        producerStateManager.abortAppend(pendingAppend, appendError);
                        result.completeExceptionally(appendError);
                        return;
                    }

                    long appendTimestamp = state.time().milliseconds();
                    ProducerStateManager.AppendResult appendResult =
                            producerStateManager.completeAppend(pendingAppend, entryHeader.offset(), appendTimestamp);
                    result.complete(new PartitionResponse(
                            Errors.NONE,
                            appendResult.baseOffset(),
                            appendResult.timestamp(),
                            0L));
                } catch (Throwable completeError) {
                    producerStateManager.abortAppend(pendingAppend, completeError);
                    result.completeExceptionally(completeError);
                } finally {
                    data.release();
                }
            });
        });

        return result;
    }

    CompletableFuture<PartitionResponse> appendNonIdempotentRecords(
            MemoryRecords records,
            RecordAnalysisResult analysisResult,
            CompletableFuture<Void> submissionFuture) {
        CompletableFuture<PartitionResponse> result = new CompletableFuture<>();

        initialized().whenComplete((logInstance, logError) -> {
            if (logError != null) {
                submissionFuture.complete(null);
                result.completeExceptionally(logError);
                return;
            }

            if (closed) {
                submissionFuture.complete(null);
                result.completeExceptionally(ownershipLostException());
                return;
            }

            final ByteBuf data;
            try {
                data = KafkaEntryFormatter.encode(records);
            } catch (Throwable encodeError) {
                submissionFuture.complete(null);
                result.completeExceptionally(encodeError);
                return;
            }

            CompletableFuture<LogEntryHeader> appendFuture;
            try {
                log.debug("Appending {} records ({} bytes) to log {} for partition {}, analysisResult: {}",
                        analysisResult.recordCount(), data.readableBytes(), logInstance.id(),
                        topicIdPartition, analysisResult);
                appendFuture = logInstance.append(analysisResult.recordCount(), data);
                if (appendFuture == null) {
                    throw new IllegalStateException("Log.append returned null future");
                }
            } catch (Throwable appendInitError) {
                data.release();
                submissionFuture.complete(null);
                result.completeExceptionally(appendInitError);
                return;
            }

            submissionFuture.complete(null);
            appendFuture.whenComplete((entryHeader, appendError) -> {
                try {
                    if (appendError != null) {
                        result.completeExceptionally(appendError);
                        return;
                    }

                    long appendTimestamp = state.time().milliseconds();
                    result.complete(new PartitionResponse(
                            Errors.NONE,
                            entryHeader.offset(),
                            appendTimestamp,
                            0L));
                } catch (Throwable completeError) {
                    result.completeExceptionally(completeError);
                } finally {
                    data.release();
                }
            });
        });

        return result;
    }

    private CompletableFuture<Long> getHighWatermark(Log logInstance) {
        return logInstance.getLastOffset().thenApply(lastOffset -> {
            if (isInvalidOffset(lastOffset)) {
                return 0L;
            }
            return lastOffset.offset() + lastOffset.numberOfRecords();
        });
    }

    private CompletableFuture<FetchOffsetRange> getFetchOffsetRange(Log logInstance) {
        return getEarliestOffset(logInstance).thenCombine(
                getHighWatermark(logInstance),
                (logStartOffset, highWatermark) -> new FetchOffsetRange(logStartOffset, highWatermark));
    }

    private CompletableFuture<Long> getEarliestOffset(Log logInstance) {
        return logInstance.getFirstOffset().thenApply(firstOffset ->
                isInvalidOffset(firstOffset) ? 0L : firstOffset.offset());
    }

    private boolean isInvalidOffset(LogOffset offset) {
        return offset == null || offset.offset() < 0;
    }

    private ByteBuffer writableKafkaRecords(ByteBuf entryBuffer, long baseOffset) {
        ByteBuffer decoded = KafkaEntryFormatter.decode(entryBuffer.duplicate());
        ByteBuffer writable = ByteBuffer.allocate(decoded.remaining());
        writable.put(decoded.duplicate());
        writable.flip();
        if (writable.remaining() >= Long.BYTES) {
            writable.putLong(writable.position(), baseOffset);
        }
        return writable;
    }

    private long[] findFirstTimestampGe(ByteBuf entryBuffer, long baseOffset, long targetTimestamp) {
        try {
            MemoryRecords records = MemoryRecords.readableRecords(writableKafkaRecords(entryBuffer, baseOffset));
            for (RecordBatch batch : records.batches()) {
                for (Record record : batch) {
                    if (record.timestamp() >= targetTimestamp) {
                        return new long[]{record.timestamp(), record.offset()};
                    }
                }
            }
            return null;
        } catch (Exception e) {
            log.warn("Failed to parse records at offset {} for timestamp search", baseOffset, e);
            return null;
        }
    }

    private TimestampAndOffset findMaxTimestamp(ByteBuf entryBuffer, long baseOffset) {
        try {
            TimestampAndOffset best = null;
            MemoryRecords records = MemoryRecords.readableRecords(writableKafkaRecords(entryBuffer, baseOffset));
            for (RecordBatch batch : records.batches()) {
                for (Record record : batch) {
                    if (record.timestamp() == RecordBatch.NO_TIMESTAMP) {
                        continue;
                    }
                    if (best == null || record.timestamp() > best.timestamp()) {
                        best = new TimestampAndOffset(record.timestamp(), record.offset());
                    }
                }
            }
            return best;
        } catch (Exception e) {
            log.warn("Failed to parse records at offset {} for max-timestamp search", baseOffset, e);
            return null;
        }
    }

    private CompletableFuture<MemoryRecords> readRecords(
            long fetchOffset,
            long maxOffsetExclusive,
            int maxEntries,
            int maxBytes) {
        LogCursorPool pool = fetchCursorPool;
        if (pool == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Fetch cursor pool is not initialized for " + topicIdPartition));
        }

        CompletableFuture<MemoryRecords> result = new CompletableFuture<>();
        CompletableFuture<LogCursorPool.Lease> acquireFuture = pool.acquire(fetchOffset);
        acquireFuture.whenComplete((lease, acquireError) -> {
            if (acquireError != null) {
                completeFromSource(result, acquireFuture, null, acquireError);
                return;
            }
            if (result.isDone()) {
                lease.close();
                return;
            }

            CompletableFuture<List<LogEntry>> readFuture;
            try {
                readFuture = lease.cursor().readEntries(
                        maxEntries, maxBytes, null, maxOffsetExclusive);
                if (readFuture == null) {
                    throw new IllegalStateException("LogCursor.readEntries returned null future");
                }
            } catch (Throwable readError) {
                lease.close();
                result.completeExceptionally(readError);
                return;
            }

            readFuture.whenComplete((entries, readError) -> {
                MemoryRecords records = null;
                Throwable completionError = readError;
                try {
                    if (completionError == null && !result.isDone()) {
                        records = convertLogEntriesToMemoryRecords(entries);
                    }
                } catch (Throwable conversionError) {
                    completionError = conversionError;
                } finally {
                    completionError = closeLogEntries(entries, completionError);
                    lease.close();
                }

                if (completionError != null) {
                    completeFromSource(result, readFuture, null, completionError);
                } else {
                    result.complete(records);
                }
            });
        });
        return result;
    }

    private MemoryRecords convertLogEntriesToMemoryRecords(List<LogEntry> entries) {
        if (entries.isEmpty()) {
            return MemoryRecords.EMPTY;
        }

        List<ByteBuffer> decodedEntries = new ArrayList<>(entries.size());
        int totalSize = 0;
        for (LogEntry entry : entries) {
            ByteBuf payload = entry.payload();
            if (payload == null || payload.readableBytes() == 0) {
                continue;
            }

            ByteBuffer kafkaRecords = writableKafkaRecords(payload, entry.offset());
            if (!kafkaRecords.hasRemaining()) {
                continue;
            }
            totalSize = Math.addExact(totalSize, kafkaRecords.remaining());
            decodedEntries.add(kafkaRecords);
        }

        if (totalSize == 0) {
            return MemoryRecords.EMPTY;
        }

        ByteBuffer combined = ByteBuffer.allocate(totalSize);
        for (ByteBuffer kafkaRecords : decodedEntries) {
            combined.put(kafkaRecords);
        }
        combined.flip();
        return MemoryRecords.readableRecords(combined);
    }

    private CompletableFuture<ListOffsetsPartitionResponse> handleEarliestTimestamp(Log logInstance) {
        return getEarliestOffset(logInstance).thenApply(offset -> {
            log.debug("EARLIEST for partition {}: offset={}", topicIdPartition, offset);
            return ListOffsetsPartitionResponse.success(topicIdPartition, offset, UNKNOWN_TIMESTAMP);
        });
    }

    private CompletableFuture<ListOffsetsPartitionResponse> handleLatestTimestamp(Log logInstance) {
        return getHighWatermark(logInstance).thenApply(highWatermark -> {
            log.debug("LATEST for partition {}: hwm={}", topicIdPartition, highWatermark);
            return ListOffsetsPartitionResponse.success(topicIdPartition, highWatermark, UNKNOWN_TIMESTAMP);
        });
    }

    private CompletableFuture<ListOffsetsPartitionResponse> handleMaxTimestamp(Log logInstance) {
        return logInstance.getFirstOffset().thenCompose(firstOffset -> {
            if (isInvalidOffset(firstOffset)) {
                return listOffsetsNotFound();
            }

            return getHighWatermark(logInstance)
                    .thenCompose(highWatermark -> scanForMaxTimestamp(
                            logInstance, firstOffset.offset(), highWatermark))
                    .thenApply(result -> result == null
                            ? ListOffsetsPartitionResponse.success(topicIdPartition, -1L, -1L)
                            : ListOffsetsPartitionResponse.success(
                                    topicIdPartition, result.offset(), result.timestamp()));
        });
    }

    private CompletableFuture<ListOffsetsPartitionResponse> handleTimestampSearch(
            Log logInstance,
            long targetTimestamp) {
        return logInstance.getFirstOffset().thenCompose(firstOffset -> {
            if (isInvalidOffset(firstOffset)) {
                return listOffsetsNotFound();
            }

            return getHighWatermark(logInstance)
                    .thenCompose(highWatermark -> scanForFirstTimestampAtOrAfter(
                            logInstance, firstOffset.offset(), highWatermark, targetTimestamp))
                    .thenApply(result -> result == null
                            ? ListOffsetsPartitionResponse.success(topicIdPartition, -1L, -1L)
                            : ListOffsetsPartitionResponse.success(topicIdPartition, result[1], result[0]));
        });
    }

    private CompletableFuture<long[]> scanForFirstTimestampAtOrAfter(
            Log logInstance,
            long startOffset,
            long maxOffset,
            long targetTimestamp) {
        CompletableFuture<LogCursor> cursorFuture;
        try {
            cursorFuture = logInstance.openEphemeralCursor(
                    "kafka-scan-for-timestamp-" + System.nanoTime(), startOffset);
            if (cursorFuture == null) {
                throw new IllegalStateException("Log.openEphemeralCursor returned null future");
            }
        } catch (Throwable openError) {
            return CompletableFuture.failedFuture(openError);
        }
        return runWithClosingCursor(
                cursorFuture,
                cursor -> scanCursorForFirstTimestampAtOrAfter(cursor, maxOffset, targetTimestamp),
                "timestamp-scan");
    }

    private CompletableFuture<long[]> scanCursorForFirstTimestampAtOrAfter(
            LogCursor cursor,
            long maxOffset,
            long targetTimestamp) {
        CompletableFuture<List<LogEntry>> readFuture;
        try {
            readFuture = cursor.readEntries(MAX_ENTRIES_PER_FETCH, Long.MAX_VALUE, null, maxOffset);
            if (readFuture == null) {
                throw new IllegalStateException("LogCursor.readEntries returned null future");
            }
        } catch (Throwable readError) {
            return CompletableFuture.failedFuture(readError);
        }
        return consumeLogEntries(readFuture, entries -> {
            if (entries.isEmpty()) {
                return new ScanBatchResult<long[]>(true, null);
            }

            for (LogEntry entry : entries) {
                long[] found = findFirstTimestampGe(entry.payload(), entry.offset(), targetTimestamp);
                if (found != null) {
                    return new ScanBatchResult<>(true, found);
                }
            }

            return new ScanBatchResult<long[]>(false, null);
        }).thenCompose(batchResult -> batchResult.done()
                ? CompletableFuture.completedFuture(batchResult.value())
                : scanCursorForFirstTimestampAtOrAfter(cursor, maxOffset, targetTimestamp));
    }

    private CompletableFuture<TimestampAndOffset> scanForMaxTimestamp(
            Log logInstance,
            long startOffset,
            long maxOffset) {
        CompletableFuture<LogCursor> cursorFuture;
        try {
            cursorFuture = logInstance.openEphemeralCursor(
                    "kafka-scan-for-max-timestamp-" + System.nanoTime(), startOffset);
            if (cursorFuture == null) {
                throw new IllegalStateException("Log.openEphemeralCursor returned null future");
            }
        } catch (Throwable openError) {
            return CompletableFuture.failedFuture(openError);
        }
        return runWithClosingCursor(
                cursorFuture,
                cursor -> scanCursorForMaxTimestamp(cursor, maxOffset, null),
                "max-timestamp-scan");
    }

    private CompletableFuture<TimestampAndOffset> scanCursorForMaxTimestamp(
            LogCursor cursor,
            long maxOffset,
            TimestampAndOffset bestSoFar) {
        CompletableFuture<List<LogEntry>> readFuture;
        try {
            readFuture = cursor.readEntries(MAX_ENTRIES_PER_FETCH, Long.MAX_VALUE, null, maxOffset);
            if (readFuture == null) {
                throw new IllegalStateException("LogCursor.readEntries returned null future");
            }
        } catch (Throwable readError) {
            return CompletableFuture.failedFuture(readError);
        }
        return consumeLogEntries(readFuture, entries -> {
            if (entries.isEmpty()) {
                return new ScanBatchResult<>(true, bestSoFar);
            }

            TimestampAndOffset best = bestSoFar;
            for (LogEntry entry : entries) {
                TimestampAndOffset candidate = findMaxTimestamp(entry.payload(), entry.offset());
                if (candidate != null && (best == null || candidate.timestamp() > best.timestamp())) {
                    best = candidate;
                }
            }

            return new ScanBatchResult<>(false, best);
        }).thenCompose(batchResult -> batchResult.done()
                ? CompletableFuture.completedFuture(batchResult.value())
                : scanCursorForMaxTimestamp(cursor, maxOffset, batchResult.value()));
    }

    private <T> CompletableFuture<T> runWithClosingCursor(
            CompletableFuture<LogCursor> cursorFuture,
            Function<LogCursor, CompletableFuture<T>> operation,
            String purpose) {
        CompletableFuture<T> result = new CompletableFuture<>();
        cursorFuture.whenComplete((cursor, openError) -> {
            if (openError != null) {
                completeFromSource(result, cursorFuture, null, openError);
                return;
            }
            if (cursor == null) {
                result.completeExceptionally(
                        new IllegalStateException("Log.openEphemeralCursor returned null cursor"));
                return;
            }
            if (result.isDone()) {
                closeCursorQuietly(cursor, purpose);
                return;
            }

            CompletableFuture<T> operationFuture;
            try {
                operationFuture = operation.apply(cursor);
                if (operationFuture == null) {
                    throw new IllegalStateException("Cursor operation returned null future");
                }
            } catch (Throwable operationError) {
                closeCursorQuietly(cursor, purpose);
                result.completeExceptionally(operationError);
                return;
            }
            operationFuture.whenComplete((value, operationError) -> {
                closeCursorQuietly(cursor, purpose);
                completeFromSource(result, operationFuture, value, operationError);
            });
        });
        return result;
    }

    private <T> CompletableFuture<T> consumeLogEntries(
            CompletableFuture<List<LogEntry>> readFuture,
            Function<List<LogEntry>, T> consumer) {
        CompletableFuture<T> result = new CompletableFuture<>();
        readFuture.whenComplete((entries, readError) -> {
            T value = null;
            Throwable completionError = readError;
            try {
                if (completionError == null && !result.isDone()) {
                    value = consumer.apply(entries);
                }
            } catch (Throwable consumerError) {
                completionError = consumerError;
            } finally {
                completionError = closeLogEntries(entries, completionError);
            }

            if (completionError != null) {
                completeFromSource(result, readFuture, null, completionError);
            } else {
                result.complete(value);
            }
        });
        return result;
    }

    private Throwable closeLogEntries(List<LogEntry> entries, Throwable precedingError) {
        try {
            OwnedResultFutures.closeLogEntries(entries);
        } catch (Throwable closeError) {
            if (precedingError == null) {
                return closeError;
            }
            if (precedingError != closeError) {
                precedingError.addSuppressed(closeError);
            }
        }
        return precedingError;
    }

    private static <T> void completeFromSource(
            CompletableFuture<T> result,
            CompletableFuture<?> source,
            T value,
            Throwable error) {
        if (error == null) {
            result.complete(value);
        } else if (source.isCancelled()) {
            result.cancel(false);
        } else {
            result.completeExceptionally(error);
        }
    }

    private void closeCursorQuietly(LogCursor cursor, String purpose) {
        try {
            cursor.close();
        } catch (Exception e) {
            log.warn("Failed to close {} cursor for partition {}", purpose, topicIdPartition, e);
        }
    }

    private CompletableFuture<ListOffsetsPartitionResponse> listOffsetsNotFound() {
        return CompletableFuture.completedFuture(ListOffsetsPartitionResponse.success(topicIdPartition, -1L, -1L));
    }

    private Errors mapException(Throwable error) {
        Throwable cause = unwrapCompletionException(error);
        if (cause instanceof NotLeaderOrFollowerException || isClosedOrFenced(cause)) {
            invalidate();
            return Errors.NOT_LEADER_OR_FOLLOWER;
        }
        if (isNotFound(cause)) {
            invalidate();
            return Errors.UNKNOWN_TOPIC_OR_PARTITION;
        }
        if (isOffsetOutOfRange(cause)) {
            return Errors.OFFSET_OUT_OF_RANGE;
        }
        return Errors.KAFKA_STORAGE_ERROR;
    }

    private PartitionResponse writeErrorResponse(Throwable error) {
        Throwable cause = unwrapCompletionException(error);
        if (cause instanceof NotLeaderOrFollowerException || isClosedOrFenced(cause)) {
            invalidate();
            log.info("Partition log is no longer local owner for partition {}", topicIdPartition, cause);
            return new PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER);
        }

        log.error("Failed to write to partition {}", topicIdPartition, error);
        return new PartitionResponse(Errors.KAFKA_STORAGE_ERROR);
    }

    private Throwable unwrapCompletionException(Throwable error) {
        Throwable cause = error;
        while (cause.getCause() != null && cause instanceof CompletionException) {
            cause = cause.getCause();
        }
        return cause;
    }

    private static boolean isClosedOrFenced(Throwable error) {
        Throwable cause = error;
        while (cause != null) {
            String className = cause.getClass().getSimpleName();
            String message = cause.getMessage();
            if (className.contains("Fenced")
                    || className.contains("AlreadyClosed")
                    || className.contains("ClosedException")
                    || isClosedOrFencedMessage(message)) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private static boolean isClosedOrFencedMessage(String message) {
        if (message == null) {
            return false;
        }
        String normalized = message.trim().toLowerCase(Locale.ROOT);
        if (normalized.equals("already closed") || normalized.startsWith("already closed:")) {
            return true;
        }
        boolean lifecycleSubject = normalized.startsWith("stream ") || normalized.startsWith("log ");
        return lifecycleSubject
                && (normalized.endsWith(" is fenced") || normalized.endsWith(" is closed"));
    }

    private static boolean isNotFound(Throwable error) {
        Throwable cause = error;
        while (cause != null) {
            String className = cause.getClass().getSimpleName();
            if (className.contains("NotFound") || className.contains("NoSuchStream")) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private static boolean isOffsetOutOfRange(Throwable error) {
        Throwable cause = error;
        while (cause != null) {
            String className = cause.getClass().getSimpleName();
            if (className.contains("OffsetOutOfRange") || className.contains("NoSuchOffset")) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private FetchPartitionData createFetchErrorResponse(Errors error) {
        return new FetchPartitionData(
                error,
                -1,
                -1,
                MemoryRecords.EMPTY,
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                false
        );
    }

    boolean cleanupNonOwnedProducerStates(Set<String> ownedZones, boolean deletePartition) {
        boolean cleaned = false;
        for (String zone : snapshotProducerStateZones()) {
            if (ownedZones.contains(zone)) {
                continue;
            }
            cleaned = cleanupProducerState(zone, deletePartition) || cleaned;
        }
        return cleaned;
    }

    boolean cleanup(boolean deletePartition) {
        boolean cleaned = false;
        for (String zone : snapshotProducerStateZones()) {
            cleaned = cleanupProducerState(zone, deletePartition) || cleaned;
        }
        return cleaned;
    }

    Set<String> snapshotProducerStateZones() {
        return new LinkedHashSet<>(producerStateManagers.keySet());
    }

    void close() {
        close(false);
    }

    void close(boolean deletePartition) {
        CompletableFuture<Log> retentionFuture;
        CompletableFuture<Long> trimFuture;
        synchronized (this) {
            closed = true;
            retentionFuture = inFlightRetention.getAndSet(null);
            trimFuture = activeTrimFuture;
        }
        pendingRetention.set(null);
        if (retentionFuture != null) {
            retentionFuture.cancel(false);
        }
        awaitTrimCompletion(trimFuture);
        cleanupWriteState();
        cleanupGlobalState();
        closeLog();
        cleanup(deletePartition);
    }

    private void awaitTrimCompletion(CompletableFuture<Long> trimFuture) {
        if (trimFuture == null) {
            return;
        }
        trimFuture.handle((ignored, error) -> {
            if (error != null) {
                log.debug("Retention trim settled with an error while closing {}", topicIdPartition, error);
            }
            return null;
        }).join();
    }

    void cleanupWriteState() {
        writeSequencer.reset();
    }

    void cleanupReadState() {
        closeFetchCursorPool();
    }

    boolean cleanupGlobalState() {
        boolean cleaned = logMetrics.remove(topicIdPartition);
        return closeFetchCursorPool() || cleaned;
    }

    private boolean cleanupProducerState(String zone, boolean deletePartition) {
        ProducerStateManager producerStateManager = producerStateManagers.remove(zone);
        if (producerStateManager == null) {
            return false;
        }

        producerStateManager.cleanup(deletePartition).exceptionally(error -> {
            log.warn("Failed to cleanup producer state manager for partition {} and zone {}",
                    topicIdPartition, zone, error);
            return null;
        });
        return true;
    }

    private CompletableFuture<Log> createInitFuture(CompletableFuture<Log> logFuture) {
        CompletableFuture<Log> initialized = new CompletableFuture<>();
        logFuture.whenComplete((logInstance, error) -> {
            if (error != null) {
                if (closed) {
                    initialized.completeExceptionally(
                            new NotLeaderOrFollowerException("Partition log already closed"));
                } else {
                    log.warn("Failed to open Log for partition {}, evicting from cache",
                            topicIdPartition, error);
                    cleanupWriteState();
                    cleanupReadState();
                    cleanup(false);
                    initialized.completeExceptionally(error);
                    // An already-failed future invokes this callback inside the
                    // ConcurrentHashMap.computeIfAbsent mapping function, where removing the
                    // same key is a recursive update. In that case getOrCreatePartitionLog
                    // evicts the failed value immediately after it is published.
                    if (state.partitionLog(topicIdPartition) == this) {
                        state.removePartitionLog(topicIdPartition, this);
                    }
                }
                return;
            }

            if (closed) {
                fenceLogUnlessReplaced(logInstance);
                closeLogQuietly(logInstance);
                initialized.completeExceptionally(
                        new NotLeaderOrFollowerException("Partition log already closed"));
                return;
            }

            UrsaPartitionLog activePartitionLog = state.partitionLog(topicIdPartition);
            if (activePartitionLog != null && activePartitionLog != this) {
                closeLogQuietly(logInstance);
                initialized.completeExceptionally(
                        new NotLeaderOrFollowerException("Partition log already replaced"));
                return;
            }

            try {
                activateAndInitGlobalState(logInstance, initialized);
            } catch (Throwable activationError) {
                fenceLogUnlessReplaced(logInstance);
                closeLogQuietly(logInstance);
                initialized.completeExceptionally(activationError);
            }
        });
        return initialized;
    }

    private CompletableFuture<Log> initialized() {
        return initFuture;
    }

    boolean initializationFailed() {
        return initFuture.isCompletedExceptionally();
    }

    void triggerInitialRetention(long retentionMs, long retentionBytes) {
        if (initialRetentionTriggered.compareAndSet(false, true)) {
            triggerRetention(retentionMs, retentionBytes);
        }
    }

    void triggerRetention(long retentionMs, long retentionBytes) {
        if (closed) {
            return;
        }
        pendingRetention.set(new RetentionRequest(retentionMs, retentionBytes));
        if (retentionWorkerRunning.compareAndSet(false, true)) {
            runNextRetention();
        }
    }

    private void runNextRetention() {
        if (closed) {
            pendingRetention.set(null);
            retentionWorkerRunning.set(false);
            return;
        }

        RetentionRequest request = pendingRetention.getAndSet(null);
        if (request == null) {
            retentionWorkerRunning.set(false);
            // Close the race where a request arrived after getAndSet(null) but before
            // retentionWorkerRunning became false.
            if (pendingRetention.get() != null && retentionWorkerRunning.compareAndSet(false, true)) {
                runNextRetention();
            }
            return;
        }

        CompletableFuture<Log> retentionFuture;
        try {
            retentionFuture = initialized().thenCompose(logInstance -> state.maybeApplyRetention(
                    this,
                    logInstance,
                    request.retentionMs(),
                    request.retentionBytes()));
        } catch (Throwable error) {
            log.warn("Failed to start retention for {}", topicIdPartition, error);
            runNextRetention();
            return;
        }
        synchronized (this) {
            if (closed) {
                retentionFuture.cancel(false);
                retentionWorkerRunning.set(false);
                return;
            }
            inFlightRetention.set(retentionFuture);
        }
        retentionFuture.whenComplete((ignored, error) -> {
            inFlightRetention.compareAndSet(retentionFuture, null);
            if (error != null && !closed) {
                Throwable cause = unwrapCompletionException(error);
                if (isClosedOrFenced(cause)) {
                    invalidate();
                } else {
                    log.warn("Failed to apply retention for {}", topicIdPartition, error);
                }
            }
            runNextRetention();
        });
    }

    CompletableFuture<Log> softTrimIfActive(Log logInstance, long trimOffset) {
        synchronized (this) {
            if (closed || state.partitionLog(topicIdPartition) != this) {
                return CompletableFuture.completedFuture(logInstance);
            }
            try {
                CompletableFuture<Long> trimFuture = logInstance.softTrim(trimOffset);
                if (trimFuture == null) {
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("Log.softTrim returned null future"));
                }
                activeTrimFuture = trimFuture;
                trimFuture.whenComplete((ignored, error) -> {
                    synchronized (this) {
                        if (activeTrimFuture == trimFuture) {
                            activeTrimFuture = null;
                        }
                    }
                });
                return trimFuture.thenApply(ignored -> logInstance);
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
        }
    }

    private record RetentionRequest(long retentionMs, long retentionBytes) {
    }

    private void activateAndInitGlobalState(
            Log logInstance,
            CompletableFuture<Log> initialized) {
        synchronized (this) {
            if (closed) {
                throw new NotLeaderOrFollowerException("Partition log already closed");
            }
            UrsaPartitionLog activePartitionLog = state.partitionLog(topicIdPartition);
            if (activePartitionLog != null && activePartitionLog != this) {
                throw new NotLeaderOrFollowerException("Partition log already replaced");
            }
            logInstance.activate();
            if (fetchCursorPool == null) {
                fetchCursorPool = new LogCursorPool(
                        logInstance,
                        fetchCursorNamePrefix(topicIdPartition),
                        FETCH_CURSOR_POOL_SIZE);
                try {
                    logMetrics.register(topicIdPartition, logInstance);
                } catch (Throwable registrationError) {
                    logMetrics.remove(topicIdPartition);
                    fetchCursorPool.close();
                    fetchCursorPool = null;
                    throw registrationError;
                }
            }
            initialized.complete(logInstance);
        }
    }

    private void closeLog() {
        initialized().whenComplete((logInstance, error) -> {
            if (logInstance != null) {
                fenceLogUnlessReplaced(logInstance);
                closeLogQuietly(logInstance);
            }
        });
    }

    private void fenceLogUnlessReplaced(Log logInstance) {
        UrsaPartitionLog activePartitionLog = state.partitionLog(topicIdPartition);
        if (activePartitionLog != null && activePartitionLog != this) {
            return;
        }
        try {
            logInstance.fence();
        } catch (Exception e) {
            log.warn("Failed to fence Log for partition {}", topicIdPartition, e);
        }
    }

    private void closeLogQuietly(Log logInstance) {
        try {
            logInstance.close();
        } catch (Exception e) {
            log.warn("Failed to close Log for partition {}", topicIdPartition, e);
        }
    }

    private boolean closeFetchCursorPool() {
        LogCursorPool pool;
        synchronized (this) {
            pool = fetchCursorPool;
            fetchCursorPool = null;
        }
        if (pool == null) {
            return false;
        }
        try {
            pool.close();
        } catch (Exception e) {
            log.warn("Failed to close fetch cursor pool for partition {}", topicIdPartition, e);
        }
        return true;
    }

    private static List<ProducerStateManager.AppendBatch> buildAppendBatches(MemoryRecords records) {
        List<ProducerStateManager.AppendBatch> appendBatches = new ArrayList<>();
        for (RecordBatch batch : records.batches()) {
            if (!batch.hasProducerId()) {
                continue;
            }
            int recordCount = batch.countOrNull() != null ? batch.countOrNull() : 0;
            appendBatches.add(new ProducerStateManager.AppendBatch(
                    batch.producerId(),
                    batch.producerEpoch(),
                    batch.baseSequence(),
                    batch.lastSequence(),
                    recordCount,
                    batch.maxTimestamp()
            ));
        }
        return appendBatches;
    }

    private static String fetchCursorNamePrefix(TopicIdPartition tp) {
        return "kafka-fetch-" + tp.topic() + "-partition-" + tp.partition() + "-cursor";
    }

    private record FetchOffsetRange(long logStartOffset, long highWatermark) {
    }

    private record ScanBatchResult<T>(boolean done, T value) {
    }

    private record TimestampAndOffset(long timestamp, long offset) {
    }
}
