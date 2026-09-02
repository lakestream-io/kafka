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
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionRequest;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionResponse;
import org.apache.kafka.storage.diskless.handlers.RecordAnalyzer.RecordAnalysisResult;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
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

import io.lakestream.api.Log;
import io.lakestream.api.LogEntryHeader;
import io.lakestream.api.exception.LogFencedException;
import io.lakestream.api.exception.NoSuchStreamException;
import io.netty.buffer.ByteBuf;
import io.oxia.client.api.AsyncOxiaClient;

final class UrsaPartitionLog {

    private static final Logger log = LoggerFactory.getLogger(UrsaPartitionLog.class);
    private static final int MAX_ENTRIES_PER_FETCH = 10;

    private final TopicIdPartition topicIdPartition;
    private final UrsaStorageState state;
    private final Supplier<AsyncOxiaClient> oxiaClientSupplier;
    private final long producerStateSnapshotIntervalMs;
    private final int producerStateSnapshotRecordThreshold;
    private final ScheduledExecutorService producerStateScheduler;
    private final DisklessLogMetrics logMetrics;
    private final CompletableFuture<Log> logFuture;
    private final CompletableFuture<Log> initFuture;
    private final PartitionWriteSequencer writeSequencer;
    private final AtomicBoolean initialRetentionTriggered = new AtomicBoolean();
    private final AtomicBoolean retentionWorkerRunning = new AtomicBoolean();
    private final AtomicReference<RetentionRequest> pendingRetention = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<Log>> inFlightRetention = new AtomicReference<>();
    private final Set<OwnedWritePayload> ownedWritePayloads = ConcurrentHashMap.newKeySet();
    private CompletableFuture<Long> activeTrimFuture;
    private volatile PartitionReader reader;
    private volatile boolean closed;
    private CompletableFuture<Void> logCloseAttempt;
    private final CompletableFuture<Void> logCloseDrain = new CompletableFuture<>();
    private boolean logClosed;
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
        this.logFuture = logFuture;
        this.initFuture = createInitFuture(logFuture);
    }

    synchronized ProducerStateManager getOrCreateProducerStateManager(String zone) {
        if (closed) {
            throw ownershipLostException();
        }
        return producerStateManagers.computeIfAbsent(zone, zoneId -> new ProducerStateManager(
                topicIdPartition,
                oxiaClientSupplier,
                () -> initFuture,
                zoneId,
                producerStateSnapshotIntervalMs,
                producerStateSnapshotRecordThreshold,
                producerStateScheduler));
    }

    TopicIdPartition topicIdPartition() {
        return topicIdPartition;
    }

    synchronized void installProducerStateManager(String zone, ProducerStateManager producerStateManager) {
        if (closed) {
            throw ownershipLostException();
        }
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

        PreparedWrite preparedWrite = prepareWrite(records);
        if (preparedWrite.errorResponse != null) {
            return CompletableFuture.completedFuture(preparedWrite.errorResponse);
        }

        OwnedWritePayload payload = preparedWrite.payload;
        ownedWritePayloads.add(payload);
        if (closed) {
            payload.release();
            return CompletableFuture.completedFuture(notLeaderResponse());
        }

        try {
            return writeSequencer.submit(() -> writeValidated(
                    payload,
                    preparedWrite.analysisResult,
                    preparedWrite.appendBatches,
                    preparedWrite.idempotent,
                    zone));
        } catch (Throwable submitError) {
            payload.release();
            return CompletableFuture.completedFuture(writeErrorResponse(submitError));
        }
    }

    private PreparedWrite prepareWrite(MemoryRecords records) {
        boolean hasProducerId = false;
        for (RecordBatch batch : records.batches()) {
            if (batch.isTransactional()) {
                log.warn("Transactional produce rejected for partition {}", topicIdPartition);
                return new PreparedWrite(null, null, null, false, new PartitionResponse(Errors.INVALID_REQUEST));
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
            return new PreparedWrite(null, null, null, false, new PartitionResponse(Errors.INVALID_RECORD));
        }

        try {
            KafkaRecordsPayload.validateForAppend(records, analysisResult.recordCount());
        } catch (Exception e) {
            log.warn("Raw MemoryRecords validation failed for partition {}: {}", topicIdPartition, e.getMessage());
            return new PreparedWrite(null, null, null, false, new PartitionResponse(Errors.INVALID_RECORD));
        }

        final boolean idempotent = hasProducerId;
        final List<ProducerStateManager.AppendBatch> appendBatches;
        final OwnedWritePayload payload;
        try {
            appendBatches = idempotent ? buildAppendBatches(records) : List.of();
            payload = new OwnedWritePayload(KafkaRecordsPayload.copyForAppend(records));
        } catch (Throwable prepareError) {
            log.error("Failed to copy records for partition {}", topicIdPartition, prepareError);
            return new PreparedWrite(null, null, null, false, new PartitionResponse(Errors.KAFKA_STORAGE_ERROR));
        }
        return new PreparedWrite(payload, analysisResult, appendBatches, idempotent, null);
    }

    private PartitionWriteSequencer.WriteTask<PartitionResponse> writeValidated(
            OwnedWritePayload payload,
            RecordAnalysisResult analysisResult,
            List<ProducerStateManager.AppendBatch> appendBatches,
            boolean idempotent,
            String zone) {
        if (closed) {
            payload.release();
            return closedWriteTask();
        }

        CompletableFuture<Void> submissionFuture = new CompletableFuture<>();
        CompletableFuture<PartitionResponse> result;
        try {
            result = idempotent
                    ? appendIdempotentRecords(
                            payload, appendBatches, analysisResult, zone, submissionFuture)
                    : appendNonIdempotentRecords(payload, analysisResult, submissionFuture);
        } catch (Throwable writeError) {
            payload.release();
            submissionFuture.complete(null);
            result = CompletableFuture.failedFuture(writeError);
        }
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
        return initialized()
                .thenCompose(logInstance -> activeReader().fetch(partitionData))
                .exceptionally(error -> createFetchErrorResponse(mapException(error)));
    }

    CompletableFuture<ListOffsetsPartitionResponse> listOffsets(ListOffsetsPartitionRequest request) {
        return initialized()
                .thenCompose(logInstance -> activeReader().listOffsets(request))
                .exceptionally(error -> ListOffsetsPartitionResponse.error(topicIdPartition, mapException(error)));
    }

    /** The reader installed once the log opened; it is dropped as soon as this partition log closes. */
    private PartitionReader activeReader() {
        PartitionReader currentReader = reader;
        if (currentReader == null) {
            throw ownershipLostException();
        }
        return currentReader;
    }

    CompletableFuture<PartitionResponse> appendIdempotentRecords(
            OwnedWritePayload payload,
            List<ProducerStateManager.AppendBatch> appendBatches,
            RecordAnalysisResult analysisResult,
            String zone,
            CompletableFuture<Void> submissionFuture) {
        if (closed) {
            payload.release();
            submissionFuture.complete(null);
            return CompletableFuture.completedFuture(notLeaderResponse());
        }

        ProducerStateManager producerStateManager = getOrCreateProducerStateManager(zone);
        if (appendBatches.isEmpty()) {
            payload.release();
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
                        payload.release();
                        submissionFuture.complete(null);
                        return CompletableFuture.completedFuture(notLeaderResponse());
                    }

                    if (prepareResult instanceof ProducerStateManager.InvalidEpoch invalidEpoch) {
                        payload.release();
                        submissionFuture.complete(null);
                        return CompletableFuture.completedFuture(
                                new PartitionResponse(Errors.INVALID_PRODUCER_EPOCH, invalidEpoch.message()));
                    }
                    if (prepareResult instanceof ProducerStateManager.OutOfOrderSequence outOfOrderSequence) {
                        payload.release();
                        submissionFuture.complete(null);
                        return CompletableFuture.completedFuture(
                                new PartitionResponse(
                                        Errors.OUT_OF_ORDER_SEQUENCE_NUMBER,
                                        outOfOrderSequence.message()));
                    }
                    if (prepareResult instanceof ProducerStateManager.Duplicate duplicate) {
                        payload.release();
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
                                payload,
                                analysisResult,
                                producerStateManager,
                                ready.pendingAppend(),
                                submissionFuture);
                    }
                    payload.release();
                    submissionFuture.complete(null);
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("Unexpected prepare result: " + prepareResult));
                })
                .whenComplete((response, error) -> {
                    if (error != null) {
                        payload.release();
                        submissionFuture.complete(null);
                    }
                });
    }

    private CompletableFuture<PartitionResponse> appendPreparedBatches(
            OwnedWritePayload payload,
            RecordAnalysisResult analysisResult,
            ProducerStateManager producerStateManager,
            ProducerStateManager.PendingAppend pendingAppend,
            CompletableFuture<Void> submissionFuture) {
        CompletableFuture<PartitionResponse> result = new CompletableFuture<>();

        initialized().whenComplete((logInstance, logError) -> {
            if (logError != null) {
                producerStateManager.abortAppend(pendingAppend, logError);
                payload.release();
                submissionFuture.complete(null);
                result.completeExceptionally(logError);
                return;
            }

            if (closed) {
                NotLeaderOrFollowerException ownershipError = ownershipLostException();
                producerStateManager.abortAppend(pendingAppend, ownershipError);
                payload.release();
                submissionFuture.complete(null);
                result.completeExceptionally(ownershipError);
                return;
            }

            if (!payload.beginAppend()) {
                NotLeaderOrFollowerException ownershipError = ownershipLostException();
                producerStateManager.abortAppend(pendingAppend, ownershipError);
                submissionFuture.complete(null);
                result.completeExceptionally(ownershipError);
                return;
            }
            ByteBuf data = payload.buffer();

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
                payload.release();
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
                    payload.release();
                }
            });
        });

        return result;
    }

    CompletableFuture<PartitionResponse> appendNonIdempotentRecords(
            OwnedWritePayload payload,
            RecordAnalysisResult analysisResult,
            CompletableFuture<Void> submissionFuture) {
        CompletableFuture<PartitionResponse> result = new CompletableFuture<>();

        initialized().whenComplete((logInstance, logError) -> {
            if (logError != null) {
                payload.release();
                submissionFuture.complete(null);
                result.completeExceptionally(logError);
                return;
            }

            if (closed) {
                payload.release();
                submissionFuture.complete(null);
                result.completeExceptionally(ownershipLostException());
                return;
            }

            if (!payload.beginAppend()) {
                submissionFuture.complete(null);
                result.completeExceptionally(ownershipLostException());
                return;
            }
            ByteBuf data = payload.buffer();

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
                payload.release();
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
                    payload.release();
                }
            });
        });

        return result;
    }

    private Errors mapException(Throwable error) {
        Throwable cause = unwrapCompletionException(error);
        if (hasCause(cause, NotLeaderOrFollowerException.class)
                || hasCause(cause, LogFencedException.class)) {
            invalidate();
            return Errors.NOT_LEADER_OR_FOLLOWER;
        }
        if (hasCause(cause, NoSuchStreamException.class)) {
            invalidate();
            return Errors.UNKNOWN_TOPIC_OR_PARTITION;
        }
        return Errors.KAFKA_STORAGE_ERROR;
    }

    private PartitionResponse writeErrorResponse(Throwable error) {
        Throwable cause = unwrapCompletionException(error);
        if (hasCause(cause, NotLeaderOrFollowerException.class)
                || hasCause(cause, LogFencedException.class)) {
            invalidate();
            log.info("Partition log is no longer local owner for partition {}", topicIdPartition, cause);
            return new PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER);
        }
        if (hasCause(cause, NoSuchStreamException.class)) {
            invalidate();
            log.debug("Partition log is not provisioned yet for partition {}", topicIdPartition, cause);
            return new PartitionResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION);
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

    private static boolean hasCause(Throwable error, Class<? extends Throwable> type) {
        Throwable cause = error;
        while (cause != null) {
            if (type.isInstance(cause)) {
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

    CompletableFuture<Void> close(boolean deletePartition) {
        CompletableFuture<Log> retentionFuture;
        boolean firstClose;
        synchronized (this) {
            firstClose = !closed;
            closed = true;
            retentionFuture = inFlightRetention.getAndSet(null);
        }
        if (firstClose) {
            pendingRetention.set(null);
            releasePendingWritePayloads();
            if (retentionFuture != null) {
                retentionFuture.cancel(false);
            }
            cleanupWriteState();
            cleanupGlobalState();
            cleanup(deletePartition);
        }
        CompletableFuture<Void> closeAttempt = retryCloseLog();
        state.trackRetiredPartitionLog(this, closeAttempt);
        return logCloseDrain;
    }

    void cleanupWriteState() {
        writeSequencer.reset();
    }

    int ownedWritePayloadCount() {
        return ownedWritePayloads.size();
    }

    private void releasePendingWritePayloads() {
        ownedWritePayloads.forEach(OwnedWritePayload::cancelBeforeAppend);
    }

    void cleanupReadState() {
        closeReader();
    }

    boolean cleanupGlobalState() {
        boolean cleaned = logMetrics.remove(topicIdPartition);
        return closeReader() || cleaned;
    }

    private synchronized boolean cleanupProducerState(String zone, boolean deletePartition) {
        ProducerStateManager producerStateManager = producerStateManagers.get(zone);
        if (producerStateManager == null) {
            return false;
        }

        Optional<CompletableFuture<Void>> cleanupFuture = state.startProducerStateCleanup(() -> {
            producerStateManagers.remove(zone, producerStateManager);
            return producerStateManager.cleanup(deletePartition);
        });
        if (cleanupFuture.isEmpty()) {
            return false;
        }
        cleanupFuture.orElseThrow().whenComplete((ignored, error) -> {
            if (error != null) {
                log.warn("Failed to cleanup producer state manager for partition {} and zone {}",
                        topicIdPartition, zone, error);
            }
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
                initialized.completeExceptionally(
                        new NotLeaderOrFollowerException("Partition log already closed"));
                return;
            }

            UrsaPartitionLog activePartitionLog = state.partitionLog(topicIdPartition);
            if (activePartitionLog != null && activePartitionLog != this) {
                closed = true;
                initialized.completeExceptionally(
                        new NotLeaderOrFollowerException("Partition log already replaced"));
                state.trackRetiredPartitionLog(this, retryCloseLog());
                return;
            }

            try {
                initGlobalState(logInstance, initialized);
            } catch (Throwable initializationError) {
                closed = true;
                initialized.completeExceptionally(initializationError);
                state.trackRetiredPartitionLog(this, retryCloseLog());
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
                if (hasCause(cause, LogFencedException.class)) {
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

    private void initGlobalState(
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
            if (reader == null) {
                PartitionReader openedReader =
                        new PartitionReader(topicIdPartition, logInstance, MAX_ENTRIES_PER_FETCH);
                try {
                    logMetrics.register(topicIdPartition, logInstance);
                } catch (Throwable metricRegistrationError) {
                    logMetrics.remove(topicIdPartition);
                    openedReader.close();
                    throw metricRegistrationError;
                }
                reader = openedReader;
            }
            initialized.complete(logInstance);
        }
    }

    synchronized CompletableFuture<Void> retryCloseLog() {
        // The cursor must be gone before the log handle it reads through is closed.
        closeReader();
        if (logClosed) {
            logCloseDrain.complete(null);
            return CompletableFuture.completedFuture(null);
        }
        if (logCloseAttempt != null && !logCloseAttempt.isDone()) {
            return logCloseAttempt;
        }

        CompletableFuture<Void> trimDrain = activeTrimFuture == null
                ? CompletableFuture.completedFuture(null)
                : activeTrimFuture.handle((ignored, error) -> {
                    if (error != null) {
                        log.debug("Retention trim settled with an error while closing {}",
                                topicIdPartition, error);
                    }
                    return null;
                });
        CompletableFuture<Void> attempt = trimDrain
                .thenCompose(ignored -> logFuture
                .handle((logInstance, openError) -> {
                    if (openError != null || logInstance == null) {
                        return CompletableFuture.<Void>completedFuture(null);
                    }
                    CompletableFuture<Void> closeFuture = state.runRetiredResourceClose(logInstance::close);
                    return closeFuture == null
                            ? CompletableFuture.runAsync(() -> closeLogHandle(logInstance))
                            : closeFuture;
                })
                .thenCompose(Function.identity()));
        logCloseAttempt = attempt;
        attempt.whenComplete((ignored, error) -> {
            synchronized (UrsaPartitionLog.this) {
                if (logCloseAttempt == attempt) {
                    logCloseAttempt = null;
                    if (error == null) {
                        logClosed = true;
                        logCloseDrain.complete(null);
                    }
                }
            }
        });
        return attempt;
    }

    private static void closeLogHandle(Log logInstance) {
        try {
            logInstance.close();
        } catch (Throwable closeError) {
            throw new CompletionException(closeError);
        }
    }

    /** Drops the reader and closes its cached cursor. Idempotent; returns true for the first close. */
    private boolean closeReader() {
        PartitionReader retiredReader;
        synchronized (this) {
            retiredReader = reader;
            reader = null;
        }
        if (retiredReader == null) {
            return false;
        }
        retiredReader.close();
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

    private enum WritePayloadState {
        PENDING,
        APPENDING,
        RELEASED
    }

    private static final class PreparedWrite {
        private final OwnedWritePayload payload;
        private final RecordAnalysisResult analysisResult;
        private final List<ProducerStateManager.AppendBatch> appendBatches;
        private final boolean idempotent;
        private final PartitionResponse errorResponse;

        private PreparedWrite(
                OwnedWritePayload payload,
                RecordAnalysisResult analysisResult,
                List<ProducerStateManager.AppendBatch> appendBatches,
                boolean idempotent,
                PartitionResponse errorResponse) {
            this.payload = payload;
            this.analysisResult = analysisResult;
            this.appendBatches = appendBatches;
            this.idempotent = idempotent;
            this.errorResponse = errorResponse;
        }

    }

    private final class OwnedWritePayload {
        private final ByteBuf data;
        private final AtomicReference<WritePayloadState> payloadState =
                new AtomicReference<>(WritePayloadState.PENDING);

        private OwnedWritePayload(ByteBuf data) {
            this.data = data;
        }

        private boolean beginAppend() {
            return payloadState.compareAndSet(WritePayloadState.PENDING, WritePayloadState.APPENDING);
        }

        private ByteBuf buffer() {
            if (payloadState.get() != WritePayloadState.APPENDING) {
                throw new IllegalStateException("Write payload is not owned by an append");
            }
            return data;
        }

        private void cancelBeforeAppend() {
            if (payloadState.compareAndSet(WritePayloadState.PENDING, WritePayloadState.RELEASED)) {
                releaseBuffer();
            }
        }

        private void release() {
            WritePayloadState previous = payloadState.getAndSet(WritePayloadState.RELEASED);
            if (previous != WritePayloadState.RELEASED) {
                releaseBuffer();
            }
        }

        private void releaseBuffer() {
            try {
                data.release();
            } finally {
                ownedWritePayloads.remove(this);
            }
        }
    }

}
