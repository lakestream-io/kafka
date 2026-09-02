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
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionRequest;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionResponse;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
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
import io.lakestream.api.exception.LogFencedException;
import io.lakestream.api.exception.NoSuchStreamException;
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
    private final PartitionWriter writer;
    private final AtomicBoolean initialRetentionTriggered = new AtomicBoolean();
    private final AtomicBoolean retentionWorkerRunning = new AtomicBoolean();
    private final AtomicReference<RetentionRequest> pendingRetention = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<Log>> inFlightRetention = new AtomicReference<>();
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
        this.closed = false;
        this.logFuture = logFuture;
        // The writer must exist before the log-open callback can retire it: an already-failed open
        // runs that callback inline, from this constructor.
        this.writer = new PartitionWriter(
                topicIdPartition,
                this::initialized,
                this::getOrCreateProducerStateManager,
                state.timestampTypeSupplier(topicIdPartition),
                state.time(),
                state.timer());
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
        return writer.write(records, zone).exceptionally(this::writeErrorResponse);
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

    /**
     * Registers a long-poll waiter for this partition. The reader registers one before its first
     * read so that an append landing during that read wakes the request instead of being missed.
     */
    CompletableFuture<Void> awaitAppend(long maxWaitMs) {
        return writer.awaitAppend(maxWaitMs);
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

    /** Stops accepting writes: releases every payload that has not reached storage and wakes fetch waiters. */
    void cleanupWriteState() {
        writer.close();
    }

    int ownedWritePayloadCount() {
        return writer.ownedWritePayloadCount();
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
                    // The writer stays open on purpose: this partition log was never leased, so a
                    // request that still reaches it must report the open failure rather than a lost
                    // leadership. Every in-flight write drains through the failed init future below.
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
                cleanupWriteState();
                initialized.completeExceptionally(
                        new NotLeaderOrFollowerException("Partition log already replaced"));
                state.trackRetiredPartitionLog(this, retryCloseLog());
                return;
            }

            try {
                initGlobalState(logInstance, initialized);
            } catch (Throwable initializationError) {
                closed = true;
                cleanupWriteState();
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

}
