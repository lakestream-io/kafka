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
import org.apache.kafka.storage.diskless.OxiaServiceUrl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import io.lakestream.api.EntryIndex;
import io.lakestream.api.Log;
import io.lakestream.api.LogCursor;
import io.lakestream.api.LogEntry;
import io.lakestream.api.LogEntryHeader;
import io.lakestream.api.LogId;
import io.lakestream.api.LogOffset;
import io.lakestream.api.LogStorage;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamCatalogLoader;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.exception.NoSuchStreamException;
import io.netty.buffer.ByteBuf;
import io.oxia.client.api.AsyncOxiaClient;

/**
 * Holds a {@link StreamCatalog} instance for Kafka diskless storage.
 */
final class LakestreamStorageHolder implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(LakestreamStorageHolder.class);
    private static final long OXIA_CONNECT_TIMEOUT_SECONDS = 10;
    private static final long LIFECYCLE_RETRY_MS = 1_000;
    private static final long CLOSE_DRAIN_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
    private static final String OXIA_STORAGE_URL_PROP = "oxiaStorageUrl";

    private final StreamCatalog catalog;
    private final AsyncOxiaClient producerStateOxiaClient;
    private final Object streamLifecycleLock = new Object();
    private final Map<StreamIdentifier, StreamOpenState> streamOpenStates = new HashMap<>();
    private final ScheduledExecutorService lifecycleRetryScheduler =
            Executors.newSingleThreadScheduledExecutor(task -> {
                Thread thread = new Thread(task, "diskless-log-lifecycle-retry");
                thread.setDaemon(true);
                return thread;
            });
    private final long lifecycleRetryMs;
    private final long closeDrainTimeoutMs;
    private boolean closing;
    private boolean closed;
    private boolean retrySchedulerClosed;
    private boolean catalogClosed;
    private boolean producerStateOxiaClientClosed;

    LakestreamStorageHolder(
            StreamCatalog catalog,
            AsyncOxiaClient producerStateOxiaClient) {
        this(catalog, producerStateOxiaClient, LIFECYCLE_RETRY_MS, CLOSE_DRAIN_TIMEOUT_MS);
    }

    LakestreamStorageHolder(
            StreamCatalog catalog,
            AsyncOxiaClient producerStateOxiaClient,
            long lifecycleRetryMs,
            long closeDrainTimeoutMs) {
        if (lifecycleRetryMs <= 0) {
            throw new IllegalArgumentException("lifecycleRetryMs must be positive");
        }
        if (closeDrainTimeoutMs <= 0) {
            throw new IllegalArgumentException("closeDrainTimeoutMs must be positive");
        }
        this.catalog = catalog;
        this.producerStateOxiaClient = producerStateOxiaClient;
        this.lifecycleRetryMs = lifecycleRetryMs;
        this.closeDrainTimeoutMs = closeDrainTimeoutMs;
    }

    StreamCatalog catalog() {
        return catalog;
    }

    AsyncOxiaClient oxiaClient() {
        return producerStateOxiaClient;
    }

    CompletableFuture<Log> openPartition(TopicIdPartition tp) {
        StreamIdentifier identifier = streamIdentifier(tp);
        StreamOpenState openState = beginOpen(identifier);
        if (openState == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Kafka topic incarnation is already deleted: " + tp));
        }
        CompletableFuture<Log> openFuture;
        try {
            openFuture = openPartition(catalog, tp);
        } catch (Throwable error) {
            completeFailedOpen(identifier, openState);
            return CompletableFuture.failedFuture(error);
        }
        return openFuture.handle((openedLog, error) -> {
            if (error != null) {
                completeFailedOpen(identifier, openState);
                return CompletableFuture.<Log>failedFuture(error);
            }
            return completeSuccessfulOpen(identifier, openState, tp, openedLog);
        }).thenCompose(future -> future);
    }

    void markTopicDeleted(TopicIdPartition tp) {
        StreamIdentifier identifier = streamIdentifier(tp);
        StreamOpenState state;
        synchronized (streamLifecycleLock) {
            if (closing || closed) {
                return;
            }
            state = streamOpenStates.computeIfAbsent(identifier, ignored -> new StreamOpenState());
            state.deleted = true;
        }
        maybeReleaseDeletionFence(identifier, state);
    }

    static CompletableFuture<Log> openPartition(StreamCatalog catalog, TopicIdPartition tp) {
        StreamIdentifier identifier = streamIdentifier(tp);
        return catalog.loadStream(identifier)
                .thenCompose(metadata -> metadata.layout().logIds())
                .thenCompose(logIds -> openLayoutLog(catalog, identifier, tp, logIds));
    }

    private static CompletableFuture<Log> openLayoutLog(
            StreamCatalog catalog,
            StreamIdentifier identifier,
            TopicIdPartition tp,
            List<LogId> logIds) {
        if (tp.partition() < 0 || tp.partition() >= logIds.size()) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                "Partition " + tp.partition() + " is not present in the committed layout for "
                    + identifier.fullName() + " (log count " + logIds.size() + ")"));
        }
        return catalog.openLog(identifier, logIds.get(tp.partition()));
    }

    private StreamOpenState beginOpen(StreamIdentifier identifier) {
        synchronized (streamLifecycleLock) {
            if (closing || closed) {
                return null;
            }
            StreamOpenState state = streamOpenStates.computeIfAbsent(
                    identifier, ignored -> new StreamOpenState());
            if (state.deleted) {
                return null;
            }
            state.inFlightOpens++;
            return state;
        }
    }

    private void completeFailedOpen(
            StreamIdentifier identifier,
            StreamOpenState state) {
        boolean checkDeletion;
        synchronized (streamLifecycleLock) {
            state.inFlightOpens--;
            checkDeletion = handleDrainedState(identifier, state);
            streamLifecycleLock.notifyAll();
        }
        if (checkDeletion) {
            maybeReleaseDeletionFence(identifier, state);
        }
    }

    private CompletableFuture<Log> completeSuccessfulOpen(
            StreamIdentifier identifier,
            StreamOpenState state,
            TopicIdPartition tp,
            Log openedLog) {
        if (openedLog == null) {
            completeFailedOpen(identifier, state);
            return CompletableFuture.failedFuture(
                    new IllegalStateException("StreamCatalog.openLog returned null for " + tp));
        }
        TrackedLog trackedLog = new TrackedLog(
                openedLog, closedLog -> completeHandleClose(identifier, state, closedLog));
        boolean rejectOpen;
        synchronized (streamLifecycleLock) {
            state.inFlightOpens--;
            state.activeHandles.add(trackedLog);
            rejectOpen = state.deleted || closing || closed;
            streamLifecycleLock.notifyAll();
        }
        if (!rejectOpen) {
            return CompletableFuture.completedFuture(trackedLog);
        }
        IllegalStateException failure = new IllegalStateException(
                "Kafka topic incarnation was deleted while opening its log: " + tp);
        closeRejectedLog(trackedLog, failure);
        return CompletableFuture.failedFuture(failure);
    }

    private void completeHandleClose(
            StreamIdentifier identifier,
            StreamOpenState state,
            TrackedLog closedLog) {
        boolean checkDeletion;
        synchronized (streamLifecycleLock) {
            state.activeHandles.remove(closedLog);
            checkDeletion = handleDrainedState(identifier, state);
            streamLifecycleLock.notifyAll();
        }
        if (checkDeletion) {
            maybeReleaseDeletionFence(identifier, state);
        }
    }

    private boolean handleDrainedState(StreamIdentifier identifier, StreamOpenState state) {
        if (!state.isLogDrained()) {
            return false;
        }
        if (!state.deleted) {
            streamOpenStates.remove(identifier, state);
            return false;
        }
        return !closing && !closed;
    }

    private void closeRejectedLog(TrackedLog trackedLog, Throwable failure) {
        try {
            trackedLog.close();
        } catch (Throwable closeError) {
            if (failure != closeError) {
                failure.addSuppressed(closeError);
            }
            scheduleRetiredLogClose(trackedLog);
        }
    }

    private void scheduleRetiredLogClose(TrackedLog trackedLog) {
        if (trackedLog.isClosed()) {
            return;
        }
        synchronized (streamLifecycleLock) {
            if (closed || retrySchedulerClosed) {
                return;
            }
        }
        try {
            lifecycleRetryScheduler.schedule(
                    () -> retryRetiredLogClose(trackedLog),
                    lifecycleRetryMs,
                    TimeUnit.MILLISECONDS);
        } catch (RuntimeException scheduleError) {
            synchronized (streamLifecycleLock) {
                if (!closed && !retrySchedulerClosed) {
                    log.warn("Failed to schedule close retry for rejected Log {}",
                            trackedLog, scheduleError);
                }
            }
        }
    }

    private void retryRetiredLogClose(TrackedLog trackedLog) {
        try {
            trackedLog.close();
        } catch (Throwable closeError) {
            log.warn("Failed to close rejected Log {}, retrying", trackedLog, closeError);
            scheduleRetiredLogClose(trackedLog);
        }
    }

    private void maybeReleaseDeletionFence(StreamIdentifier identifier, StreamOpenState state) {
        synchronized (streamLifecycleLock) {
            if (!shouldStartDeletionCheck(identifier, state)) {
                return;
            }
            state.deletionCheckInFlight = true;
        }

        CompletableFuture<?> loadFuture;
        try {
            loadFuture = catalog.loadStream(identifier);
            if (loadFuture == null) {
                loadFuture = CompletableFuture.failedFuture(
                        new IllegalStateException("StreamCatalog.loadStream returned null for "
                                + identifier.fullName()));
            }
        } catch (Throwable error) {
            loadFuture = CompletableFuture.failedFuture(error);
        }
        boolean cancelForClose;
        synchronized (streamLifecycleLock) {
            if (streamOpenStates.get(identifier) != state || !state.deletionCheckInFlight) {
                return;
            }
            state.deletionCheckSource = loadFuture;
            cancelForClose = closing || closed;
        }
        loadFuture.whenComplete((ignored, error) -> completeDeletionCheck(identifier, state, error));
        if (cancelForClose) {
            loadFuture.cancel(false);
        }
    }

    private void completeDeletionCheck(
            StreamIdentifier identifier,
            StreamOpenState state,
            Throwable error) {
        boolean retry;
        synchronized (streamLifecycleLock) {
            if (streamOpenStates.get(identifier) != state) {
                return;
            }
            state.deletionCheckInFlight = false;
            state.deletionCheckSource = null;
            streamLifecycleLock.notifyAll();
            if (closing || closed || !state.deleted || !state.isLogDrained()) {
                return;
            }
            Throwable failure = unwrapCompletionException(error);
            if (failure instanceof NoSuchStreamException) {
                streamOpenStates.remove(identifier, state);
                return;
            }
            retry = true;
            if (failure != null) {
                log.warn("Failed to verify deletion of {}, retrying", identifier.fullName(), failure);
            }
        }
        if (retry) {
            scheduleDeletionCheck(identifier, state);
        }
    }

    private void scheduleDeletionCheck(StreamIdentifier identifier, StreamOpenState state) {
        synchronized (streamLifecycleLock) {
            if (!shouldStartDeletionCheck(identifier, state)) {
                return;
            }
            state.deletionCheckScheduled = true;
        }
        try {
            lifecycleRetryScheduler.schedule(
                    () -> runScheduledDeletionCheck(identifier, state),
                    lifecycleRetryMs,
                    TimeUnit.MILLISECONDS);
        } catch (RuntimeException scheduleError) {
            synchronized (streamLifecycleLock) {
                state.deletionCheckScheduled = false;
                streamLifecycleLock.notifyAll();
                if (!closing && !closed && streamOpenStates.get(identifier) == state) {
                    log.warn("Failed to schedule deletion check for {}",
                            identifier.fullName(), scheduleError);
                }
            }
        }
    }

    private boolean shouldStartDeletionCheck(StreamIdentifier identifier, StreamOpenState state) {
        if (closing || closed) {
            return false;
        }
        if (streamOpenStates.get(identifier) != state || !state.deleted) {
            return false;
        }
        return state.isLogDrained()
                && !state.deletionCheckInFlight
                && !state.deletionCheckScheduled;
    }

    private void runScheduledDeletionCheck(StreamIdentifier identifier, StreamOpenState state) {
        synchronized (streamLifecycleLock) {
            if (streamOpenStates.get(identifier) != state) {
                return;
            }
            state.deletionCheckScheduled = false;
            streamLifecycleLock.notifyAll();
        }
        maybeReleaseDeletionFence(identifier, state);
    }

    private static Throwable unwrapCompletionException(Throwable error) {
        Throwable failure = error;
        while ((failure instanceof CompletionException || failure instanceof ExecutionException)
                && failure.getCause() != null) {
            failure = failure.getCause();
        }
        return failure;
    }

    int deletionFenceCount() {
        synchronized (streamLifecycleLock) {
            return (int) streamOpenStates.values().stream().filter(state -> state.deleted).count();
        }
    }

    boolean isClosing() {
        synchronized (streamLifecycleLock) {
            return closing && !closed;
        }
    }

    static LakestreamStorageHolder create(UrsaStorageConfig config) throws Exception {
        StreamCatalog catalog = null;
        AsyncOxiaClient producerStateOxiaClient = null;
        CompletableFuture<AsyncOxiaClient> producerStateOxiaClientFuture = null;
        try {
            String oxiaUrl = config.getCatalogOxiaServiceUrl();
            Properties properties = buildStorageProperties(config);

            catalog = StreamCatalogLoader.open(oxiaUrl, properties);

            producerStateOxiaClientFuture = new OxiaServiceUrl(config.getUrsaOxiaServiceUrl()).client();
            producerStateOxiaClient = producerStateOxiaClientFuture.get(
                    OXIA_CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            return new LakestreamStorageHolder(catalog, producerStateOxiaClient);
        } catch (Exception e) {
            try {
                if (catalog != null) {
                    catalog.close();
                }
            } catch (Exception ignored) {
            }
            closeOxiaClientAfterFailedCreate(producerStateOxiaClient, producerStateOxiaClientFuture, e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw e;
        }
    }

    static void closeOxiaClientAfterFailedCreate(
            AsyncOxiaClient oxiaClient,
            CompletableFuture<AsyncOxiaClient> oxiaClientFuture,
            Exception createFailure) {
        if (oxiaClient != null) {
            try {
                oxiaClient.close();
            } catch (Exception closeFailure) {
                createFailure.addSuppressed(closeFailure);
            }
        } else if (oxiaClientFuture != null) {
            oxiaClientFuture.thenAccept(client -> {
                try {
                    client.close();
                } catch (Exception ignored) {
                }
            });
        }
    }

    static Properties buildStorageProperties(UrsaStorageConfig config) {
        Properties properties = new Properties();
        String normalizedBackendType = normalizeBackendType(config.getBackendType());
        properties.setProperty("backendStorageType", normalizedBackendType);
        properties.setProperty("storagePath", config.getStoragePath());
        properties.setProperty(OXIA_STORAGE_URL_PROP, config.getUrsaOxiaServiceUrl());
        properties.setProperty("writeBufferFlushIntervalMs", String.valueOf(config.getWriteBufferFlushIntervalMs()));
        properties.setProperty("writeBufferSize", String.valueOf(config.getWriteBufferSize()));
        properties.setProperty("writeBufferFlushSize", String.valueOf(config.getWriteBufferFlushSize()));

        if (isRemoteBackend(normalizedBackendType)) {
            setIfNotEmpty(config.getS3Endpoint(), v -> properties.setProperty("cloudStorageEndpoint", v));
            setIfNotEmpty(config.getS3Bucket(), v -> properties.setProperty("bucket", v));
            setIfNotEmpty(config.getStoragePath(), v -> properties.setProperty("prefix", v));
            setIfNotEmpty(config.getS3Region(), v -> properties.setProperty("region", v));
        }

        if (isS3Backend(normalizedBackendType)) {
            setIfNotEmpty(config.getS3AccessKey(), v -> properties.setProperty("s3AccessKeyId", v));
            setIfNotEmpty(config.getS3SecretKey(), v -> properties.setProperty("s3SecretAccessKey", v));
            setIfNotEmpty(config.getS3SessionToken(), v -> properties.setProperty("s3SessionToken", v));
            if (config.getS3PathStyleAccess() != null) {
                properties.setProperty("s3PathStyleAccess", String.valueOf(config.getS3PathStyleAccess()));
            }
            // Deprecated fields, keep for compatibility with older configs.
            setIfNotEmpty(config.getS3Bucket(), v -> properties.setProperty("s3Bucket", v));
            setIfNotEmpty(config.getStoragePath(), v -> properties.setProperty("s3Prefix", v));
            setIfNotEmpty(config.getS3Region(), v -> properties.setProperty("s3Region", v));
        }

        setIfNotEmpty(config.getCompactionBucket(), v -> properties.setProperty("compactionBucket", v));
        setIfNotEmpty(config.getCompactionPrefix(), v -> properties.setProperty("compactionPrefix", v));
        return properties;
    }

    private static boolean isRemoteBackend(String normalizedBackendType) {
        return !"LOCAL".equals(normalizedBackendType);
    }

    private static boolean isS3Backend(String normalizedBackendType) {
        return "S3".equals(normalizedBackendType);
    }

    private static String normalizeBackendType(String backendType) {
        String normalizedBackendType = backendType.toUpperCase(Locale.ROOT);
        if ("AZURE_BLOB".equals(normalizedBackendType) || "AZUREBLOB".equals(normalizedBackendType)) {
            return "AZUREBLOB";
        }
        return normalizedBackendType;
    }

    private static void setIfNotEmpty(String value, Consumer<String> setter) {
        if (value != null && !value.isEmpty()) {
            setter.accept(value);
        }
    }

    static StreamIdentifier streamIdentifier(TopicIdPartition tp) {
        return StreamIdentifier.of(
                KafkaStreamIdentity.NAMESPACE,
                KafkaStreamIdentity.streamName(tp.topic(), tp.topicId()));
    }

    @Override
    public synchronized void close() throws IOException {
        synchronized (streamLifecycleLock) {
            if (closed) {
                return;
            }
            closing = true;
            streamOpenStates.values().forEach(state -> state.deleted = true);
        }
        cancelDeletionChecks();
        drainOpenLogs();
        closeRetryScheduler();

        List<Exception> failures = new ArrayList<>();
        if (!catalogClosed) {
            try {
                if (catalog != null) {
                    catalog.close();
                }
                catalogClosed = true;
            } catch (Exception e) {
                failures.add(e);
            }
        }
        if (!producerStateOxiaClientClosed) {
            try {
                if (producerStateOxiaClient != null) {
                    producerStateOxiaClient.close();
                }
                producerStateOxiaClientClosed = true;
            } catch (Exception e) {
                failures.add(e);
            }
        }

        if (failures.isEmpty()) {
            synchronized (streamLifecycleLock) {
                closed = true;
                streamOpenStates.clear();
                streamLifecycleLock.notifyAll();
            }
            return;
        }
        IOException failure = new IOException(failures.get(0));
        failures.stream().skip(1).forEach(failure::addSuppressed);
        throw failure;
    }

    private void cancelDeletionChecks() {
        List<CompletableFuture<?>> deletionChecks = new ArrayList<>();
        synchronized (streamLifecycleLock) {
            streamOpenStates.values().forEach(state -> {
                if (state.deletionCheckSource != null) {
                    deletionChecks.add(state.deletionCheckSource);
                }
            });
        }
        deletionChecks.forEach(check -> check.cancel(false));
    }

    private void drainOpenLogs() throws IOException {
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(closeDrainTimeoutMs);
        Exception lastCloseFailure = null;
        while (true) {
            List<TrackedLog> handlesToClose;
            synchronized (streamLifecycleLock) {
                if (isLifecycleDrained()) {
                    return;
                }
                handlesToClose = streamOpenStates.values().stream()
                        .flatMap(state -> state.activeHandles.stream())
                        .toList();
            }
            for (TrackedLog trackedLog : handlesToClose) {
                try {
                    trackedLog.close();
                } catch (Exception closeError) {
                    lastCloseFailure = closeError;
                }
            }
            synchronized (streamLifecycleLock) {
                if (isLifecycleDrained()) {
                    return;
                }
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    throw closeDrainFailure(lastCloseFailure);
                }
                long waitMillis = Math.max(1, Math.min(
                        lifecycleRetryMs,
                        TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
                try {
                    streamLifecycleLock.wait(waitMillis);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while draining Lakestream Log handles", error);
                }
            }
        }
    }

    private boolean isLifecycleDrained() {
        return streamOpenStates.values().stream().allMatch(state ->
                state.inFlightOpens == 0
                        && state.activeHandles.isEmpty()
                        && !state.deletionCheckInFlight);
    }

    private IOException closeDrainFailure(Exception lastCloseFailure) {
        int inFlightOpens = streamOpenStates.values().stream()
                .mapToInt(state -> state.inFlightOpens)
                .sum();
        int activeHandles = streamOpenStates.values().stream()
                .mapToInt(state -> state.activeHandles.size())
                .sum();
        long deletionChecks = streamOpenStates.values().stream()
                .filter(state -> state.deletionCheckInFlight)
                .count();
        String message = "Timed out after " + closeDrainTimeoutMs
                + " ms draining Lakestream lifecycle operations: "
                + inFlightOpens + " open(s), "
                + activeHandles + " Log handle(s), "
                + deletionChecks + " deletion check(s) remain";
        return lastCloseFailure == null
                ? new IOException(message)
                : new IOException(message, lastCloseFailure);
    }

    private void closeRetryScheduler() throws IOException {
        if (retrySchedulerClosed) {
            return;
        }
        lifecycleRetryScheduler.shutdownNow();
        try {
            if (!lifecycleRetryScheduler.awaitTermination(closeDrainTimeoutMs, TimeUnit.MILLISECONDS)) {
                throw new IOException("Timed out waiting for Lakestream lifecycle retry scheduler to stop");
            }
            retrySchedulerClosed = true;
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while stopping Lakestream lifecycle retry scheduler", error);
        }
    }

    private static final class StreamOpenState {
        private final Set<TrackedLog> activeHandles =
                Collections.newSetFromMap(new IdentityHashMap<>());
        private int inFlightOpens;
        private boolean deleted;
        private boolean deletionCheckInFlight;
        private boolean deletionCheckScheduled;
        private CompletableFuture<?> deletionCheckSource;

        private boolean isLogDrained() {
            return inFlightOpens == 0 && activeHandles.isEmpty();
        }
    }

    private static final class TrackedLog implements Log {
        private final Log delegate;
        private final Consumer<TrackedLog> onClosed;
        private volatile boolean closed;

        private TrackedLog(Log delegate, Consumer<TrackedLog> onClosed) {
            this.delegate = delegate;
            this.onClosed = onClosed;
        }

        private boolean isClosed() {
            return closed;
        }

        @Override
        public LogId id() {
            return delegate.id();
        }

        @Override
        public CompletableFuture<LogEntryHeader> append(int numberOfRecords, ByteBuf data) {
            return delegate.append(numberOfRecords, data);
        }

        @Override
        public CompletableFuture<List<LogEntry>> readEntries(
                long startOffset,
                int maxMessageCount,
                long maxSizeBytes) {
            return delegate.readEntries(startOffset, maxMessageCount, maxSizeBytes);
        }

        @Override
        public CompletableFuture<List<LogEntry>> readEntries(
                long startOffset,
                int maxMessageCount,
                long maxSizeBytes,
                boolean includeTrimmed) {
            return delegate.readEntries(startOffset, maxMessageCount, maxSizeBytes, includeTrimmed);
        }

        @Override
        public CompletableFuture<LogEntry> readEntry(long offset) {
            return delegate.readEntry(offset);
        }

        @Override
        public CompletableFuture<LogEntryHeader> getEntryMetadata(long offset) {
            return delegate.getEntryMetadata(offset);
        }

        @Override
        public CompletableFuture<EntryIndex> getEntryIndex(long offset) {
            return delegate.getEntryIndex(offset);
        }

        @Override
        public CompletableFuture<List<EntryIndex>> readIndexRange(long startOffset, long endOffset) {
            return delegate.readIndexRange(startOffset, endOffset);
        }

        @Override
        public CompletableFuture<List<LogEntryHeader>> getEntryMetadataRange(long startOffset, long endOffset) {
            return delegate.getEntryMetadataRange(startOffset, endOffset);
        }

        @Override
        public CompletableFuture<LogOffset> getFirstOffset() {
            return delegate.getFirstOffset();
        }

        @Override
        public CompletableFuture<LogOffset> getFirstOffset(boolean includeTrimmed) {
            return delegate.getFirstOffset(includeTrimmed);
        }

        @Override
        public CompletableFuture<LogOffset> getLastOffset() {
            return delegate.getLastOffset();
        }

        @Override
        public CompletableFuture<Long> softTrim(long offsetIncluded) {
            return delegate.softTrim(offsetIncluded);
        }

        @Override
        public LogStorage logStorage() {
            return delegate.logStorage();
        }

        @Override
        public void cacheIndex(EntryIndex index) {
            delegate.cacheIndex(index);
        }

        @Override
        public void invalidateCache() {
            delegate.invalidateCache();
        }

        @Override
        public void invalidateCache(long offset) {
            delegate.invalidateCache(offset);
        }

        @Override
        public long getMessageCount(long startOffset, long endOffset) {
            return delegate.getMessageCount(startOffset, endOffset);
        }

        @Override
        public void fence() {
            delegate.fence();
        }

        @Override
        public CompletableFuture<LogCursor> openCursor(String name, long initialOffset) {
            return delegate.openCursor(name, initialOffset);
        }

        @Override
        public CompletableFuture<LogCursor> openEphemeralCursor(String name, long initialOffset) {
            return delegate.openEphemeralCursor(name, initialOffset);
        }

        @Override
        public CompletableFuture<LogCursor> loadCursor(String name) {
            return delegate.loadCursor(name);
        }

        @Override
        public CompletableFuture<List<LogCursor>> loadAllCursors() {
            return delegate.loadAllCursors();
        }

        @Override
        public CompletableFuture<Void> deleteCursor(String name) {
            return delegate.deleteCursor(name);
        }

        @Override
        public CompletableFuture<Long> computeRetentionTrimOffset(
                long maxOffset,
                long retentionMillis,
                long retentionSizeBytes) {
            return delegate.computeRetentionTrimOffset(maxOffset, retentionMillis, retentionSizeBytes);
        }

        @Override
        public CompletableFuture<Long> binarySearchOffset(
                long min,
                long max,
                Predicate<LogEntryHeader> predicate) {
            return delegate.binarySearchOffset(min, max, predicate);
        }

        @Override
        public synchronized void close() throws Exception {
            if (closed) {
                return;
            }
            delegate.close();
            closed = true;
            onClosed.accept(this);
        }
    }
}
