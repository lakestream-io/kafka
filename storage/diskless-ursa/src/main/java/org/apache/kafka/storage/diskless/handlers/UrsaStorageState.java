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
import org.apache.kafka.storage.diskless.DisklessStorageStateOperations;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import io.lakestream.api.Log;
import io.lakestream.api.StreamCatalog;
import io.oxia.client.api.AsyncOxiaClient;

/**
 * Shared state container for Ursa storage components.
 * Manages per-partition Lakestream logs for the diskless reader and writer.
 */
public class UrsaStorageState implements DisklessStorageStateOperations {

    private static final Logger log = LoggerFactory.getLogger(UrsaStorageState.class);
    private static final long TOPIC_CONFIG_UPDATE_TIMEOUT_SECONDS = 30;
    private static final long RETIRED_LOG_CLOSE_RETRY_MS = 1_000;
    private static final long SHUTDOWN_TIMEOUT_MS = 5_000;

    private final Time time;
    private final int brokerId;
    private final UrsaStorageConfig config;
    private final BrokerTopicStats brokerTopicStats;
    private final DisklessLogMetrics logMetrics = new DisklessLogMetrics();

    private final ConcurrentHashMap<TopicIdPartition, UrsaPartitionLog> partitionLogs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UrsaPartitionLog, CompletableFuture<Void>> retiredPartitionLogs =
            new ConcurrentHashMap<>();

    private final LakestreamStorageHolder lakestreamStorageHolder;
    private final StreamCatalog catalog;
    private final Supplier<AsyncOxiaClient> oxiaClientSupplier;
    private final ScheduledExecutorService producerStateScheduler;
    private final ScheduledExecutorService retentionScheduler;
    private final ScheduledExecutorService retiredLogCloseScheduler;
    private final ScheduledFuture<?> retentionTask;
    private final Map<String, Object> logConfigDefaults;
    private final Function<String, Map<String, String>> topicConfigSupplier;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object lifecycleLock = new Object();
    private final Object shutdownLock = new Object();
    private final Object producerStateCleanupLock = new Object();
    private final CompletableFuture<Void> producerStateCleanupDrain = new CompletableFuture<>();
    private int pendingProducerStateCleanups;
    private boolean producerStateCleanupRegistrationSealed;
    private boolean shutdownInitialized;
    private volatile boolean resourcesClosed;
    private CompletableFuture<Void> shutdownDrain;
    private CompletableFuture<Void> holderCloseAttempt;

    /**
     * Creates UrsaStorageState for production use.
     */
    public UrsaStorageState(
            Time time,
            int brokerId,
            UrsaStorageConfig config,
            BrokerTopicStats brokerTopicStats) {
        this(time, brokerId, config, brokerTopicStats,
            (Map<String, Object>) null,
            (Function<String, Map<String, String>>) null);
    }

    public UrsaStorageState(
            Time time,
            int brokerId,
            UrsaStorageConfig config,
            BrokerTopicStats brokerTopicStats,
            Map<String, Object> logConfigDefaults,
            Function<String, Map<String, String>> topicConfigSupplier) {
        this.time = time;
        this.brokerId = brokerId;
        this.config = config;
        this.brokerTopicStats = brokerTopicStats;
        this.logConfigDefaults = logConfigDefaults != null ? logConfigDefaults : Collections.emptyMap();
        this.topicConfigSupplier = topicConfigSupplier;
        this.producerStateScheduler = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "producer-state-manager");
            thread.setDaemon(true);
            return thread;
        });
        this.retiredLogCloseScheduler = newDaemonScheduler("diskless-retired-log-close");

        LakestreamStorageHolder holder;
        try {
            holder = LakestreamStorageHolder.create(config);
            log.info("Initialized StreamCatalog for Kafka diskless storage");
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize StreamCatalog", e);
        }
        this.lakestreamStorageHolder = holder;
        this.catalog = holder.catalog();
        this.oxiaClientSupplier = holder::oxiaClient;
        this.retentionScheduler = newDaemonScheduler("diskless-retention");
        this.retentionTask = startRetentionChecks();
    }

    UrsaStorageState(
            Time time,
            int brokerId,
            UrsaStorageConfig config,
            BrokerTopicStats brokerTopicStats,
            StreamCatalog catalog) {
        this(time, brokerId, config, brokerTopicStats, catalog, Collections.emptyMap(), null);
    }

    UrsaStorageState(
            Time time,
            int brokerId,
            UrsaStorageConfig config,
            BrokerTopicStats brokerTopicStats,
            StreamCatalog catalog,
            Map<String, Object> logConfigDefaults,
            Function<String, Map<String, String>> topicConfigSupplier) {
        this.time = Objects.requireNonNull(time, "time must not be null");
        this.brokerId = brokerId;
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.brokerTopicStats = Objects.requireNonNull(brokerTopicStats, "brokerTopicStats must not be null");
        this.lakestreamStorageHolder = null;
        this.catalog = Objects.requireNonNull(catalog, "catalog must not be null");
        this.oxiaClientSupplier = () -> null;
        this.producerStateScheduler = newDaemonScheduler("producer-state-manager-test");
        this.retentionScheduler = newDaemonScheduler("diskless-retention-test");
        this.retiredLogCloseScheduler = newDaemonScheduler("diskless-retired-log-close-test");
        this.logConfigDefaults = logConfigDefaults != null ? logConfigDefaults : Collections.emptyMap();
        this.topicConfigSupplier = topicConfigSupplier;
        this.retentionTask = startRetentionChecks();
    }

    UrsaStorageState(
            Time time,
            int brokerId,
            UrsaStorageConfig config,
            BrokerTopicStats brokerTopicStats,
            LakestreamStorageHolder lakestreamStorageHolder,
            Map<String, Object> logConfigDefaults,
            Function<String, Map<String, String>> topicConfigSupplier) {
        this.time = Objects.requireNonNull(time, "time must not be null");
        this.brokerId = brokerId;
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.brokerTopicStats = Objects.requireNonNull(brokerTopicStats, "brokerTopicStats must not be null");
        this.lakestreamStorageHolder = Objects.requireNonNull(
                lakestreamStorageHolder, "lakestreamStorageHolder must not be null");
        this.catalog = lakestreamStorageHolder.catalog();
        this.oxiaClientSupplier = lakestreamStorageHolder::oxiaClient;
        this.producerStateScheduler = newDaemonScheduler("producer-state-manager-test");
        this.retentionScheduler = newDaemonScheduler("diskless-retention-test");
        this.retiredLogCloseScheduler = newDaemonScheduler("diskless-retired-log-close-test");
        this.logConfigDefaults = logConfigDefaults != null ? logConfigDefaults : Collections.emptyMap();
        this.topicConfigSupplier = topicConfigSupplier;
        this.retentionTask = startRetentionChecks();
    }

    public Time time() {
        return time;
    }

    public int brokerId() {
        return brokerId;
    }

    public UrsaStorageConfig config() {
        return config;
    }

    public BrokerTopicStats brokerTopicStats() {
        return brokerTopicStats;
    }

    public void applyTopicConfig(String topicName, Uuid topicId, Map<String, String> topicConfig) {
        try {
            applyTopicConfigAsync(topicName, topicId, topicConfig)
                    .get(TOPIC_CONFIG_UPDATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while updating topic config for " + topicName, e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to update topic config for " + topicName, e.getCause());
        } catch (TimeoutException e) {
            throw new RuntimeException("Timed out after " + TOPIC_CONFIG_UPDATE_TIMEOUT_SECONDS
                    + " seconds while updating topic config for " + topicName, e);
        }
    }

    CompletableFuture<Void> applyTopicConfigAsync(
            String topicName,
            Uuid topicId,
            Map<String, String> topicConfig
    ) {
        if (topicName == null || topicId == null || topicConfig == null) {
            return CompletableFuture.completedFuture(null);
        }
        Map<String, String> configSnapshot = Map.copyOf(topicConfig);
        // Kafka metadata is authoritative for broker-side retention. Catalog properties are
        // reconciled by the active controller's DisklessTopicLifecycleReconciler.
        triggerRetentionForTopic(topicName, topicId, configSnapshot);
        return CompletableFuture.completedFuture(null);
    }

    public void fenceDeletedTopic(String topicName, Uuid topicId) {
        if (topicName == null || topicId == null) {
            return;
        }
        TopicIdPartition topicIdentity = new TopicIdPartition(
                topicId, new TopicPartition(topicName, 0));
        if (lakestreamStorageHolder != null) {
            lakestreamStorageHolder.markTopicDeleted(topicIdentity);
        }
        // Publish the local lifecycle fence before doing any best-effort cleanup work. A request
        // racing this committed deletion must not be able to open a new leased Log while
        // retention callbacks are being scheduled.
        triggerRetentionForTopic(topicName, topicId, Map.of());
    }

    UrsaPartitionLog getOrCreatePartitionLog(TopicIdPartition tp) {
        synchronized (lifecycleLock) {
            if (closed.get()) {
                throw new IllegalStateException("Ursa storage state is closed");
            }
            UrsaPartitionLog partitionLog = partitionLogs.computeIfAbsent(tp,
                    ignored -> new UrsaPartitionLog(
                            tp,
                            this,
                            logMetrics,
                            openLog(tp),
                            oxiaClientSupplier,
                            config.getProducerStateSnapshotIntervalMs(),
                            config.getProducerStateSnapshotRecordThreshold(),
                            producerStateScheduler));
            // An already-failed open can complete in the constructor, before computeIfAbsent
            // publishes the value and before the init callback can evict it. Remove it after
            // publication so a subsequent request can retry.
            if (partitionLog.initializationFailed()) {
                partitionLogs.remove(tp, partitionLog);
            } else {
                try {
                    RetentionConfig retentionConfig = buildRetentionConfig(tp);
                    partitionLog.triggerInitialRetention(
                            retentionConfig.retentionMs(),
                            retentionConfig.retentionBytes());
                } catch (Throwable error) {
                    log.warn("Failed to schedule initial retention for {}", tp, error);
                }
            }
            return partitionLog;
        }
    }

    UrsaPartitionLog partitionLog(TopicIdPartition tp) {
        return partitionLogs.get(tp);
    }

    void removePartitionLog(TopicIdPartition tp, UrsaPartitionLog partitionLog) {
        if (tp == null || partitionLog == null) {
            return;
        }
        partitionLogs.remove(tp, partitionLog);
    }

    void trackRetiredPartitionLog(
            UrsaPartitionLog partitionLog,
            CompletableFuture<Void> closeAttempt) {
        Objects.requireNonNull(partitionLog, "partitionLog must not be null");
        Objects.requireNonNull(closeAttempt, "closeAttempt must not be null");
        while (true) {
            CompletableFuture<Void> current = retiredPartitionLogs.get(partitionLog);
            if (current != null && !current.isDone()) {
                return;
            }
            boolean installed = current == null
                    ? retiredPartitionLogs.putIfAbsent(partitionLog, closeAttempt) == null
                    : retiredPartitionLogs.replace(partitionLog, current, closeAttempt);
            if (installed) {
                observeRetiredPartitionLogClose(partitionLog, closeAttempt);
                return;
            }
        }
    }

    private void observeRetiredPartitionLogClose(
            UrsaPartitionLog partitionLog,
            CompletableFuture<Void> closeAttempt) {
        closeAttempt.whenComplete((ignored, error) -> {
            if (error == null) {
                retiredPartitionLogs.remove(partitionLog, closeAttempt);
                return;
            }
            if (retiredPartitionLogs.get(partitionLog) != closeAttempt) {
                return;
            }
            try {
                retiredLogCloseScheduler.schedule(
                        () -> retryRetiredPartitionLog(partitionLog, closeAttempt),
                        RETIRED_LOG_CLOSE_RETRY_MS,
                        TimeUnit.MILLISECONDS);
            } catch (RuntimeException scheduleError) {
                if (!resourcesClosed) {
                    log.warn("Failed to schedule Log close retry for {}",
                            partitionLog.topicIdPartition(), scheduleError);
                }
            }
        });
    }

    private void retryRetiredPartitionLog(
            UrsaPartitionLog partitionLog,
            CompletableFuture<Void> previousAttempt) {
        if (retiredPartitionLogs.get(partitionLog) != previousAttempt) {
            return;
        }
        CompletableFuture<Void> retry = partitionLog.retryCloseLog();
        if (retiredPartitionLogs.replace(partitionLog, previousAttempt, retry)) {
            observeRetiredPartitionLogClose(partitionLog, retry);
        }
    }

    void retryRetiredPartitionLogs() {
        retiredPartitionLogs.forEach((partitionLog, attempt) -> {
            if (attempt.isDone()) {
                retryRetiredPartitionLog(partitionLog, attempt);
            }
        });
    }

    ScheduledExecutorService retiredResourceCloseScheduler() {
        return retiredLogCloseScheduler;
    }

    CompletableFuture<Void> runRetiredResourceClose(RetiredResourceClose closeOperation) {
        try {
            return CompletableFuture.runAsync(() -> {
                try {
                    closeOperation.close();
                } catch (Throwable error) {
                    throw new java.util.concurrent.CompletionException(error);
                }
            }, retiredLogCloseScheduler);
        } catch (RuntimeException scheduleError) {
            return CompletableFuture.failedFuture(scheduleError);
        }
    }

    int retiredPartitionLogCount() {
        return retiredPartitionLogs.size();
    }

    /**
     * Best-effort cleanup for a topic-partition that is no longer hosted by this broker.
     * <p>
     * Removes in-memory state and closes any cached {@link Log} instance.
     *
     * @return true if any state was cleaned up; false if there was nothing to do.
     */
    public boolean cleanupPartition(TopicIdPartition tp) {
        return cleanupPartition(tp, false);
    }

    /**
     * Best-effort cleanup for a topic-partition that is no longer hosted by this broker.
     * <p>
     * Removes in-memory state and closes any cached {@link Log} instance. Optionally deletes any
     * persisted producer-state snapshot when the partition is permanently deleted (for example, topic deletion).
     *
     * @param deletePartition whether to delete persisted producer-state data for this partition
     * @return true if any in-memory state was cleaned up; false if there was nothing to do.
     */
    public boolean cleanupPartition(TopicIdPartition tp, boolean deletePartition) {
        if (tp == null) {
            return false;
        }
        retryRetiredPartitionLogs();
        synchronized (lifecycleLock) {
            if (closed.get()) {
                return false;
            }
            AtomicBoolean cleaned = new AtomicBoolean();
            partitionLogs.computeIfPresent(tp, (ignored, partitionLog) -> {
                // Publish the local closed state while this key is still locked in the map. A
                // concurrent computeIfAbsent can only install a new leased handle after the retired
                // handle has stopped accepting broker requests.
                partitionLog.close(deletePartition);
                cleaned.set(true);
                return null;
            });
            return cleaned.get();
        }
    }

    public boolean cleanupNonOwnedProducerStates(
            TopicIdPartition tp,
            Set<String> ownedZones,
            boolean deletePartition) {
        if (tp == null) {
            return false;
        }
        UrsaPartitionLog partitionLog = partitionLogs.get(tp);
        if (partitionLog == null) {
            return false;
        }

        return partitionLog.cleanupNonOwnedProducerStates(ownedZones, deletePartition);
    }

    public Set<TopicIdPartition> snapshotTrackedPartitions() {
        return new LinkedHashSet<>(partitionLogs.keySet());
    }

    Optional<CompletableFuture<Void>> startProducerStateCleanup(
            Supplier<CompletableFuture<Void>> cleanupStarter
    ) {
        synchronized (producerStateCleanupLock) {
            if (producerStateCleanupRegistrationSealed) {
                return Optional.empty();
            }
            pendingProducerStateCleanups++;
        }

        CompletableFuture<Void> cleanupFuture;
        try {
            cleanupFuture = cleanupStarter.get();
            if (cleanupFuture == null) {
                cleanupFuture = CompletableFuture.failedFuture(
                        new IllegalStateException("Producer state cleanup returned a null future"));
            }
        } catch (Throwable error) {
            cleanupFuture = CompletableFuture.failedFuture(error);
        }
        cleanupFuture.whenComplete((ignored, error) -> completeProducerStateCleanup());
        return Optional.of(cleanupFuture);
    }

    private void completeProducerStateCleanup() {
        boolean completeDrain;
        synchronized (producerStateCleanupLock) {
            pendingProducerStateCleanups--;
            completeDrain = producerStateCleanupRegistrationSealed && pendingProducerStateCleanups == 0;
        }
        if (completeDrain) {
            producerStateCleanupDrain.complete(null);
        }
    }

    private CompletableFuture<Void> sealProducerStateCleanups() {
        boolean completeDrain;
        synchronized (producerStateCleanupLock) {
            producerStateCleanupRegistrationSealed = true;
            completeDrain = pendingProducerStateCleanups == 0;
        }
        if (completeDrain) {
            producerStateCleanupDrain.complete(null);
        }
        return producerStateCleanupDrain;
    }

    CompletableFuture<Log> maybeApplyRetention(Log logInstance, long retentionMs, long retentionBytes) {
        return maybeApplyRetention(null, logInstance, retentionMs, retentionBytes)
                .exceptionally(error -> {
                    log.warn("Failed to apply retention to log {}", logInstance.id(), error);
                    return logInstance;
                });
    }

    CompletableFuture<Log> maybeApplyRetention(
            UrsaPartitionLog owner,
            Log logInstance,
            long retentionMs,
            long retentionBytes) {
        if (retentionMs < 0 && retentionBytes < 0) {
            return CompletableFuture.completedFuture(logInstance);
        }
        return logInstance.getLastOffset()
                .thenCompose(lastOffset -> lastOffset == null || lastOffset.offset() < 0
                        ? CompletableFuture.completedFuture(-1L)
                        : logInstance.computeRetentionTrimOffset(
                                lastOffset.offset(), retentionMs, retentionBytes))
                .thenCompose(trimOffset -> maybeTrim(owner, logInstance, trimOffset));
    }

    private CompletableFuture<Log> maybeTrim(UrsaPartitionLog owner, Log logInstance, long trimOffset) {
        if (trimOffset < 0) {
            return CompletableFuture.completedFuture(logInstance);
        }
        return logInstance.getFirstOffset()
                .thenCompose(firstOffset -> {
                    if (firstOffset == null || firstOffset.offset() < 0 || trimOffset < firstOffset.offset()) {
                        return CompletableFuture.completedFuture(logInstance);
                    }
                    if (owner != null) {
                        return owner.softTrimIfActive(logInstance, trimOffset);
                    }
                    return logInstance.softTrim(trimOffset).thenApply(ignored -> logInstance);
                });
    }

    private RetentionConfig buildRetentionConfig(TopicIdPartition tp) {
        Map<String, String> topicConfigs = null;
        if (topicConfigSupplier != null) {
            topicConfigs = topicConfigSupplier.apply(tp.topic());
        }
        return buildRetentionConfig(topicConfigs);
    }

    private RetentionConfig buildRetentionConfig(Map<String, String> topicConfigs) {
        Properties overrides = new Properties();
        if (topicConfigs != null) {
            topicConfigs.forEach(overrides::setProperty);
        }
        long retentionMs = getLongConfig(overrides, TopicConfig.RETENTION_MS_CONFIG,
                getDefaultLongConfig(TopicConfig.RETENTION_MS_CONFIG, -1L));
        long retentionBytes = getLongConfig(overrides, TopicConfig.RETENTION_BYTES_CONFIG,
                getDefaultLongConfig(TopicConfig.RETENTION_BYTES_CONFIG, -1L));
        return new RetentionConfig(retentionMs, retentionBytes);
    }

    private ScheduledFuture<?> startRetentionChecks() {
        long retentionCheckMs = getDefaultLongConfig(
                ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_CONFIG,
                ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_DEFAULT);
        if (retentionCheckMs <= 0) {
            throw new IllegalArgumentException(
                    ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_CONFIG + " must be greater than zero");
        }
        return retentionScheduler.scheduleWithFixedDelay(
                this::triggerPeriodicRetention,
                retentionCheckMs,
                retentionCheckMs,
                TimeUnit.MILLISECONDS);
    }

    void triggerPeriodicRetention() {
        if (closed.get()) {
            return;
        }
        partitionLogs.forEach((tp, partitionLog) -> {
            try {
                RetentionConfig retentionConfig = buildRetentionConfig(tp);
                partitionLog.triggerRetention(retentionConfig.retentionMs(), retentionConfig.retentionBytes());
            } catch (Throwable error) {
                log.warn("Failed to schedule retention for {}", tp, error);
            }
        });
    }

    private void triggerRetentionForTopic(
            String topicName,
            Uuid topicId,
            Map<String, String> topicConfig
    ) {
        try {
            RetentionConfig retentionConfig = buildRetentionConfig(topicConfig);
            partitionLogs.forEach((tp, partitionLog) -> {
                if (tp.topic().equals(topicName) && tp.topicId().equals(topicId)) {
                    partitionLog.triggerRetention(retentionConfig.retentionMs(), retentionConfig.retentionBytes());
                }
            });
        } catch (Throwable error) {
            log.warn("Failed to schedule updated retention for topic {} ({})", topicName, topicId, error);
        }
    }

    private static ScheduledExecutorService newDaemonScheduler(String threadName) {
        return new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, threadName);
            thread.setDaemon(true);
            return thread;
        });
    }

    private long getDefaultLongConfig(String key, long fallback) {
        Object value = logConfigDefaults.get(key);
        if (value == null) {
            return fallback;
        }
        return Long.parseLong(String.valueOf(value));
    }

    private static long getLongConfig(Properties overrides, String key, long fallback) {
        String value = overrides.getProperty(key);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return Long.parseLong(value);
    }

    private record RetentionConfig(long retentionMs, long retentionBytes) {
    }

    @Override
    public void close() throws IOException {
        close(SHUTDOWN_TIMEOUT_MS);
    }

    void close(long timeoutMs) throws IOException {
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("timeoutMs must be greater than zero");
        }
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        synchronized (shutdownLock) {
            if (resourcesClosed) {
                return;
            }
            initializeShutdown();
            awaitShutdownStage(shutdownDrain, deadlineNanos, "partition and producer-state cleanup");

            if (lakestreamStorageHolder != null) {
                if (holderCloseAttempt == null
                        || holderCloseAttempt.isCompletedExceptionally()
                        || holderCloseAttempt.isCancelled()) {
                    holderCloseAttempt = runRetiredResourceClose(lakestreamStorageHolder::close);
                }
                try {
                    awaitShutdownStage(holderCloseAttempt, deadlineNanos, "Ursa storage resources");
                } catch (IOException closeError) {
                    if (holderCloseAttempt.isDone()) {
                        holderCloseAttempt = null;
                    }
                    throw closeError;
                }
            }

            shutdownScheduler(retentionScheduler, deadlineNanos, "retention scheduler");
            shutdownScheduler(producerStateScheduler, deadlineNanos, "producer-state scheduler");
            shutdownScheduler(retiredLogCloseScheduler, deadlineNanos, "retired-resource scheduler");
            retiredPartitionLogs.clear();
            resourcesClosed = true;
        }
    }

    private void initializeShutdown() {
        if (shutdownInitialized) {
            return;
        }
        List<UrsaPartitionLog> partitionLogsToClose;
        synchronized (lifecycleLock) {
            if (closed.compareAndSet(false, true)) {
                retentionTask.cancel(false);
            }
            LinkedHashSet<UrsaPartitionLog> logsToClose = new LinkedHashSet<>(partitionLogs.values());
            logsToClose.addAll(retiredPartitionLogs.keySet());
            partitionLogsToClose = List.copyOf(logsToClose);
            partitionLogs.clear();
        }
        List<CompletableFuture<Void>> closeDrains = new java.util.ArrayList<>(partitionLogsToClose.size() + 1);
        partitionLogsToClose.forEach(partitionLog -> closeDrains.add(partitionLog.close(false)));
        retryRetiredPartitionLogs();
        closeDrains.add(sealProducerStateCleanups());
        shutdownDrain = CompletableFuture.allOf(closeDrains.toArray(new CompletableFuture<?>[0]));
        shutdownInitialized = true;
    }

    private static void awaitShutdownStage(
            CompletableFuture<Void> stage,
            long deadlineNanos,
            String description) throws IOException {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            throw new IOException("Timed out while closing " + description);
        }
        try {
            stage.get(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while closing " + description, interrupted);
        } catch (ExecutionException executionError) {
            Throwable cause = executionError.getCause();
            if (cause instanceof IOException ioException) {
                throw ioException;
            }
            throw new IOException("Failed to close " + description, cause);
        } catch (TimeoutException timeout) {
            throw new IOException("Timed out while closing " + description, timeout);
        }
    }

    private static void shutdownScheduler(
            ScheduledExecutorService scheduler,
            long deadlineNanos,
            String description) throws IOException {
        scheduler.shutdown();
        long remainingNanos = Math.max(0L, deadlineNanos - System.nanoTime());
        try {
            if (!scheduler.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)) {
                scheduler.shutdownNow();
                if (!scheduler.isTerminated()) {
                    throw new IOException("Timed out while closing " + description);
                }
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while closing " + description, e);
        }
    }

    @FunctionalInterface
    interface RetiredResourceClose {
        void close() throws Exception;
    }

    CompletableFuture<Log> openLog(TopicIdPartition tp) {
        if (lakestreamStorageHolder != null) {
            return lakestreamStorageHolder.openPartition(tp);
        }
        return LakestreamStorageHolder.openPartition(catalog, tp);
    }

}
