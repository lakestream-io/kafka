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
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.storage.diskless.DisklessStorageStateOperations;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
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
    private static final long SHUTDOWN_TIMEOUT_MS = 5_000;

    private final Time time;
    private final int brokerId;
    private final UrsaStorageConfig config;
    private final BrokerTopicStats brokerTopicStats;
    private final DisklessLogMetrics logMetrics = new DisklessLogMetrics();

    private final ConcurrentHashMap<TopicIdPartition, UrsaPartitionLog> partitionLogs = new ConcurrentHashMap<>();

    private final LakestreamStorageHolder lakestreamStorageHolder;
    private final StreamCatalog catalog;
    private final Supplier<AsyncOxiaClient> oxiaClientSupplier;
    private final ScheduledExecutorService producerStateScheduler;
    /** One timer for every periodic and delayed task: retention checks and long-poll timeouts. */
    private final ScheduledExecutorService disklessTimer;
    private final ScheduledFuture<?> retentionTask;
    private final Map<String, Object> logConfigDefaults;
    private final Function<String, Map<String, String>> topicConfigSupplier;
    private final Function<String, OptionalInt> partitionCountSupplier;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object lifecycleLock = new Object();
    private final Object shutdownLock = new Object();
    private final Object producerStateCleanupLock = new Object();
    private final CompletableFuture<Void> producerStateCleanupDrain = new CompletableFuture<>();
    private int pendingProducerStateCleanups;
    private boolean producerStateCleanupRegistrationSealed;
    private boolean resourcesClosed;

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
            (Function<String, Map<String, String>>) null,
            topic -> OptionalInt.empty());
    }

    public UrsaStorageState(
            Time time,
            int brokerId,
            UrsaStorageConfig config,
            BrokerTopicStats brokerTopicStats,
            Map<String, Object> logConfigDefaults,
            Function<String, Map<String, String>> topicConfigSupplier,
            Function<String, OptionalInt> partitionCountSupplier) {
        this.time = time;
        this.brokerId = brokerId;
        this.config = config;
        this.brokerTopicStats = brokerTopicStats;
        this.logConfigDefaults = logConfigDefaults != null ? logConfigDefaults : Collections.emptyMap();
        this.topicConfigSupplier = topicConfigSupplier;
        this.partitionCountSupplier = partitionCountSupplier != null
                ? partitionCountSupplier
                : topic -> OptionalInt.empty();
        this.producerStateScheduler = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "producer-state-manager");
            thread.setDaemon(true);
            return thread;
        });
        this.disklessTimer = newDaemonScheduler("diskless-timer");

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
        this.disklessTimer = newDaemonScheduler("diskless-timer-test");
        this.logConfigDefaults = logConfigDefaults != null ? logConfigDefaults : Collections.emptyMap();
        this.topicConfigSupplier = topicConfigSupplier;
        this.partitionCountSupplier = topic -> OptionalInt.empty();
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
        this.disklessTimer = newDaemonScheduler("diskless-timer-test");
        this.logConfigDefaults = logConfigDefaults != null ? logConfigDefaults : Collections.emptyMap();
        this.topicConfigSupplier = topicConfigSupplier;
        this.partitionCountSupplier = topic -> OptionalInt.empty();
        this.retentionTask = startRetentionChecks();
    }

    public Time time() {
        return time;
    }

    public int brokerId() {
        return brokerId;
    }

    /** The shared timer behind retention checks and long-poll timeouts. */
    ScheduledExecutorService timer() {
        return disklessTimer;
    }

    /**
     * The effective {@code message.timestamp.type} of a partition, resolved on every call so that a
     * topic config update takes effect on the next produce request.
     */
    Supplier<TimestampType> timestampTypeSupplier(TopicIdPartition tp) {
        return () -> timestampType(tp.topic());
    }

    private TimestampType timestampType(String topic) {
        String configured = null;
        if (topicConfigSupplier != null) {
            Map<String, String> topicConfig = topicConfigSupplier.apply(topic);
            if (topicConfig != null) {
                configured = topicConfig.get(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG);
            }
        }
        if (configured == null || configured.isBlank()) {
            configured = defaultTimestampType();
        }
        try {
            return TimestampType.forName(configured);
        } catch (RuntimeException unknownType) {
            log.warn("Unknown message timestamp type {} for topic {}, using {}",
                    configured, topic, TimestampType.CREATE_TIME.name, unknownType);
            return TimestampType.CREATE_TIME;
        }
    }

    /** The broker default, which the log config map may carry under either the server or the topic name. */
    private String defaultTimestampType() {
        Object serverDefault = logConfigDefaults.get(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_CONFIG);
        if (serverDefault == null) {
            serverDefault = logConfigDefaults.get(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG);
        }
        return serverDefault == null ? TimestampType.CREATE_TIME.name : String.valueOf(serverDefault);
    }

    public UrsaStorageConfig config() {
        return config;
    }

    public BrokerTopicStats brokerTopicStats() {
        return brokerTopicStats;
    }

    /** Current partition count of the topic as seen by the broker metadata cache, when known. */
    public Function<String, OptionalInt> partitionCountSupplier() {
        return partitionCountSupplier;
    }

    public void applyTopicConfig(String topicName, Uuid topicId, Map<String, String> topicConfig) {
        if (topicName == null || topicId == null || topicConfig == null) {
            return;
        }
        // Kafka metadata is authoritative for broker-side retention. Catalog properties are
        // reconciled by the active controller's DisklessTopicLifecycleReconciler.
        triggerRetentionForTopic(topicName, topicId, Map.copyOf(topicConfig));
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

    /**
     * The partition log for {@code tp}, opening one if this broker does not hold it yet. The common
     * case - a partition that is already open - answers from the map without taking the lifecycle
     * lock, so a produce or fetch never queues behind an open or a close.
     */
    UrsaPartitionLog getOrCreatePartitionLog(TopicIdPartition tp) {
        UrsaPartitionLog existing = partitionLogs.get(tp);
        if (existing != null && !existing.initializationFailed()) {
            return existing;
        }
        synchronized (lifecycleLock) {
            if (closed.get()) {
                throw new IllegalStateException("Ursa storage state is closed");
            }
            UrsaPartitionLog partitionLog = partitionLogs.compute(tp, (key, current) ->
                    current != null && !current.initializationFailed() ? current : newPartitionLog(key));
            // An already-failed open completes in the constructor, before compute publishes the
            // value and before the init callback can evict it. Remove it after publication so a
            // subsequent request can retry.
            if (partitionLog.initializationFailed()) {
                partitionLogs.remove(tp, partitionLog);
            } else {
                try {
                    RetentionConfig retentionConfig = buildRetentionConfig(tp);
                    partitionLog.retention().request(
                            retentionConfig.retentionMs(),
                            retentionConfig.retentionBytes());
                } catch (Throwable error) {
                    log.warn("Failed to schedule initial retention for {}", tp, error);
                }
            }
            return partitionLog;
        }
    }

    private UrsaPartitionLog newPartitionLog(TopicIdPartition tp) {
        return new UrsaPartitionLog(
                tp,
                this,
                logMetrics,
                openLog(tp),
                oxiaClientSupplier,
                config.getProducerStateSnapshotIntervalMs(),
                config.getProducerStateSnapshotRecordThreshold(),
                producerStateScheduler);
    }

    /** Test-only hook: runs {@code task} while holding the lock that guards partition lifecycle. */
    void withLifecycleLock(Runnable task) {
        synchronized (lifecycleLock) {
            task.run();
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
        synchronized (lifecycleLock) {
            if (closed.get()) {
                return false;
            }
            AtomicBoolean cleaned = new AtomicBoolean();
            partitionLogs.computeIfPresent(tp, (ignored, partitionLog) -> {
                // Publish the local closed state while this key is still locked in the map. A
                // concurrent open can only install a new leased handle after the retired handle has
                // stopped accepting broker requests.
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
        return disklessTimer.scheduleWithFixedDelay(
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
                partitionLog.retention().request(
                        retentionConfig.retentionMs(), retentionConfig.retentionBytes());
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
                    partitionLog.retention().request(
                            retentionConfig.retentionMs(), retentionConfig.retentionBytes());
                }
            });
        } catch (Throwable error) {
            log.warn("Failed to schedule updated retention for topic {} ({})", topicName, topicId, error);
        }
    }

    private static ScheduledExecutorService newDaemonScheduler(String threadName) {
        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, threadName);
            thread.setDaemon(true);
            return thread;
        });
        // Long-poll timeouts are cancelled as soon as their fetch is answered; drop them from the
        // delay queue instead of letting them linger until they would have fired.
        scheduler.setRemoveOnCancelPolicy(true);
        return scheduler;
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

    /**
     * Retires every partition log and then releases the shared resources. The wait is bounded: a
     * partition log that does not settle within {@code timeoutMs} is logged and left behind rather
     * than allowed to hold the catalog, the Oxia client and the schedulers open.
     */
    void close(long timeoutMs) {
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("timeoutMs must be greater than zero");
        }
        synchronized (shutdownLock) {
            if (resourcesClosed) {
                return;
            }
            List<CompletableFuture<Void>> drains = new ArrayList<>();
            synchronized (lifecycleLock) {
                if (closed.compareAndSet(false, true)) {
                    retentionTask.cancel(false);
                }
                partitionLogs.values().forEach(partitionLog -> drains.add(partitionLog.close(false)));
            }
            // Cleanups started by an earlier cleanupPartition are no longer reachable through a
            // partition log, so they are drained separately.
            drains.add(sealProducerStateCleanups());
            try {
                CompletableFuture.allOf(drains.toArray(new CompletableFuture<?>[0]))
                        .get(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException | ExecutionException error) {
                log.warn("Diskless partition logs did not close within {} ms", timeoutMs, error);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            } finally {
                partitionLogs.clear();
                Utils.closeQuietly(lakestreamStorageHolder, "Ursa storage resources");
                shutdownScheduler(disklessTimer);
                shutdownScheduler(producerStateScheduler);
                resourcesClosed = true;
            }
        }
    }

    private static void shutdownScheduler(ScheduledExecutorService scheduler) {
        scheduler.shutdownNow();
    }

    CompletableFuture<Log> openLog(TopicIdPartition tp) {
        if (lakestreamStorageHolder != null) {
            return lakestreamStorageHolder.openPartition(tp);
        }
        return LakestreamStorageHolder.openPartition(catalog, tp);
    }

}
