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

import io.oxia.client.api.AsyncOxiaClient;
import io.streamnative.lakestream.api.Log;
import io.streamnative.lakestream.api.LogId;
import io.streamnative.ursa.lakestream.impl.IndexedStreamCatalog;

/**
 * Shared state container for Ursa storage components.
 * Manages per-partition Lakestream logs for the diskless reader and writer.
 */
public class UrsaStorageState implements DisklessStorageStateOperations {

    private static final Logger log = LoggerFactory.getLogger(UrsaStorageState.class);
    private static final long TOPIC_CONFIG_UPDATE_TIMEOUT_SECONDS = 30;

    private final Time time;
    private final int brokerId;
    private final UrsaStorageConfig config;
    private final BrokerTopicStats brokerTopicStats;
    private final DisklessLogMetrics logMetrics = new DisklessLogMetrics();

    private final ConcurrentHashMap<TopicIdPartition, UrsaPartitionLog> partitionLogs = new ConcurrentHashMap<>();

    private final LakestreamStorageHolder lakestreamStorageHolder;
    private final IndexedStreamCatalog catalog;
    private final Supplier<AsyncOxiaClient> oxiaClientSupplier;
    private final ScheduledExecutorService producerStateScheduler;
    private final ScheduledExecutorService retentionScheduler;
    private final ScheduledFuture<?> retentionTask;
    private final Map<String, Object> logConfigDefaults;
    private final Function<String, Map<String, String>> topicConfigSupplier;
    private final AtomicBoolean closed = new AtomicBoolean();

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

        LakestreamStorageHolder holder;
        try {
            holder = LakestreamStorageHolder.create(config);
            log.info("Initialized IndexedStreamCatalog for Kafka diskless storage");
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize IndexedStreamCatalog", e);
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
            IndexedStreamCatalog catalog) {
        this(time, brokerId, config, brokerTopicStats, catalog, Collections.emptyMap(), null);
    }

    UrsaStorageState(
            Time time,
            int brokerId,
            UrsaStorageConfig config,
            BrokerTopicStats brokerTopicStats,
            IndexedStreamCatalog catalog,
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

    public void updateTopicConfig(String topic, Map<String, String> topicConfig) {
        try {
            updateTopicConfigAsync(topic, topicConfig).get(TOPIC_CONFIG_UPDATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while updating topic config for " + topic, e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to update topic config for " + topic, e.getCause());
        } catch (TimeoutException e) {
            throw new RuntimeException("Timed out after " + TOPIC_CONFIG_UPDATE_TIMEOUT_SECONDS
                    + " seconds while updating topic config for " + topic, e);
        }
    }

    CompletableFuture<Void> updateTopicConfigAsync(String topic, Map<String, String> topicConfig) {
        if (topic == null || topicConfig == null || lakestreamStorageHolder == null) {
            return CompletableFuture.completedFuture(null);
        }
        Map<String, String> configSnapshot = Map.copyOf(topicConfig);
        // Kafka metadata is authoritative for retention. Trigger it immediately; catalog
        // persistence is for Lakestream/materialization consumers and must not delay local cleanup.
        triggerRetentionForTopic(topic, configSnapshot);
        return lakestreamStorageHolder.asyncUpdateTopicConfig(topic, configSnapshot)
                .whenComplete((ignored, error) -> {
                    if (error != null) {
                        log.warn("Failed to update topic config for Lakestream topic {}", topic, error);
                    }
                });
    }

    public void deleteTopicConfig(String topic) {
        if (topic == null || lakestreamStorageHolder == null) {
            return;
        }
        triggerRetentionForTopic(topic, Map.of());
        lakestreamStorageHolder.asyncDeleteTopicConfig(topic).join();
    }

    UrsaPartitionLog getOrCreatePartitionLog(TopicIdPartition tp) {
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
        AtomicBoolean cleaned = new AtomicBoolean();
        partitionLogs.computeIfPresent(tp, (ignored, partitionLog) -> {
            // Fence the old log while this key is still locked in the map. Otherwise a concurrent
            // computeIfAbsent can activate a replacement between remove() and close(), after which
            // the old owner's delayed fence would disable the replacement.
            partitionLog.close(deletePartition);
            cleaned.set(true);
            return null;
        });
        return cleaned.get();
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

    /**
     * Permanently delete the Lakestream log and its catalog metadata for a partition.
     *
     * <p>This path is idempotent: missing metadata is treated as success because the desired end state is already
     * reached.
     */
    public void deletePartitionData(TopicIdPartition tp) {
        if (tp == null) {
            return;
        }

        if (lakestreamStorageHolder == null) {
            return;
        }
        lakestreamStorageHolder.deletePartitionData(tp).join();
    }

    public Set<TopicIdPartition> snapshotTrackedPartitions() {
        return new LinkedHashSet<>(partitionLogs.keySet());
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

    private void triggerRetentionForTopic(String topic, Map<String, String> topicConfig) {
        try {
            RetentionConfig retentionConfig = buildRetentionConfig(topicConfig);
            partitionLogs.forEach((tp, partitionLog) -> {
                if (tp.topic().equals(topic)) {
                    partitionLog.triggerRetention(retentionConfig.retentionMs(), retentionConfig.retentionBytes());
                }
            });
        } catch (Throwable error) {
            log.warn("Failed to schedule updated retention for topic {}", topic, error);
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
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        retentionTask.cancel(false);
        shutdownScheduler(retentionScheduler);
        partitionLogs.values().forEach(UrsaPartitionLog::close);
        partitionLogs.clear();
        shutdownScheduler(producerStateScheduler);
        if (lakestreamStorageHolder != null) {
            lakestreamStorageHolder.close();
        }
    }

    private static void shutdownScheduler(ScheduledExecutorService scheduler) {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    CompletableFuture<Log> openLog(TopicIdPartition tp) {
        String name = KafkaManagedLedgerNaming.managedLedgerName(tp);
        Map<String, String> suppliedTopicConfig = topicConfigSupplier != null
                ? topicConfigSupplier.apply(tp.topic())
                : null;
        Map<String, String> topicConfig = suppliedTopicConfig != null ? Map.copyOf(suppliedTopicConfig) : Map.of();

        return catalog.generateStreamId(Optional.of(name))
                .thenCompose(streamId -> {
                    CompletableFuture<Void> registration = lakestreamStorageHolder == null
                            ? CompletableFuture.completedFuture(null)
                            : lakestreamStorageHolder.registerPartition(tp, streamId, topicConfig);
                    // registerPartition serializes registration with config events and exact-replaces
                    // properties using the latest published snapshot. Re-applying topicConfig here
                    // would let this open's stale snapshot overwrite a racing newer event.
                    return registration.thenApply(ignored -> catalog.createLog(LogId.of(streamId)));
                });
    }

}
