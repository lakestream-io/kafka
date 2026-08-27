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

package kafka.server.ursa.sdt;

import kafka.cluster.Partition;
import kafka.server.KafkaConfig;
import kafka.server.ReplicaManagerInterceptor;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.metadata.ConfigRepository;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.storage.internals.log.LogAppendInfo;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.Striped;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;

import io.lakestream.ursa.compaction.CompactionManager;
import io.lakestream.ursa.compaction.DynamicConfigs;
import io.lakestream.ursa.compaction.OxiaCompactTaskManager;
import io.lakestream.ursa.storage.UrsaStorage;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;

/**
 * UrsaSDTInterceptor to initialize the ursa compaction task publisher to prepare the compaction task
 * for write the data to the lakehouse table.
 */
public class UrsaSDTInterceptor implements ReplicaManagerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(UrsaSDTInterceptor.class);
    private static final long INACTIVE_PUBLISHER_FENCE_RETENTION_MINUTES = 5L;
    private static final long PUBLICATION_CLOSE_RETRY_INITIAL_MS = 100L;
    private static final long PUBLICATION_CLOSE_RETRY_MAX_MS = 5_000L;
    public static final String PUBLISHER_BROKER_ID_PROPERTY = "publisherBrokerId";
    public static final String EXTRA_INTERNAL_TOPICS_CONFIG = "ursaExtraInternalTopics";

    private static final Set<String> DEFAULT_INTERNAL_TOPICS = Set.of(
        "__consumer_offsets",
        "sn.system.__kafka_aliveness",
        "strimzi.cruisecontrol.metrics",
        "strimzi.cruisecontrol.modeltrainingsamples",
        "strimzi.cruisecontrol.partitionmetricsamples"
    );

    private final KafkaConfig clusterConfig;
    private final ConfigRepository topicConfigRepository;
    private final ScheduledExecutorService scheduledExecutorService;
    private final DynamicConfigs clusterDynamicConfigs;
    private final AsyncOxiaClient oxiaClient;
    private final CompactionManager compactionManager;
    private final long publishTaskCheckInterval;

    private Cache<String, TopicCompactionTaskPublisher> compactionTaskPublisherCache;
    private Cache<String, DisklessCompactionTaskPublisher> disklessCompactionTaskPublisherCache;
    private final ConcurrentHashMap<String, TopicCompactionTaskPublisher> currentClassicPublishers =
        new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, DisklessCompactionTaskPublisher> currentDisklessPublishers =
        new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, PublisherEvent> latestPublisherEvents = new ConcurrentHashMap<>();
    private final Set<ScheduledFuture<?>> inactiveEventCleanupTasks = ConcurrentHashMap.newKeySet();
    private final Set<CompactionManager.PublicationSession> pendingPublicationSessionCloses =
        ConcurrentHashMap.newKeySet();
    private final AtomicLong publisherEventSequence = new AtomicLong();
    private final AtomicBoolean closing = new AtomicBoolean();
    private final Striped<Lock> publisherEventLocks = Striped.lock(64);
    private final Striped<ReadWriteLock> publisherOperationLocks = Striped.readWriteLock(64);

    private final Set<String> internalTopics;

    public UrsaSDTInterceptor(KafkaConfig clusterConfig, ConfigRepository topicConfigRepository) {
        this(clusterConfig, topicConfigRepository, null, null, null);
    }

    UrsaSDTInterceptor(KafkaConfig clusterConfig,
                       ConfigRepository topicConfigRepository,
                       ScheduledExecutorService scheduledExecutorService,
                       AsyncOxiaClient oxiaClient,
                       CompactionManager compactionManager) {
        this.clusterConfig = clusterConfig;
        this.topicConfigRepository = topicConfigRepository;
        var properties = new Properties();
        properties.putAll(clusterConfig.props());
        this.clusterDynamicConfigs = new DynamicConfigs("default", properties, false);
        this.publishTaskCheckInterval = validatedPublishInterval(clusterDynamicConfigs);
        this.internalTopics = buildInternalTopics(clusterConfig);
        this.compactionTaskPublisherCache = createPublisherCache();
        this.disklessCompactionTaskPublisherCache = createDisklessPublisherCache();
        this.scheduledExecutorService = scheduledExecutorService == null
            ? Executors.newScheduledThreadPool(getUrsaCompactionTaskPublisherThreadNum())
            : scheduledExecutorService;
        if (compactionManager == null) {
            this.oxiaClient = oxiaClient == null ? getOxiaClient() : oxiaClient;
            this.compactionManager = getCompactionManager();
        } else {
            this.oxiaClient = oxiaClient;
            this.compactionManager = compactionManager;
        }
    }

    private static Set<String> buildInternalTopics(KafkaConfig clusterConfig) {
        var topics = new HashSet<>(DEFAULT_INTERNAL_TOPICS);
        var extraTopicsValue = Optional.ofNullable(clusterConfig.props().get(EXTRA_INTERNAL_TOPICS_CONFIG))
            .map(Object::toString)
            .orElse("");
        if (!extraTopicsValue.isBlank()) {
            Arrays.stream(extraTopicsValue.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .forEach(topics::add);
        }
        return Set.copyOf(topics);
    }

    private static long validatedPublishInterval(DynamicConfigs configs) {
        long seconds = Long.parseLong(configs
            .getProperty("clusterTailCompactDataVisibilityIntervalInSeconds").orElse("180"));
        if (seconds <= 0) {
            throw new IllegalArgumentException(
                "clusterTailCompactDataVisibilityIntervalInSeconds must be greater than zero");
        }
        return seconds;
    }

    private Cache<String, TopicCompactionTaskPublisher> createPublisherCache() {
        return CacheBuilder.newBuilder()
            .removalListener(notification -> {
                var publisher = (TopicCompactionTaskPublisher) notification.getValue();
                if (publisher != null) {
                    currentClassicPublishers.remove((String) notification.getKey(), publisher);
                    try {
                        publisher.close();
                    } catch (Exception e) {
                        log.warn("Failed to close the compaction task publisher for topic {}", notification.getKey(), e);
                    }
                }
            })
            .build();
    }

    private Cache<String, DisklessCompactionTaskPublisher> createDisklessPublisherCache() {
        return CacheBuilder.newBuilder()
            .removalListener(notification -> {
                var publisher = (DisklessCompactionTaskPublisher) notification.getValue();
                if (publisher != null) {
                    publisher.close();
                    currentDisklessPublishers.remove((String) notification.getKey(), publisher);
                }
            })
            .build();
    }

    private int getUrsaCompactionTaskPublisherThreadNum() {
        var configs = clusterConfig.props();
        return Optional.ofNullable(configs.get("ursaCompactionTaskPublisherThreadNum"))
            .map(Object::toString)
            .map(Integer::parseInt)
            .orElse(10);
    }

    AsyncOxiaClient getOxiaClient() {
        try {
            var configuredOxiaServiceUrl = clusterConfig.getString(ServerLogConfigs.URSA_OXIA_SERVICE_URL_CONFIG);
            var oxiaServiceUrl = UrsaStorage.validateOxiaUrl(configuredOxiaServiceUrl);
            return OxiaClientBuilder.create(oxiaServiceUrl.getLeft())
                .namespace(oxiaServiceUrl.getRight())
                .asyncClient().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize the oxia client for the compaction manager", e);
        }
    }

    CompactionManager getCompactionManager() {
        try {
            var compactTaskManager = new OxiaCompactTaskManager(oxiaClient);
            return new CompactionManager(compactTaskManager);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize the compaction manager with the configuration, please "
                                       + "check if the oxia service url is set right", e);
        }
    }

    public DynamicConfigs getDynamicConfigs(String topic) {
        var configuredTopicProperties = topicConfigRepository.topicConfig(topic);
        var topicProperties = Optional.ofNullable(configuredTopicProperties).orElseGet(Properties::new).entrySet()
            .stream().collect(
                Collectors.toMap(
                    entry -> entry.getKey().toString(),
                    entry -> entry.getValue().toString()));
        var configs = DynamicConfigs.of(clusterDynamicConfigs);
        configs.overrideWith(topicProperties);
        return configs;
    }

    @Override
    public void onAppend(MemoryRecords records, LogAppendInfo appendInfo, Partition partition) {
        triggerClassicPublication(partition, 0L, false);
    }

    @Override
    public void onAppend(
            MemoryRecords records,
            LogAppendInfo appendInfo,
            Partition partition,
            long publisherGeneration) {
        triggerClassicPublication(partition, publisherGeneration, true);
    }

    @Override
    public void onLeadershipAcquired(Partition partition, long publisherGeneration) {
        triggerClassicPublication(partition, publisherGeneration, true);
    }

    @Override
    public void onLeadershipLost(TopicIdPartition topicIdPartition) {
        if (closing.get() || topicIdPartition == null) {
            return;
        }
        String topicName = getTopicName(topicIdPartition);
        PublisherEvent event;
        Lock eventLock = publisherEventLocks.get(topicName);
        eventLock.lock();
        try {
            if (closing.get()) {
                return;
            }
            event = recordClassicLeadershipLoss(topicName, topicIdPartition);
            if (event == null) {
                return;
            }
            beforePublisherFence(topicName);
            fenceMatchingClassicPublisher(topicName, topicIdPartition);
        } finally {
            eventLock.unlock();
        }
        scheduleInactiveEventCleanup(topicName, event);
    }

    private void triggerClassicPublication(
            Partition partition,
            long publisherGeneration,
            boolean authoritativeIncarnation) {
        try {
            if (partition == null || !partition.isLeader()) {
                return;
            }
            var topic = partition.topic();
            if (closing.get() || internalTopics.contains(topic)) {
                return;
            }
            var topicName = getTopicName(partition);
            if (partition.topicId().isEmpty()) {
                log.warn("Cannot start the compaction publisher for {} without a topic id", topicName);
                return;
            }
            var topicIdPartition = new TopicIdPartition(
                partition.topicId().get(), new TopicPartition(topic, partition.partitionId()));
            PublisherEvent event;
            Lock eventLock = publisherEventLocks.get(topicName);
            eventLock.lock();
            try {
                if (closing.get() || !isCurrentClassicLeader(partition, topicIdPartition)) {
                    return;
                }
                event = recordPublisherEvent(
                    topicName,
                    topicIdPartition,
                    PublisherMode.CLASSIC,
                    publisherGeneration,
                    authoritativeIncarnation);
                if (event == null) {
                    return;
                }
                beforePublisherFence(topicName);
                fenceConflictingPublisher(event, null);
            } finally {
                eventLock.unlock();
            }
            scheduleClassicPublisherUpdate(topicName, event, partition);
        } catch (Throwable throwable) {
            // Compaction side effects must not break either append or leadership changes.
            log.error("Unexpected error while triggering classic Ursa compaction", throwable);
        }
    }

    private void scheduleClassicPublisherUpdate(
            String topicName,
            PublisherEvent event,
            Partition partition) {
        try {
            scheduledExecutorService.execute(() -> updateClassicPublisher(event, partition));
        } catch (RejectedExecutionException error) {
            // A shutdown race must not leave an event which can suppress a later callback.
            latestPublisherEvents.remove(topicName, event);
            if (!closing.get()) {
                log.warn("Could not schedule classic compaction publisher update for {}", topicName, error);
            }
        }
    }

    @Override
    public void onDisklessAppend(
            TopicIdPartition topicIdPartition,
            long streamId,
            long highWatermark) {
        onDisklessAppend(topicIdPartition, streamId, highWatermark, 0L, false);
    }

    @Override
    public void onDisklessAppend(
            TopicIdPartition topicIdPartition,
            long streamId,
            long highWatermark,
            long ownershipGeneration) {
        onDisklessAppend(topicIdPartition, streamId, highWatermark, ownershipGeneration, true);
    }

    private void onDisklessAppend(
            TopicIdPartition topicIdPartition,
            long streamId,
            long highWatermark,
            long ownershipGeneration,
            boolean authoritativeIncarnation) {
        try {
            if (closing.get()
                    || topicIdPartition == null
                    || internalTopics.contains(topicIdPartition.topic())) {
                return;
            }
            String topicName = getTopicName(topicIdPartition);
            PublisherEvent event;
            Lock eventLock = publisherEventLocks.get(topicName);
            eventLock.lock();
            try {
                if (closing.get()) {
                    return;
                }
                event = recordPublisherEvent(
                    topicName,
                    topicIdPartition,
                    PublisherMode.DISKLESS,
                    ownershipGeneration,
                    authoritativeIncarnation);
                if (event == null) {
                    return;
                }
                beforePublisherFence(topicName);
                fenceConflictingPublisher(event, streamId);
            } finally {
                eventLock.unlock();
            }
            scheduledExecutorService.execute(
                () -> updateDisklessPublisher(event, streamId, highWatermark));
        } catch (Throwable throwable) {
            // The durable append has already succeeded. Compaction side effects cannot fail the produce request.
            log.error("Unexpected error in the diskless ursa sdt interceptor", throwable);
        }
    }

    @Override
    public void onPartitionOwnershipLost(TopicIdPartition topicIdPartition) {
        onPartitionOwnershipLost(topicIdPartition, 0L, false);
    }

    @Override
    public void onPartitionOwnershipLost(
            TopicIdPartition topicIdPartition,
            long ownershipGeneration) {
        onPartitionOwnershipLost(topicIdPartition, ownershipGeneration, true);
    }

    private void onPartitionOwnershipLost(
            TopicIdPartition topicIdPartition,
            long ownershipGeneration,
            boolean authoritativeGeneration) {
        if (closing.get() || topicIdPartition == null) {
            return;
        }
        String topicName = getTopicName(topicIdPartition);
        PublisherEvent event;
        Lock eventLock = publisherEventLocks.get(topicName);
        eventLock.lock();
        try {
            if (closing.get()) {
                return;
            }
            event = recordOwnershipLoss(
                topicName, topicIdPartition, ownershipGeneration, authoritativeGeneration);
            if (event == null) {
                return;
            }
            beforePublisherFence(topicName);
            fenceConflictingPublisher(event, null);
        } finally {
            eventLock.unlock();
        }
        // Do not wait for a possibly blocked remote publication. Closing the generation makes
        // each subsequent remote phase abort, while the event tombstone prevents queued appends
        // from reinstalling it.
        scheduleInactiveEventCleanup(topicName, event);
    }

    private PublisherEvent recordPublisherEvent(
            String topicName,
            TopicIdPartition topicIdPartition,
            PublisherMode mode,
            long ownershipGeneration,
            boolean authoritativeIncarnation) {
        var event = new PublisherEvent(
            publisherEventSequence.incrementAndGet(),
            topicIdPartition,
            mode,
            ownershipGeneration);
        var selected = latestPublisherEvents.compute(
            topicName,
            (ignored, current) -> selectPublisherEvent(
                topicName, current, event, authoritativeIncarnation));
        return selected == event ? event : null;
    }

    private PublisherEvent selectPublisherEvent(
            String topicName,
            PublisherEvent current,
            PublisherEvent event,
            boolean authoritativeIncarnation) {
        if (current == null) {
            return event;
        }
        if (current.sequence() > event.sequence()) {
            return current;
        }
        boolean sameTopicIncarnation = current.topicIdPartition().topicId()
            .equals(event.topicIdPartition().topicId());
        if (!sameTopicIncarnation) {
            return selectTopicIncarnation(
                current, event, authoritativeIncarnation);
        }
        return isStaleSameIncarnation(current, event, authoritativeIncarnation)
            ? current : event;
    }

    private PublisherEvent selectTopicIncarnation(
            PublisherEvent current,
            PublisherEvent event,
            boolean authoritativeIncarnation) {
        if (!authoritativeIncarnation && current.mode() != PublisherMode.OWNERSHIP_LOST) {
            return current;
        }
        if (authoritativeIncarnation
                && event.ownershipGeneration() < current.ownershipGeneration()) {
            return current;
        }
        return event;
    }

    private boolean isStaleSameIncarnation(
            PublisherEvent current,
            PublisherEvent event,
            boolean authoritativeIncarnation) {
        if (event.mode() == PublisherMode.DISKLESS) {
            if (current.mode() == PublisherMode.OWNERSHIP_LOST) {
                return event.ownershipGeneration() <= current.ownershipGeneration();
            }
            if (current.mode() == PublisherMode.DISKLESS) {
                return event.ownershipGeneration() < current.ownershipGeneration();
            }
        }
        if (authoritativeIncarnation
                && event.mode() == PublisherMode.CLASSIC
                && current.mode() == PublisherMode.CLASSIC) {
            return event.ownershipGeneration() < current.ownershipGeneration();
        }
        if (!authoritativeIncarnation) {
            return false;
        }
        if (current.mode() == PublisherMode.OWNERSHIP_LOST) {
            return event.mode() == PublisherMode.CLASSIC
                && event.ownershipGeneration() < current.ownershipGeneration();
        }
        return event.mode() != current.mode()
            && event.ownershipGeneration() <= current.ownershipGeneration();
    }

    private PublisherEvent recordOwnershipLoss(
            String topicName,
            TopicIdPartition topicIdPartition,
            long ownershipGeneration,
            boolean authoritativeGeneration) {
        var event = new PublisherEvent(
            publisherEventSequence.incrementAndGet(),
            topicIdPartition,
            PublisherMode.OWNERSHIP_LOST,
            ownershipGeneration);
        var selected = latestPublisherEvents.compute(topicName, (ignored, current) -> {
            if (current != null && current.sequence() > event.sequence()) {
                return current;
            }
            if (current != null) {
                if (current.mode() == PublisherMode.CLASSIC
                        || !current.topicIdPartition().topicId().equals(topicIdPartition.topicId())) {
                    return current;
                }
                if (authoritativeGeneration
                        && ownershipGeneration <= current.ownershipGeneration()) {
                    return current;
                }
                if (!authoritativeGeneration && current.mode() == PublisherMode.OWNERSHIP_LOST) {
                    return current;
                }
            }
            return event;
        });
        return selected == event ? event : null;
    }

    private PublisherEvent recordClassicLeadershipLoss(
            String topicName,
            TopicIdPartition topicIdPartition) {
        var recorded = new AtomicReference<PublisherEvent>();
        latestPublisherEvents.compute(topicName, (ignored, current) -> {
            // A delayed follower transition must not fence a same-name recreation or a
            // diskless owner which has already superseded the classic publisher.
            if (current == null
                    || current.mode() != PublisherMode.CLASSIC
                    || !current.topicIdPartition().equals(topicIdPartition)) {
                return current;
            }
            var event = new PublisherEvent(
                publisherEventSequence.incrementAndGet(),
                topicIdPartition,
                PublisherMode.OWNERSHIP_LOST,
                current.ownershipGeneration());
            recorded.set(event);
            return event;
        });
        return recorded.get();
    }

    private void fenceConflictingPublisher(PublisherEvent event, Long streamId) {
        String topicName = getTopicName(event.topicIdPartition());
        if (event.mode() == PublisherMode.CLASSIC) {
            closePublisher(currentDisklessPublishers.get(topicName));
            TopicCompactionTaskPublisher current = currentClassicPublishers.get(topicName);
            if (current != null
                    && !current.topicIdPartition().equals(event.topicIdPartition())) {
                current.close();
            }
            return;
        }
        if (event.mode() == PublisherMode.DISKLESS) {
            closePublisher(currentClassicPublishers.get(topicName));
            DisklessCompactionTaskPublisher current = currentDisklessPublishers.get(topicName);
            if (current != null
                    && (!current.topicIdPartition().topicId().equals(event.topicIdPartition().topicId())
                        || current.streamId() != streamId)) {
                current.close();
            }
            return;
        }
        closeMatchingPublishers(topicName, event.topicIdPartition());
    }

    void beforePublisherFence(String topicName) {
        // Test seam for deterministic event-ordering coverage.
    }

    private void updateClassicPublisher(PublisherEvent event, Partition partition) {
        String topicName = getTopicName(event.topicIdPartition());
        var writeLock = publisherOperationLocks.get(topicName).writeLock();
        writeLock.lock();
        try {
            updateClassicPublisherLocked(topicName, event, partition);
        } catch (Throwable error) {
            log.error("Failed to update the classic compaction publisher for {}", topicName, error);
        } finally {
            writeLock.unlock();
        }
    }

    private void updateClassicPublisherLocked(
            String topicName,
            PublisherEvent event,
            Partition partition) {
        if (closing.get() || !isLatestEvent(topicName, event)) {
            return;
        }
        if (!isCurrentClassicLeader(partition, event.topicIdPartition())) {
            latestPublisherEvents.remove(topicName, event);
            return;
        }
        if (!getDynamicConfigs(event.topicIdPartition().topic()).sdtEnabled()) {
            closeDisklessPublisher(topicName);
            closeClassicPublisher(topicName);
            scheduleInactiveEventCleanup(topicName, event);
            return;
        }
        if (hasReusableClassicPublisher(topicName, event.topicIdPartition())) {
            return;
        }
        installClassicPublisher(topicName, event, partition);
    }

    private boolean hasReusableClassicPublisher(
            String topicName,
            TopicIdPartition topicIdPartition) {
        TopicCompactionTaskPublisher current = currentClassicPublishers.get(topicName);
        return current != null
            && !current.isClosed()
            && current.topicIdPartition().equals(topicIdPartition);
    }

    private void installClassicPublisher(
            String topicName,
            PublisherEvent event,
            Partition partition) {
        var replacement = createPublisher(partition);
        if (!isValidClassicReplacement(topicName, event, partition, replacement)) {
            replacement.close();
            return;
        }
        closeDisklessPublisher(topicName);
        closeClassicPublisher(topicName);
        currentClassicPublishers.put(topicName, replacement);
        compactionTaskPublisherCache.put(topicName, replacement);
        if (!isValidClassicReplacement(topicName, event, partition, replacement)) {
            closeClassicPublisher(topicName);
            return;
        }
        startClassicPublisher(topicName, event, replacement);
    }

    private boolean isValidClassicReplacement(
            String topicName,
            PublisherEvent event,
            Partition partition,
            TopicCompactionTaskPublisher replacement) {
        return replacement.topicIdPartition().equals(event.topicIdPartition())
            && isCurrentClassicLeader(partition, event.topicIdPartition())
            && isLatestEvent(topicName, event);
    }

    private void startClassicPublisher(
            String topicName,
            PublisherEvent event,
            TopicCompactionTaskPublisher replacement) {
        try {
            replacement.start(Duration.ofSeconds(publishTaskCheckInterval));
            log.info("Starting the compaction task publisher for the topic {}", topicName);
        } catch (RejectedExecutionException error) {
            closeClassicPublisher(topicName);
            latestPublisherEvents.remove(topicName, event);
            if (!closing.get()) {
                log.warn("Could not start the classic compaction publisher for {}", topicName, error);
            }
        }
    }

    private boolean isCurrentClassicLeader(
            Partition partition,
            TopicIdPartition expectedTopicIdPartition) {
        return partition.isLeader()
            && partition.partitionId() == expectedTopicIdPartition.partition()
            && partition.topic().equals(expectedTopicIdPartition.topic())
            && !partition.topicId().isEmpty()
            && partition.topicId().get().equals(expectedTopicIdPartition.topicId());
    }

    private void updateDisklessPublisher(
            PublisherEvent event,
            long streamId,
            long highWatermark) {
        TopicIdPartition topicIdPartition = event.topicIdPartition();
        String topicName = getTopicName(topicIdPartition);
        var writeLock = publisherOperationLocks.get(topicName).writeLock();
        writeLock.lock();
        try {
            if (closing.get() || !isLatestEvent(topicName, event)) {
                return;
            }
            closeClassicPublisher(topicName);
            if (!getDynamicConfigs(topicIdPartition.topic()).sdtEnabled()) {
                closeDisklessPublisher(topicName);
                scheduleInactiveEventCleanup(topicName, event);
                return;
            }
            DisklessCompactionTaskPublisher current = currentDisklessPublishers.get(topicName);
            if (current != null
                    && !current.isClosed()
                    && current.streamId() == streamId
                    && current.topicIdPartition().equals(topicIdPartition)) {
                current.observeHighWatermark(highWatermark);
                return;
            }
            closeDisklessPublisher(topicName);
            var replacement = new DisklessCompactionTaskPublisher(
                this, compactionManager, topicIdPartition, streamId, scheduledExecutorService);
            replacement.observeHighWatermark(highWatermark);
            currentDisklessPublishers.put(topicName, replacement);
            disklessCompactionTaskPublisherCache.put(topicName, replacement);
            if (!isLatestEvent(topicName, event)) {
                closeDisklessPublisher(topicName);
                return;
            }
            replacement.start(Duration.ofSeconds(publishTaskCheckInterval));
            log.info("Starting the diskless compaction task publisher for {} with stream id {}",
                topicName, streamId);
        } catch (Throwable error) {
            log.error("Failed to update the diskless compaction publisher for {}", topicName, error);
        } finally {
            writeLock.unlock();
        }
    }

    boolean runIfCurrentDisklessPublisher(
            String topicName,
            DisklessCompactionTaskPublisher publisher,
            DisklessPublisherAction action) throws Exception {
        var readLock = publisherOperationLocks.get(topicName).readLock();
        readLock.lock();
        try {
            if (!isCurrentDisklessPublisher(topicName, publisher)) {
                return false;
            }
            action.run();
            return true;
        } finally {
            readLock.unlock();
        }
    }

    boolean runIfCurrentClassicPublisher(
            String topicName,
            TopicCompactionTaskPublisher publisher,
            ClassicPublisherAction action) throws Exception {
        var readLock = publisherOperationLocks.get(topicName).readLock();
        readLock.lock();
        try {
            if (!isCurrentClassicPublisher(topicName, publisher)) {
                return false;
            }
            action.run();
            return true;
        } finally {
            readLock.unlock();
        }
    }

    boolean isCurrentDisklessPublisher(String topicName, DisklessCompactionTaskPublisher publisher) {
        return !publisher.isClosed() && currentDisklessPublishers.get(topicName) == publisher;
    }

    boolean isCurrentClassicPublisher(String topicName, TopicCompactionTaskPublisher publisher) {
        return !publisher.isClosed() && currentClassicPublishers.get(topicName) == publisher;
    }

    boolean hasClassicPublisher(String topicName) {
        return currentClassicPublishers.containsKey(topicName)
            || compactionTaskPublisherCache.getIfPresent(topicName) != null;
    }

    boolean hasDisklessPublisher(String topicName) {
        return currentDisklessPublishers.containsKey(topicName)
            || disklessCompactionTaskPublisherCache.getIfPresent(topicName) != null;
    }

    boolean hasPublisherEvent(String topicName) {
        return latestPublisherEvents.containsKey(topicName);
    }

    void closeDisklessPublisherIfIdle(
            String topicName,
            DisklessCompactionTaskPublisher publisher) {
        var writeLock = publisherOperationLocks.get(topicName).writeLock();
        writeLock.lock();
        try {
            if (currentDisklessPublishers.get(topicName) == publisher
                    && !publisher.hasPendingData()) {
                closeDisklessPublisher(topicName);
                scheduleInactiveEventCleanup(topicName, latestPublisherEvents.get(topicName));
            }
        } finally {
            writeLock.unlock();
        }
    }

    void closeDisklessPublisherIfCurrent(
            String topicName,
            DisklessCompactionTaskPublisher publisher) {
        var writeLock = publisherOperationLocks.get(topicName).writeLock();
        writeLock.lock();
        try {
            if (currentDisklessPublishers.get(topicName) == publisher && publisher.isClosed()) {
                closeDisklessPublisher(topicName);
                scheduleInactiveEventCleanup(topicName, latestPublisherEvents.get(topicName));
            }
        } finally {
            writeLock.unlock();
        }
    }

    void closePublicationSessionAsync(CompactionManager.PublicationSession session) {
        session.fence();
        if (!pendingPublicationSessionCloses.add(session)) {
            return;
        }
        schedulePublicationSessionClose(session, 0, 0L);
    }

    private void schedulePublicationSessionClose(
            CompactionManager.PublicationSession session,
            int attempt,
            long delayMs) {
        try {
            Runnable closeAttempt = () -> attemptPublicationSessionClose(session, attempt);
            if (delayMs == 0L) {
                scheduledExecutorService.execute(closeAttempt);
            } else {
                scheduledExecutorService.schedule(closeAttempt, delayMs, TimeUnit.MILLISECONDS);
            }
        } catch (RejectedExecutionException error) {
            log.warn("Could not schedule publication session close for {}", session.topicName(), error);
        }
    }

    private void attemptPublicationSessionClose(
            CompactionManager.PublicationSession session,
            int attempt) {
        if (!pendingPublicationSessionCloses.contains(session)) {
            return;
        }
        try {
            session.close();
            pendingPublicationSessionCloses.remove(session);
        } catch (Exception error) {
            long retryDelayMs = Math.min(
                PUBLICATION_CLOSE_RETRY_MAX_MS,
                PUBLICATION_CLOSE_RETRY_INITIAL_MS << Math.min(attempt, 5));
            log.warn("Failed to close compaction publication session for {}; retrying in {} ms",
                session.topicName(), retryDelayMs, error);
            schedulePublicationSessionClose(session, attempt + 1, retryDelayMs);
        }
    }

    private void drainPendingPublicationSessionCloses() {
        Set.copyOf(pendingPublicationSessionCloses).forEach(session -> {
            try {
                session.close();
                pendingPublicationSessionCloses.remove(session);
            } catch (Exception error) {
                log.warn("Failed final publication session close for {}", session.topicName(), error);
            }
        });
    }

    void closeClassicPublisherIfCurrent(
            String topicName,
            TopicCompactionTaskPublisher publisher) {
        var writeLock = publisherOperationLocks.get(topicName).writeLock();
        writeLock.lock();
        try {
            if (currentClassicPublishers.get(topicName) == publisher && publisher.isClosed()) {
                closeClassicPublisher(topicName);
                scheduleInactiveEventCleanup(topicName, latestPublisherEvents.get(topicName));
            }
        } finally {
            writeLock.unlock();
        }
    }

    void closeClassicPublisherIfIdle(
            String topicName,
            TopicCompactionTaskPublisher publisher) {
        var writeLock = publisherOperationLocks.get(topicName).writeLock();
        writeLock.lock();
        try {
            if (currentClassicPublishers.get(topicName) == publisher
                    && !publisher.hasPendingData()) {
                closeClassicPublisher(topicName);
                scheduleInactiveEventCleanup(topicName, latestPublisherEvents.get(topicName));
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void scheduleInactiveEventCleanup(String topicName, PublisherEvent expectedEvent) {
        if (expectedEvent == null || closing.get()) {
            return;
        }
        try {
            var cleanupFuture = new AtomicReference<ScheduledFuture<?>>();
            Runnable cleanup = () -> {
                try {
                    if (!currentClassicPublishers.containsKey(topicName)
                            && !currentDisklessPublishers.containsKey(topicName)) {
                        latestPublisherEvents.remove(topicName, expectedEvent);
                    }
                } finally {
                    inactiveEventCleanupTasks.remove(cleanupFuture.get());
                }
            };
            ScheduledFuture<?> future = scheduledExecutorService.schedule(
                cleanup, INACTIVE_PUBLISHER_FENCE_RETENTION_MINUTES, TimeUnit.MINUTES);
            cleanupFuture.set(future);
            if (!future.isDone()) {
                inactiveEventCleanupTasks.add(future);
            }
        } catch (Throwable error) {
            if (!closing.get()) {
                log.debug("Failed to schedule inactive publisher fence cleanup for {}", topicName, error);
            }
        }
    }

    @FunctionalInterface
    interface DisklessPublisherAction {
        void run() throws Exception;
    }

    @FunctionalInterface
    interface ClassicPublisherAction {
        void run() throws Exception;
    }

    private boolean isLatestEvent(String topicName, PublisherEvent event) {
        return latestPublisherEvents.get(topicName) == event;
    }

    private void closeMatchingPublishers(String topicName, TopicIdPartition topicIdPartition) {
        TopicCompactionTaskPublisher classic = currentClassicPublishers.get(topicName);
        if (classic != null && classic.topicIdPartition().equals(topicIdPartition)) {
            closeClassicPublisher(topicName);
        }
        DisklessCompactionTaskPublisher diskless = currentDisklessPublishers.get(topicName);
        if (diskless != null && diskless.topicIdPartition().equals(topicIdPartition)) {
            closeDisklessPublisher(topicName);
        }
    }

    private void fenceMatchingClassicPublisher(
            String topicName,
            TopicIdPartition topicIdPartition) {
        TopicCompactionTaskPublisher classic = currentClassicPublishers.get(topicName);
        if (classic == null || !classic.topicIdPartition().equals(topicIdPartition)) {
            return;
        }
        classic.close();
        try {
            scheduledExecutorService.execute(
                () -> closeClassicPublisherIfCurrent(topicName, classic));
        } catch (RejectedExecutionException error) {
            if (!closing.get()) {
                log.warn("Could not schedule closed classic publisher cleanup for {}", topicName, error);
            }
        }
    }

    private void closeClassicPublisher(String topicName) {
        TopicCompactionTaskPublisher publisher = currentClassicPublishers.remove(topicName);
        compactionTaskPublisherCache.invalidate(topicName);
        closePublisher(publisher);
    }

    private void closeDisklessPublisher(String topicName) {
        DisklessCompactionTaskPublisher publisher = currentDisklessPublishers.remove(topicName);
        disklessCompactionTaskPublisherCache.invalidate(topicName);
        closePublisher(publisher);
    }

    private static void closePublisher(AutoCloseable publisher) {
        if (publisher == null) {
            return;
        }
        try {
            publisher.close();
        } catch (Exception error) {
            log.warn("Failed to close compaction publisher", error);
        }
    }

    private TopicCompactionTaskPublisher createPublisher(Partition partition) {
        return new TopicCompactionTaskPublisher(this, compactionManager,
            partition, scheduledExecutorService);
    }

    private enum PublisherMode {
        CLASSIC,
        DISKLESS,
        OWNERSHIP_LOST
    }

    private record PublisherEvent(
            long sequence,
            TopicIdPartition topicIdPartition,
            PublisherMode mode,
            long ownershipGeneration) {
    }

    public String getTopicName(Partition partition) {
        return String.format("%s-partition-%d", partition.topic(), partition.partitionId());
    }

    public String getTopicName(TopicIdPartition topicIdPartition) {
        return String.format("%s-partition-%d",
                topicIdPartition.topic(), topicIdPartition.partition());
    }

    public int brokerId() {
        return clusterConfig.nodeId();
    }

    @Override
    public void close() throws Exception {
        if (!closing.compareAndSet(false, true)) {
            return;
        }
        compactionTaskPublisherCache.invalidateAll();
        disklessCompactionTaskPublisherCache.invalidateAll();
        currentClassicPublishers.clear();
        currentDisklessPublishers.clear();
        latestPublisherEvents.clear();
        inactiveEventCleanupTasks.forEach(future -> future.cancel(false));
        inactiveEventCleanupTasks.clear();
        scheduledExecutorService.shutdown();
        try {
            if (!scheduledExecutorService.awaitTermination(30, TimeUnit.SECONDS)) {
                scheduledExecutorService.shutdownNow();
                if (!scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn("Compaction task publisher executor did not terminate after forced shutdown");
                }
            }
        } catch (InterruptedException e) {
            scheduledExecutorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        drainPendingPublicationSessionCloses();
        if (oxiaClient != null) {
            oxiaClient.close();
        }
        pendingPublicationSessionCloses.clear();
        compactionTaskPublisherCache.invalidateAll();
        disklessCompactionTaskPublisherCache.invalidateAll();
        currentClassicPublishers.clear();
        currentDisklessPublishers.clear();
        latestPublisherEvents.clear();
    }
}
