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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.storage.diskless.handlers.KafkaLogNaming;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.streamnative.ursa.compaction.CompactionManager;
import io.streamnative.ursa.compaction.DynamicConfigs;
import io.streamnative.ursa.compaction.PublicationFencedException;
import io.streamnative.ursa.compaction.task.PreparedCompactStreamTask;

public class TopicCompactionTaskPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TopicCompactionTaskPublisher.class);
    private static final int IDLE_TICKS_BEFORE_CLOSE = 3;
    static final String ENTRY_FORMAT_PROPERTY = "entryFormat";
    static final String KAFKA_ENTRY_FORMAT = "KAFKA";
    static final String SOURCE_TOPIC_PROPERTY = "sourceTopic";
    static final String SOURCE_SCHEMA_TOPIC_PROPERTY = "sourceSchemaTopic";
    static final String SOURCE_TOPIC_ID_PROPERTY = "sourceTopicId";

    private final CompactionManager compactionManager;
    private final UrsaSDTInterceptor interceptor;
    private final Partition partition;
    private final String topic;
    private final String publisherKey;
    private final String taskTopicName;
    private final TopicIdPartition topicIdPartition;
    private final long idForCompactionTask;
    private final ScheduledExecutorService executors;
    private volatile Duration duration;

    private ScheduledFuture<?> scheduledFuture;
    private volatile long lastPublishedOffset;
    private final AtomicBoolean publishRunning = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean publicationFenced = new AtomicBoolean(false);
    private final AtomicInteger idleTicks = new AtomicInteger();
    private final AtomicReference<CompactionManager.PublicationSession> publicationSession =
        new AtomicReference<>();

    public TopicCompactionTaskPublisher(UrsaSDTInterceptor interceptor,
                                        CompactionManager compactionManager,
                                        Partition partition,
                                        ScheduledExecutorService executors) {
        this.interceptor = interceptor;
        this.compactionManager = compactionManager;
        this.partition = partition;
        this.topic = partition.topic();
        if (partition.topicId().isEmpty()) {
            throw new IllegalStateException("please check that your Kafka version is new enough to provide a topic id");
        }
        var topicId = partition.topicId().get();
        this.topicIdPartition = new TopicIdPartition(
            topicId, new TopicPartition(topic, partition.partitionId()));
        // Keep the name-only publisher key so a same-name recreation fences the old publisher.
        // Persist compaction state under an incarnation-qualified name so the catalog and object
        // paths cannot alias the deleted incarnation.
        this.publisherKey = interceptor.getTopicName(partition);
        this.taskTopicName = KafkaLogNaming.logName(topicIdPartition);
        this.idForCompactionTask = kafkaSourceId(topicId, partition.partitionId());
        this.executors = executors;
        this.lastPublishedOffset = -1;
    }

    /**
     * Returns a stable identity for one Kafka topic incarnation and partition.
     *
     * <p>Partition zero deliberately retains the historical topic UUID fold so existing published-offset
     * state remains compatible. Other partitions mix their non-negative Kafka partition ID into that fold
     * so sibling partitions do not share compaction state.
     */
    static long kafkaSourceId(Uuid topicId, int partitionId) {
        if (partitionId < 0) {
            throw new IllegalArgumentException("partitionId must be non-negative");
        }
        long topicIdentity = topicId.getMostSignificantBits() ^ topicId.getLeastSignificantBits();
        return (topicIdentity ^ Integer.toUnsignedLong(partitionId)) & Long.MAX_VALUE;
    }

    public synchronized void start(Duration duration) {
        if (duration == null || duration.toSeconds() <= 0) {
            throw new IllegalArgumentException("publish interval must be at least one second");
        }
        if (closed.get()) {
            return;
        }
        this.duration = duration;
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        var seconds = duration.toSeconds();
        scheduledFuture = executors.scheduleAtFixedRate(this::publish, seconds, seconds, TimeUnit.SECONDS);
    }

    public void publish() {
        if (closed.get() || !publishRunning.compareAndSet(false, true)) {
            return;
        }
        try {
            if (!partition.isLeader()) {
                log.info("Skip publishing the compaction task for topic {} because this broker is not the leader",
                    interceptor.getTopicName(partition));
                close();
                interceptor.closeClassicPublisherIfCurrent(publisherKey, this);
                return;
            }
            if (partition.unifiedLog().isEmpty()) {
                log.warn("Unified log is not found when publishing the compaction task for the topic {}", topic);
                return;
            }
            var unifiedLog = partition.unifiedLog().get();
            var configs = interceptor.getDynamicConfigs(topic);
            if (shouldSkipPublication(configs)) {
                return;
            }

            adjustPublishInterval(configs);

            try {
                boolean ran = interceptor.runIfCurrentClassicPublisher(
                    publisherKey, this, () -> publishCurrentRange(unifiedLog, configs));
                if (ran) {
                    recordIdleTick();
                }
            } catch (PublicationFencedException error) {
                publicationFenced.set(true);
                log.warn("Compaction publication session for {} was fenced", taskTopicName, error);
                close();
            } catch (Throwable e) {
                log.error("Failed to publish the compaction task for the topic {}", topic, e);
            }
        } finally {
            publishRunning.set(false);
            if (closed.get()) {
                interceptor.closeClassicPublisherIfCurrent(publisherKey, this);
            }
        }
    }

    private boolean shouldSkipPublication(DynamicConfigs configs) {
        if (!configs.sdtEnabled()) {
            log.info("SDT is not enabled for topic {}, skip compaction task creation", topic);
            close();
            return true;
        }
        if (configs.sdtSuspended()) {
            log.info("SDT is suspended for topic {}, skip compaction task creation", topic);
            recordIdleTick();
            return true;
        }
        return false;
    }

    private void publishCurrentRange(UnifiedLog unifiedLog, DynamicConfigs configs) throws Exception {
        CompactionManager.PublicationSession session = publicationSession();
        if (session == null || !interceptor.isCurrentClassicPublisher(publisherKey, this)) {
            return;
        }
        var publishedOffset = new AtomicLong(Long.MIN_VALUE);
        CompactionManager.PublicationResult result = session.publishNext(lastOffset -> {
            lastPublishedOffset = lastOffset;
            if (!interceptor.isCurrentClassicPublisher(publisherKey, this)) {
                return Optional.empty();
            }
            long highWatermark = unifiedLog.highWatermark();
            long currentLastOffset = highWatermark - 1;
            if (currentLastOffset <= lastOffset) {
                log.info("No new data to compact for topic {}, last published offset is {}, last offset is {}",
                    taskTopicName, lastOffset, currentLastOffset);
                return Optional.empty();
            }

            var taskProperties = new HashMap<>(configs.toTaskProperties());
            taskProperties.put(UrsaSDTInterceptor.PUBLISHER_BROKER_ID_PROPERTY,
                Integer.toString(interceptor.brokerId()));
            taskProperties.put(ENTRY_FORMAT_PROPERTY, KAFKA_ENTRY_FORMAT);
            taskProperties.put(SOURCE_TOPIC_PROPERTY, publisherKey);
            taskProperties.put(SOURCE_SCHEMA_TOPIC_PROPERTY, topic);
            taskProperties.put(SOURCE_TOPIC_ID_PROPERTY, topicIdPartition.topicId().toString());

            if (!partition.isLeader() || !interceptor.isCurrentClassicPublisher(publisherKey, this)) {
                log.info("Skip publishing the compaction task for topic {} because this broker is no longer its leader",
                    taskTopicName);
                close();
                return Optional.empty();
            }
            publishedOffset.set(currentLastOffset);
            return Optional.of(PreparedCompactStreamTask.builder()
                .streamId(idForCompactionTask)
                .startOffset(lastOffset + 1)
                .endOffset(highWatermark)
                .topic(taskTopicName)
                .taskName(UUID.randomUUID().toString())
                .status(PreparedCompactStreamTask.INIT)
                .properties(taskProperties)
                .build());
        });
        if (result == CompactionManager.PublicationResult.PUBLISHED) {
            lastPublishedOffset = publishedOffset.get();
        }
    }

    private CompactionManager.PublicationSession publicationSession() throws Exception {
        CompactionManager.PublicationSession current = publicationSession.get();
        if (current != null || closed.get() || publicationFenced.get()) {
            return current;
        }
        Optional<CompactionManager.PublicationSession> opened =
            compactionManager.tryOpenPublicationSession(taskTopicName, idForCompactionTask);
        if (opened.isEmpty()) {
            return null;
        }
        CompactionManager.PublicationSession candidate = opened.get();
        if (closed.get() || publicationFenced.get()
                || !publicationSession.compareAndSet(null, candidate)) {
            interceptor.closePublicationSessionAsync(candidate);
            return closed.get() || publicationFenced.get() ? null : publicationSession.get();
        }
        if ((closed.get() || publicationFenced.get())
                && publicationSession.compareAndSet(candidate, null)) {
            interceptor.closePublicationSessionAsync(candidate);
            return null;
        }
        return candidate;
    }

    private void adjustPublishInterval(DynamicConfigs dynamicConfigs) {
        var currentSeconds = duration.getSeconds();
        var configuredSeconds = dynamicConfigs
            .getProperty("clusterTailCompactDataVisibilityIntervalInSeconds")
            .orElse(Long.toString(currentSeconds));
        long seconds;
        try {
            seconds = Long.parseLong(configuredSeconds);
        } catch (NumberFormatException error) {
            log.warn("Ignoring invalid compaction task publish interval {} for {}",
                configuredSeconds, taskTopicName);
            return;
        }
        if (seconds <= 0) {
            log.warn("Ignoring invalid compaction task publish interval {} seconds for {}", seconds, taskTopicName);
            return;
        }
        if (currentSeconds != seconds) {
            log.info("Adjust the compaction task publish interval for topic {} from {} seconds to {} seconds",
                topic, currentSeconds, seconds);
            start(Duration.ofSeconds(seconds));
        }
    }

    private void recordIdleTick() {
        if (hasPendingData()) {
            idleTicks.set(0);
            return;
        }
        if (idleTicks.incrementAndGet() >= IDLE_TICKS_BEFORE_CLOSE) {
            interceptor.closeClassicPublisherIfIdle(publisherKey, this);
        }
    }

    boolean hasPendingData() {
        return partition.unifiedLog()
            .map(log -> log.highWatermark() > lastPublishedOffset + 1)
            .orElse(false);
    }

    @Override
    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        CompactionManager.PublicationSession session = publicationSession.getAndSet(null);
        if (session != null) {
            interceptor.closePublicationSessionAsync(session);
        }
    }

    public boolean isClosed() {
        return closed.get();
    }

    boolean isPublicationFenced() {
        return publicationFenced.get();
    }

    TopicIdPartition topicIdPartition() {
        return topicIdPartition;
    }
}
