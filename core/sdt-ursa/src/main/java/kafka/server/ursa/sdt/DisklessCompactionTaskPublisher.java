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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.storage.diskless.handlers.KafkaLogNaming;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Objects;
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

/** Publishes end-exclusive compaction ranges for a Kafka partition stored in an Ursa log. */
final class DisklessCompactionTaskPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DisklessCompactionTaskPublisher.class);
    private static final int IDLE_TICKS_BEFORE_CLOSE = 3;
    static final String ENTRY_FORMAT_PROPERTY = "entryFormat";
    static final String URSA_ENTRY_FORMAT = "URSA";
    static final String ENTRY_SERDE_TYPE_PROPERTY = "entrySerDeType";
    static final String KAFKA_BATCHED_RAW_PARQUET = "KAFKA_BATCHED_RAW_PARQUET";
    static final String SOURCE_SCHEMA_TOPIC_PROPERTY = "sourceSchemaTopic";

    private final UrsaSDTInterceptor interceptor;
    private final CompactionManager compactionManager;
    private final TopicIdPartition topicIdPartition;
    private final String publisherKey;
    private final String taskTopicName;
    private final long streamId;
    private final ScheduledExecutorService executor;
    private final AtomicLong observedHighWatermark = new AtomicLong();
    private final AtomicLong publishedHighWatermark = new AtomicLong();
    private final AtomicInteger idleTicks = new AtomicInteger();
    private final AtomicBoolean publishRunning = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean publicationFenced = new AtomicBoolean();
    private final AtomicReference<CompactionManager.PublicationSession> publicationSession =
            new AtomicReference<>();

    private volatile Duration duration;
    private volatile ScheduledFuture<?> scheduledFuture;

    DisklessCompactionTaskPublisher(
            UrsaSDTInterceptor interceptor,
            CompactionManager compactionManager,
            TopicIdPartition topicIdPartition,
            long streamId,
            ScheduledExecutorService executor) {
        this.interceptor = interceptor;
        this.compactionManager = compactionManager;
        this.topicIdPartition = topicIdPartition;
        // Keep the name-only publisher key so a same-name Kafka topic recreation fences the old
        // publisher. Use the incarnation-qualified name only for externally persisted compaction
        // state and tasks, where the old and recreated topics must never alias.
        this.publisherKey = interceptor.getTopicName(topicIdPartition);
        this.taskTopicName = KafkaLogNaming.logName(topicIdPartition);
        this.streamId = streamId;
        this.executor = executor;
    }

    synchronized void start(Duration publishInterval) {
        Objects.requireNonNull(publishInterval, "publishInterval must not be null");
        long seconds = publishInterval.toSeconds();
        if (seconds <= 0) {
            throw new IllegalArgumentException("publishInterval must be at least one second");
        }
        if (closed.get()) {
            return;
        }
        duration = publishInterval;
        ScheduledFuture<?> previous = scheduledFuture;
        if (previous != null) {
            previous.cancel(false);
        }
        scheduledFuture = executor.scheduleAtFixedRate(this::publish, seconds, seconds, TimeUnit.SECONDS);
    }

    void observeHighWatermark(long highWatermark) {
        if (highWatermark < 0 || closed.get()) {
            return;
        }
        observedHighWatermark.accumulateAndGet(highWatermark, Math::max);
        idleTicks.set(0);
    }

    void publish() {
        if (closed.get() || !publishRunning.compareAndSet(false, true)) {
            return;
        }
        try {
            DynamicConfigs configs = interceptor.getDynamicConfigs(topicIdPartition.topic());
            if (!configs.sdtEnabled()) {
                close();
                return;
            }
            if (configs.sdtSuspended()) {
                recordIdleTick();
                return;
            }

            adjustPublishInterval(configs);
            boolean ran = interceptor.runIfCurrentDisklessPublisher(
                    publisherKey, this, () -> publishCurrentRange(configs));
            if (ran) {
                recordIdleTick();
            }
        } catch (PublicationFencedException error) {
            publicationFenced.set(true);
            log.warn("Diskless compaction publication session for {} was fenced", taskTopicName, error);
            close();
        } catch (Throwable error) {
            log.error("Failed to publish the diskless compaction task for {}", taskTopicName, error);
        } finally {
            publishRunning.set(false);
            if (closed.get()) {
                interceptor.closeDisklessPublisherIfCurrent(publisherKey, this);
            }
        }
    }

    private void publishCurrentRange(DynamicConfigs configs) throws Exception {
        CompactionManager.PublicationSession session = publicationSession();
        if (session == null || !interceptor.isCurrentDisklessPublisher(publisherKey, this)) {
            return;
        }
        var publishedEndOffset = new AtomicLong(Long.MIN_VALUE);
        CompactionManager.PublicationResult result = session.publishNext(lastPublishedOffset -> {
            publishedHighWatermark.accumulateAndGet(lastPublishedOffset + 1, Math::max);
            if (!interceptor.isCurrentDisklessPublisher(publisherKey, this)) {
                return Optional.empty();
            }
            long highWatermark = observedHighWatermark.get();
            if (highWatermark <= lastPublishedOffset + 1) {
                return Optional.empty();
            }

            var taskProperties = new HashMap<>(configs.toTaskProperties());
            taskProperties.put(UrsaSDTInterceptor.PUBLISHER_BROKER_ID_PROPERTY,
                    Integer.toString(interceptor.brokerId()));
            taskProperties.put(ENTRY_FORMAT_PROPERTY, URSA_ENTRY_FORMAT);
            taskProperties.put(ENTRY_SERDE_TYPE_PROPERTY, KAFKA_BATCHED_RAW_PARQUET);
            taskProperties.put(SOURCE_SCHEMA_TOPIC_PROPERTY, topicIdPartition.topic());

            if (!interceptor.isCurrentDisklessPublisher(publisherKey, this)) {
                return Optional.empty();
            }
            publishedEndOffset.set(highWatermark);
            return Optional.of(PreparedCompactStreamTask.builder()
                    .streamId(streamId)
                    .startOffset(lastPublishedOffset + 1)
                    .endOffset(highWatermark)
                    .topic(taskTopicName)
                    .taskName(UUID.randomUUID().toString())
                    .status(PreparedCompactStreamTask.INIT)
                    .properties(taskProperties)
                    .build());
        });
        if (result == CompactionManager.PublicationResult.PUBLISHED) {
            publishedHighWatermark.accumulateAndGet(publishedEndOffset.get(), Math::max);
        }
    }

    private CompactionManager.PublicationSession publicationSession() throws Exception {
        CompactionManager.PublicationSession current = publicationSession.get();
        if (current != null || closed.get() || publicationFenced.get()) {
            return current;
        }
        Optional<CompactionManager.PublicationSession> opened =
                compactionManager.tryOpenPublicationSession(taskTopicName, streamId);
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

    private void recordIdleTick() {
        if (hasPendingData()) {
            idleTicks.set(0);
            return;
        }
        if (idleTicks.incrementAndGet() >= IDLE_TICKS_BEFORE_CLOSE) {
            interceptor.closeDisklessPublisherIfIdle(publisherKey, this);
        }
    }

    private void adjustPublishInterval(DynamicConfigs configs) {
        long currentSeconds = duration.getSeconds();
        String configuredSeconds = configs
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
            log.warn("Ignoring invalid compaction task publish interval {} seconds for {}",
                    seconds, taskTopicName);
            return;
        }
        if (currentSeconds != seconds && !closed.get()) {
            log.info("Adjust the compaction task publish interval for {} from {} seconds to {} seconds",
                    taskTopicName, currentSeconds, seconds);
            start(Duration.ofSeconds(seconds));
        }
    }

    long streamId() {
        return streamId;
    }

    TopicIdPartition topicIdPartition() {
        return topicIdPartition;
    }

    boolean hasPendingData() {
        return observedHighWatermark.get() > publishedHighWatermark.get();
    }

    boolean isClosed() {
        return closed.get();
    }

    boolean isPublicationFenced() {
        return publicationFenced.get();
    }

    @Override
    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        ScheduledFuture<?> future = scheduledFuture;
        if (future != null) {
            future.cancel(false);
        }
        CompactionManager.PublicationSession session = publicationSession.getAndSet(null);
        if (session != null) {
            interceptor.closePublicationSessionAsync(session);
        }
    }
}
