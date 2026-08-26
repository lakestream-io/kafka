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
package kafka.server.metadata;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsDelta;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.storage.diskless.DisklessProducerStateStore;
import org.apache.kafka.storage.diskless.DisklessTopicLifecycle;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Active-controller publisher for post-commit diskless topic cleanup.
 *
 * <p>Topic registration is performed by {@code DisklessTopicPreCommitHandler}. Once KRaft topic
 * deletion is committed, this publisher independently unregisters the storage catalog entry and
 * removes Kafka-owned producer-state snapshots. Both operations are best effort.
 */
public final class DisklessTopicLifecyclePublisher implements MetadataPublisher {

    private final int nodeId;
    private final AtomicBoolean isActiveController = new AtomicBoolean(false);
    private final DisklessTopicLifecycle topicLifecycle;
    private final DisklessProducerStateStore producerStateStore;
    private final BiConsumer<String, Throwable> faultHandler;
    private CompletableFuture<Void> lastOp = CompletableFuture.completedFuture(null);

    public DisklessTopicLifecyclePublisher(
            int nodeId,
            DisklessTopicLifecycle topicLifecycle,
            DisklessProducerStateStore producerStateStore,
            BiConsumer<String, Throwable> faultHandler) {
        this.nodeId = nodeId;
        this.topicLifecycle = Objects.requireNonNull(topicLifecycle, "topicLifecycle must not be null");
        this.producerStateStore = Objects.requireNonNull(producerStateStore, "producerStateStore must not be null");
        this.faultHandler = Objects.requireNonNull(faultHandler, "faultHandler must not be null");
    }

    @Override
    public String name() {
        return "DisklessTopicLifecyclePublisher id=" + nodeId;
    }

    @Override
    public void onControllerChange(LeaderAndEpoch newLeaderAndEpoch) {
        isActiveController.set(newLeaderAndEpoch.isLeader(nodeId));
    }

    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        if (!isActiveController.get()) {
            return;
        }

        TopicsDelta topicsDelta = delta.topicsDelta();
        if (topicsDelta == null || topicsDelta.deletedTopicIds().isEmpty()) {
            return;
        }

        MetadataImage oldImage = delta.image();
        String context = "MetadataDelta up to " + newImage.highestOffsetAndEpoch().offset();

        for (Uuid topicId : topicsDelta.deletedTopicIds()) {
            TopicImage oldTopicImage = oldImage.topics().getTopic(topicId);
            if (oldTopicImage == null) {
                continue;
            }
            String topicName = oldTopicImage.name();
            if (!isDisklessTopic(oldImage, topicName)) {
                continue;
            }
            enqueue(
                    "unregister diskless topic " + topicName + " (" + topicId + ")",
                    context,
                    () -> topicLifecycle.unregisterTopic(topicName, topicId));
            enqueue(
                    "delete producer-state snapshots for topic " + topicName + " (" + topicId + ")",
                    context,
                    () -> producerStateStore.deleteTopicSnapshots(topicId));
        }
    }

    @Override
    public void close() {
    }

    private synchronized void enqueue(
            String opName,
            String context,
            Supplier<CompletableFuture<Void>> operation) {
        lastOp = lastOp.handle((ignored, previousError) -> null)
                .thenCompose(ignored -> invokeBestEffort(opName, context, operation));
    }

    private CompletableFuture<Void> invokeBestEffort(
            String opName,
            String context,
            Supplier<CompletableFuture<Void>> operation) {
        try {
            CompletableFuture<Void> future = Objects.requireNonNull(
                    operation.get(), "Diskless cleanup operation returned null future");
            return future.handle((ignored, error) -> {
                if (error != null) {
                    faultHandler.accept("Failed to " + opName + " in " + context, error);
                }
                return null;
            });
        } catch (Throwable error) {
            faultHandler.accept("Failed to " + opName + " in " + context, error);
            return CompletableFuture.completedFuture(null);
        }
    }

    private boolean isDisklessTopic(MetadataImage image, String topicName) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        Map<String, String> configs = image.configs().configMapForResource(resource);
        String enabledValue = configs.get(TopicConfig.URSA_STORAGE_ENABLE_CONFIG);
        return enabledValue != null && Boolean.parseBoolean(enabledValue);
    }
}
