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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Active-controller publisher for post-commit diskless topic lifecycle reconciliation.
 *
 * <p>After a diskless topic creation is committed, this publisher idempotently reconciles its
 * storage catalog registration. Once deletion is committed, it unregisters the catalog entry and
 * removes Kafka-owned producer-state snapshots. Operations for one topic ID are serialized, while
 * unrelated topics can make progress independently. Failed operations retry while their desired
 * state and the controller leadership generation remain current. A passive controller retains a
 * bounded window of deletion states for failover. Deletions older than that window cannot be
 * reconstructed from a compacted KRaft image without a durable deletion record.
 */
public final class DisklessTopicLifecyclePublisher implements MetadataPublisher {

    private static final long DEFAULT_INITIAL_RETRY_DELAY_MS = 1_000;
    private static final long DEFAULT_MAX_RETRY_DELAY_MS = 30_000;
    private static final int DEFAULT_MAX_PASSIVE_DELETION_STATES = 10_000;

    private enum DesiredStatus {
        PRESENT,
        DELETED
    }

    private record DesiredTopicState(
            long revision,
            Uuid topicId,
            String topicName,
            DesiredStatus status,
            int partitions,
            Map<String, String> configs) {
    }

    private record ScheduledOperation(long leadershipGeneration, long desiredRevision) {
    }

    private final int nodeId;
    private final DisklessTopicLifecycle topicLifecycle;
    private final DisklessProducerStateStore producerStateStore;
    private final BiConsumer<String, Throwable> faultHandler;
    private final long initialRetryDelayMs;
    private final long maxRetryDelayMs;
    private final int maxPassiveDeletionStates;
    private final ScheduledExecutorService retryExecutor;
    private final Map<Uuid, DesiredTopicState> desiredStates = new HashMap<>();
    private final Set<Uuid> passiveDeletionStates = new LinkedHashSet<>();
    private final Map<Uuid, ScheduledOperation> scheduledOperations = new HashMap<>();
    private final Map<Uuid, CompletableFuture<Void>> topicOperations = new HashMap<>();
    private final Map<Uuid, CompletableFuture<Void>> registrationOperations = new HashMap<>();
    private final Map<CompletableFuture<Void>, ScheduledFuture<?>> pendingRetryDelays = new HashMap<>();

    private boolean isActiveController = false;
    private boolean closed = false;
    private long leadershipGeneration = 0;
    private long nextDesiredRevision = 0;
    private long fullReconciliationGeneration = -1;
    private MetadataImage latestImage = null;

    public DisklessTopicLifecyclePublisher(
            int nodeId,
            DisklessTopicLifecycle topicLifecycle,
            DisklessProducerStateStore producerStateStore,
            BiConsumer<String, Throwable> faultHandler) {
        this(
                nodeId,
                topicLifecycle,
                producerStateStore,
                faultHandler,
                DEFAULT_INITIAL_RETRY_DELAY_MS,
                DEFAULT_MAX_RETRY_DELAY_MS,
                DEFAULT_MAX_PASSIVE_DELETION_STATES);
    }

    DisklessTopicLifecyclePublisher(
            int nodeId,
            DisklessTopicLifecycle topicLifecycle,
            DisklessProducerStateStore producerStateStore,
            BiConsumer<String, Throwable> faultHandler,
            long initialRetryDelayMs,
            long maxRetryDelayMs) {
        this(
                nodeId,
                topicLifecycle,
                producerStateStore,
                faultHandler,
                initialRetryDelayMs,
                maxRetryDelayMs,
                DEFAULT_MAX_PASSIVE_DELETION_STATES);
    }

    DisklessTopicLifecyclePublisher(
            int nodeId,
            DisklessTopicLifecycle topicLifecycle,
            DisklessProducerStateStore producerStateStore,
            BiConsumer<String, Throwable> faultHandler,
            long initialRetryDelayMs,
            long maxRetryDelayMs,
            int maxPassiveDeletionStates) {
        this.nodeId = nodeId;
        this.topicLifecycle = Objects.requireNonNull(topicLifecycle, "topicLifecycle must not be null");
        this.producerStateStore = Objects.requireNonNull(producerStateStore, "producerStateStore must not be null");
        this.faultHandler = Objects.requireNonNull(faultHandler, "faultHandler must not be null");
        if (initialRetryDelayMs < 1) {
            throw new IllegalArgumentException("initialRetryDelayMs must be at least 1");
        }
        if (maxRetryDelayMs < initialRetryDelayMs) {
            throw new IllegalArgumentException("maxRetryDelayMs must not be less than initialRetryDelayMs");
        }
        if (maxPassiveDeletionStates < 1) {
            throw new IllegalArgumentException("maxPassiveDeletionStates must be at least 1");
        }
        this.initialRetryDelayMs = initialRetryDelayMs;
        this.maxRetryDelayMs = maxRetryDelayMs;
        this.maxPassiveDeletionStates = maxPassiveDeletionStates;
        this.retryExecutor = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "diskless-topic-lifecycle-publisher-" + nodeId + "-retry");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public String name() {
        return "DisklessTopicLifecyclePublisher id=" + nodeId;
    }

    @Override
    public void onControllerChange(LeaderAndEpoch newLeaderAndEpoch) {
        List<CompletableFuture<Void>> delaysToComplete;
        MetadataImage imageToReconcile = null;
        long generationToReconcile = -1;
        synchronized (this) {
            isActiveController = false;
            leadershipGeneration++;
            fullReconciliationGeneration = -1;
            scheduledOperations.clear();
            delaysToComplete = cancelPendingRetryDelays();
            if (!closed && newLeaderAndEpoch.isLeader(nodeId)) {
                isActiveController = true;
                passiveDeletionStates.clear();
                generationToReconcile = leadershipGeneration;
                if (latestImage == null) {
                    fullReconciliationGeneration = leadershipGeneration;
                } else {
                    imageToReconcile = latestImage;
                }
            } else if (!closed) {
                rememberDeletionsForPassiveController();
            }
        }
        delaysToComplete.forEach(delay -> delay.complete(null));
        if (imageToReconcile != null) {
            reconcileFullImage(imageToReconcile, generationToReconcile);
        }
    }

    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        long generation;
        boolean reconcileFullImage;
        synchronized (this) {
            latestImage = newImage;
            generation = activeLeadershipGeneration();
            reconcileFullImage = generation >= 0 && fullReconciliationGeneration == generation;
            if (reconcileFullImage) {
                fullReconciliationGeneration = -1;
            }
        }

        if (reconcileFullImage) {
            reconcileFullImage(newImage, generation);
        }

        TopicsDelta topicsDelta = delta.topicsDelta();
        if (topicsDelta == null) {
            return;
        }

        MetadataImage oldImage = delta.image();
        String context = "MetadataDelta up to " + newImage.highestOffsetAndEpoch().offset();
        reconcileDeletedTopics(topicsDelta, oldImage, context, generation);
        reconcileCreatedTopics(topicsDelta, newImage, context, generation);
        reconcilePartitionCountChanges(topicsDelta, oldImage, newImage, context, generation);
    }

    private void reconcileDeletedTopics(
            TopicsDelta topicsDelta,
            MetadataImage oldImage,
            String context,
            long generation) {
        for (Uuid topicId : topicsDelta.deletedTopicIds()) {
            TopicImage oldTopicImage = oldImage.topics().getTopic(topicId);
            if (oldTopicImage == null || !isDisklessTopic(oldImage, oldTopicImage.name())) {
                continue;
            }
            DesiredTopicState desired = rememberDeletedTopic(topicId, oldTopicImage.name());
            scheduleDeletionIfActive(desired, context, generation);
        }
    }

    private void reconcileCreatedTopics(
            TopicsDelta topicsDelta,
            MetadataImage newImage,
            String context,
            long generation) {
        for (Uuid topicId : topicsDelta.createdTopicIds()) {
            TopicImage topicImage = newImage.topics().getTopic(topicId);
            if (topicImage == null) {
                continue;
            }
            Map<String, String> configs = topicConfigs(newImage, topicImage.name());
            if (!isDisklessTopic(configs)) {
                continue;
            }
            DesiredTopicState desired = rememberPresentTopic(topicImage, configs);
            scheduleRegistrationIfActive(desired, context, generation);
        }
    }

    private void reconcilePartitionCountChanges(
            TopicsDelta topicsDelta,
            MetadataImage oldImage,
            MetadataImage newImage,
            String context,
            long generation) {
        Set<Uuid> createdTopicIds = Set.copyOf(topicsDelta.createdTopicIds());
        for (Uuid topicId : topicsDelta.changedTopics().keySet()) {
            if (createdTopicIds.contains(topicId)) {
                continue;
            }
            TopicImage oldTopicImage = oldImage.topics().getTopic(topicId);
            TopicImage topicImage = newImage.topics().getTopic(topicId);
            if (oldTopicImage == null
                    || topicImage == null
                    || oldTopicImage.partitions().size() == topicImage.partitions().size()) {
                continue;
            }
            Map<String, String> configs = topicConfigs(newImage, topicImage.name());
            if (!isDisklessTopic(configs)) {
                continue;
            }
            DesiredTopicState desired = rememberPresentTopic(topicImage, configs);
            scheduleRegistrationIfActive(desired, context, generation);
        }
    }

    @Override
    public void close() {
        List<CompletableFuture<Void>> delaysToComplete;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            isActiveController = false;
            leadershipGeneration++;
            fullReconciliationGeneration = -1;
            scheduledOperations.clear();
            delaysToComplete = cancelPendingRetryDelays();
            retryExecutor.shutdownNow();
            desiredStates.clear();
            passiveDeletionStates.clear();
        }
        delaysToComplete.forEach(delay -> delay.complete(null));
    }

    private void reconcileFullImage(MetadataImage image, long generation) {
        String context = "full MetadataImage at " + image.highestOffsetAndEpoch().offset();
        Map<Uuid, TopicImage> currentDisklessTopics = new HashMap<>();
        for (TopicImage topicImage : image.topics().topicsById().values()) {
            Map<String, String> configs = topicConfigs(image, topicImage.name());
            if (!isDisklessTopic(configs)) {
                continue;
            }
            currentDisklessTopics.put(topicImage.id(), topicImage);
            DesiredTopicState desired = rememberPresentTopicForReconciliation(
                    image, generation, topicImage, configs);
            if (desired == null) {
                reconcileNewerImage(image, generation);
                return;
            }
            scheduleRegistrationIfActive(desired, context, generation);
        }

        if (!isReconciliationImageCurrent(image, generation)) {
            reconcileNewerImage(image, generation);
            return;
        }
        List<DesiredTopicState> rememberedStates;
        synchronized (this) {
            rememberedStates = new ArrayList<>(desiredStates.values());
        }
        for (DesiredTopicState desired : rememberedStates) {
            if (desired.status() == DesiredStatus.PRESENT
                    && !currentDisklessTopics.containsKey(desired.topicId())) {
                desired = rememberDeletedTopic(desired.topicId(), desired.topicName());
            }
            if (desired.status() == DesiredStatus.DELETED) {
                scheduleDeletionIfActive(desired, context, generation);
            }
        }
    }

    private DesiredTopicState rememberPresentTopic(TopicImage topicImage, Map<String, String> configs) {
        Map<String, String> immutableConfigs = Map.copyOf(configs);
        synchronized (this) {
            passiveDeletionStates.remove(topicImage.id());
            DesiredTopicState current = desiredStates.get(topicImage.id());
            if (current != null
                    && current.status() == DesiredStatus.PRESENT
                    && current.topicName().equals(topicImage.name())
                    && current.partitions() == topicImage.partitions().size()
                    && current.configs().equals(immutableConfigs)) {
                return current;
            }
            DesiredTopicState updated = new DesiredTopicState(
                    ++nextDesiredRevision,
                    topicImage.id(),
                    topicImage.name(),
                    DesiredStatus.PRESENT,
                    topicImage.partitions().size(),
                    immutableConfigs);
            desiredStates.put(topicImage.id(), updated);
            return updated;
        }
    }

    private synchronized DesiredTopicState rememberPresentTopicForReconciliation(
            MetadataImage image,
            long generation,
            TopicImage topicImage,
            Map<String, String> configs) {
        if (!isReconciliationImageCurrent(image, generation)) {
            return null;
        }
        return rememberPresentTopic(topicImage, configs);
    }

    private void reconcileNewerImage(MetadataImage previousImage, long generation) {
        MetadataImage newerImage;
        synchronized (this) {
            newerImage = isActiveController
                    && !closed
                    && leadershipGeneration == generation
                    && latestImage != previousImage
                    ? latestImage
                    : null;
        }
        if (newerImage != null) {
            reconcileFullImage(newerImage, generation);
        }
    }

    private synchronized boolean isReconciliationImageCurrent(MetadataImage image, long generation) {
        return latestImage == image && activeLeadershipGeneration() == generation;
    }

    private DesiredTopicState rememberDeletedTopic(Uuid topicId, String topicName) {
        synchronized (this) {
            DesiredTopicState current = desiredStates.get(topicId);
            if (current != null
                    && current.status() == DesiredStatus.DELETED
                    && current.topicName().equals(topicName)) {
                return current;
            }
            DesiredTopicState updated = new DesiredTopicState(
                    ++nextDesiredRevision,
                    topicId,
                    topicName,
                    DesiredStatus.DELETED,
                    0,
                    Map.of());
            desiredStates.put(topicId, updated);
            if (!isActiveController) {
                passiveDeletionStates.add(topicId);
                trimPassiveDeletionStates();
            }
            return updated;
        }
    }

    private void scheduleRegistrationIfActive(
            DesiredTopicState desired,
            String context,
            long generation) {
        ScheduledOperation scheduled = claimOperation(desired, generation);
        if (scheduled == null) {
            return;
        }
        enqueueTopicOperation(desired.topicId(), scheduled, () -> retryUntilSuccess(
                "register diskless topic " + desired.topicName() + " (" + desired.topicId() + ")",
                context,
                desired,
                scheduled,
                () -> topicLifecycle.registerTopic(
                        desired.topicName(),
                        desired.topicId(),
                        desired.partitions(),
                        desired.configs()),
                1,
                initialRetryDelayMs).thenApply(succeeded -> null));
    }

    private void scheduleDeletionIfActive(
            DesiredTopicState desired,
            String context,
            long generation) {
        ScheduledOperation scheduled = claimOperation(desired, generation);
        if (scheduled == null) {
            return;
        }
        startSupersedingDeletion(desired, scheduled, () -> retryUntilSuccess(
                "unregister diskless topic " + desired.topicName() + " (" + desired.topicId() + ")",
                context,
                desired,
                scheduled,
                () -> topicLifecycle.unregisterTopic(desired.topicName(), desired.topicId()),
                1,
                initialRetryDelayMs).thenCombine(
                        retryUntilSuccess(
                                "delete producer-state snapshots for topic "
                                        + desired.topicName() + " (" + desired.topicId() + ")",
                                context,
                                desired,
                                scheduled,
                                () -> producerStateStore.deleteTopicSnapshots(desired.topicId()),
                                1,
                                initialRetryDelayMs),
                        (unregistered, snapshotsDeleted) -> unregistered && snapshotsDeleted));
    }

    private synchronized ScheduledOperation claimOperation(DesiredTopicState desired, long generation) {
        if (!isCurrentDesiredState(desired, generation)) {
            return null;
        }
        ScheduledOperation operation = new ScheduledOperation(generation, desired.revision());
        if (operation.equals(scheduledOperations.get(desired.topicId()))) {
            return null;
        }
        scheduledOperations.put(desired.topicId(), operation);
        return operation;
    }

    private synchronized void enqueueTopicOperation(
            Uuid topicId,
            ScheduledOperation scheduled,
            Supplier<CompletableFuture<Void>> operation) {
        CompletableFuture<Void> previous = topicOperations.getOrDefault(
                topicId, CompletableFuture.completedFuture(null));
        CompletableFuture<Void> next = previous.handle((ignored, previousError) -> null)
                .thenCompose(ignored -> {
                    if (!isScheduledOperationCurrent(topicId, scheduled)) {
                        return CompletableFuture.completedFuture(null);
                    }
                    try {
                        return Objects.requireNonNull(
                                operation.get(), "Diskless topic lifecycle workflow returned null future");
                    } catch (Throwable error) {
                        reportFailure("run diskless topic lifecycle workflow for " + topicId, "", error, 1);
                        return CompletableFuture.completedFuture(null);
                    }
                });
        topicOperations.put(topicId, next);
        registrationOperations.put(topicId, next);
        next.whenComplete((ignored, error) -> {
            removeCompletedRegistrationOperation(topicId, next);
            removeCompletedTopicOperation(topicId, next);
        });
    }

    private synchronized void startSupersedingDeletion(
            DesiredTopicState desired,
            ScheduledOperation scheduled,
            Supplier<CompletableFuture<Boolean>> operation) {
        Uuid topicId = desired.topicId();
        if (!isScheduledOperationCurrent(topicId, scheduled)) {
            return;
        }
        CompletableFuture<Void> previousRegistration = registrationOperations.get(topicId);
        CompletableFuture<Boolean> deletion = startDeletionWorkflow(topicId, operation);
        if (previousRegistration != null && !previousRegistration.isDone()) {
            deletion = deletion.thenCombine(
                    previousRegistration.handle((ignored, previousError) -> null),
                    (deleted, ignored) -> deleted)
                .thenCompose(deleted -> deleted
                        ? startDeletionWorkflow(topicId, operation)
                        : CompletableFuture.completedFuture(false));
        }
        CompletableFuture<Void> next = deletion.thenAccept(deleted -> {
            if (deleted) {
                forgetCompletedDeletion(desired);
            }
        });
        topicOperations.put(topicId, next);
        next.whenComplete((ignored, error) -> removeCompletedTopicOperation(topicId, next));
    }

    private synchronized void removeCompletedRegistrationOperation(
            Uuid topicId,
            CompletableFuture<Void> completed) {
        if (registrationOperations.get(topicId) == completed) {
            registrationOperations.remove(topicId);
        }
    }

    private CompletableFuture<Boolean> startDeletionWorkflow(
            Uuid topicId,
            Supplier<CompletableFuture<Boolean>> operation) {
        try {
            return Objects.requireNonNull(
                    operation.get(), "Diskless topic lifecycle workflow returned null future");
        } catch (Throwable error) {
            reportFailure("run diskless topic lifecycle workflow for " + topicId, "", error, 1);
            return CompletableFuture.completedFuture(false);
        }
    }

    private CompletableFuture<Boolean> retryUntilSuccess(
            String opName,
            String context,
            DesiredTopicState desired,
            ScheduledOperation scheduled,
            Supplier<CompletableFuture<Void>> operation,
            long attempt,
            long retryDelayMs) {
        CompletableFuture<Void> attemptFuture;
        synchronized (this) {
            if (!isCurrentDesiredState(desired, scheduled.leadershipGeneration())) {
                return CompletableFuture.completedFuture(false);
            }
            try {
                attemptFuture = Objects.requireNonNull(
                        operation.get(), "Diskless lifecycle operation returned null future");
            } catch (Throwable error) {
                attemptFuture = CompletableFuture.failedFuture(error);
            }
        }

        return attemptFuture.handle((ignored, error) -> {
            if (error == null) {
                return CompletableFuture.completedFuture(true);
            }
            if (!isCurrentDesiredState(desired, scheduled.leadershipGeneration())) {
                return CompletableFuture.completedFuture(false);
            }
            reportFailure(opName, context, unwrap(error), attempt);
            return delayBeforeRetry(desired, scheduled, retryDelayMs)
                    .thenCompose(delayed -> retryUntilSuccess(
                            opName,
                            context,
                            desired,
                            scheduled,
                            operation,
                            attempt + 1,
                            nextRetryDelayMs(retryDelayMs)));
        }).thenCompose(result -> result);
    }

    private CompletableFuture<Void> delayBeforeRetry(
            DesiredTopicState desired,
            ScheduledOperation scheduled,
            long delayMs) {
        CompletableFuture<Void> delay = new CompletableFuture<>();
        synchronized (this) {
            if (!isCurrentDesiredState(desired, scheduled.leadershipGeneration())) {
                return CompletableFuture.completedFuture(null);
            }
            ScheduledFuture<?> scheduledDelay = retryExecutor.schedule(
                    () -> completeRetryDelay(delay), delayMs, TimeUnit.MILLISECONDS);
            pendingRetryDelays.put(delay, scheduledDelay);
        }
        return delay;
    }

    private long nextRetryDelayMs(long currentDelayMs) {
        return currentDelayMs > maxRetryDelayMs / 2
                ? maxRetryDelayMs
                : Math.min(maxRetryDelayMs, currentDelayMs * 2);
    }

    private void completeRetryDelay(CompletableFuture<Void> delay) {
        synchronized (this) {
            pendingRetryDelays.remove(delay);
        }
        delay.complete(null);
    }

    private synchronized List<CompletableFuture<Void>> cancelPendingRetryDelays() {
        List<CompletableFuture<Void>> delays = new ArrayList<>(pendingRetryDelays.keySet());
        pendingRetryDelays.values().forEach(scheduled -> scheduled.cancel(false));
        pendingRetryDelays.clear();
        return delays;
    }

    private synchronized long activeLeadershipGeneration() {
        return !closed && isActiveController ? leadershipGeneration : -1;
    }

    private synchronized boolean isCurrentDesiredState(DesiredTopicState desired, long generation) {
        return !closed
                && isActiveController
                && leadershipGeneration == generation
                && desiredStates.get(desired.topicId()) == desired;
    }

    private synchronized boolean isScheduledOperationCurrent(Uuid topicId, ScheduledOperation scheduled) {
        return !closed
                && isActiveController
                && leadershipGeneration == scheduled.leadershipGeneration()
                && scheduled.equals(scheduledOperations.get(topicId));
    }

    private synchronized void removeCompletedTopicOperation(
            Uuid topicId,
            CompletableFuture<Void> completed) {
        if (topicOperations.get(topicId) == completed) {
            topicOperations.remove(topicId);
            DesiredTopicState desired = desiredStates.get(topicId);
            if (!closed
                    && !isActiveController
                    && desired != null
                    && desired.status() == DesiredStatus.DELETED) {
                passiveDeletionStates.add(topicId);
                trimPassiveDeletionStates();
            }
        }
    }

    private synchronized void forgetCompletedDeletion(DesiredTopicState desired) {
        if (desiredStates.get(desired.topicId()) == desired) {
            desiredStates.remove(desired.topicId());
            passiveDeletionStates.remove(desired.topicId());
            scheduledOperations.remove(desired.topicId());
        }
    }

    private void rememberDeletionsForPassiveController() {
        for (DesiredTopicState desired : desiredStates.values()) {
            if (desired.status() == DesiredStatus.DELETED
                    && !topicOperations.containsKey(desired.topicId())) {
                passiveDeletionStates.add(desired.topicId());
            }
        }
        trimPassiveDeletionStates();
    }

    private void trimPassiveDeletionStates() {
        while (passiveDeletionStates.size() > maxPassiveDeletionStates) {
            Uuid topicId = passiveDeletionStates.iterator().next();
            passiveDeletionStates.remove(topicId);
            DesiredTopicState desired = desiredStates.get(topicId);
            if (desired != null && desired.status() == DesiredStatus.DELETED) {
                desiredStates.remove(topicId);
                scheduledOperations.remove(topicId);
            }
        }
    }

    private void reportFailure(
            String opName,
            String context,
            Throwable error,
            long attempt) {
        try {
            String suffix = context.isEmpty() ? "" : " in " + context;
            faultHandler.accept("Failed to " + opName + suffix + " (attempt " + attempt + ")", error);
        } catch (Throwable ignored) {
            // A fault handler failure must not stall subsequent lifecycle reconciliation.
        }
    }

    private boolean isDisklessTopic(MetadataImage image, String topicName) {
        return isDisklessTopic(topicConfigs(image, topicName));
    }

    private Map<String, String> topicConfigs(MetadataImage image, String topicName) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        return Map.copyOf(image.configs().configMapForResource(resource));
    }

    private boolean isDisklessTopic(Map<String, String> configs) {
        return Boolean.parseBoolean(configs.get(TopicConfig.URSA_STORAGE_ENABLE_CONFIG));
    }

    private static Throwable unwrap(Throwable error) {
        if (error instanceof CompletionException && error.getCause() != null) {
            return error.getCause();
        }
        return error;
    }
}
