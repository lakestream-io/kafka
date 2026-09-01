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
import org.apache.kafka.image.ConfigurationsDelta;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsDelta;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.storage.diskless.DisklessProducerStateStore;
import org.apache.kafka.storage.diskless.DisklessProducerStateStore.ManagedProducerStateTopic;
import org.apache.kafka.storage.diskless.DisklessTopicLifecycle;
import org.apache.kafka.storage.diskless.DisklessTopicLifecycle.ManagedTopic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
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
 * bounded window of deletion states for fast failover; after activation, a full KRaft image plus
 * the storage-owned lifecycle inventory also reconciles older or non-terminal catalog orphans.
 */
public final class DisklessTopicLifecyclePublisher implements MetadataPublisher {

    private static final long DEFAULT_INITIAL_RETRY_DELAY_MS = 1_000;
    private static final long DEFAULT_MAX_RETRY_DELAY_MS = 30_000;
    private static final int DEFAULT_MAX_PASSIVE_DELETION_STATES = 10_000;
    private static final long DEFAULT_MANAGED_TOPIC_INVENTORY_INTERVAL_MS = 30_000;
    private static final long DEFAULT_MANAGED_TOPIC_INVENTORY_TIMEOUT_MS = 10_000;

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
            Map<String, String> configs,
            long sourceRevision,
            boolean deleteCatalog,
            boolean deleteProducerState) {
    }

    private record ScheduledOperation(long leadershipGeneration, long desiredRevision) {
    }

    private static final class RetryDelay {
        private ScheduledFuture<?> future;
    }

    private static final class RetryingOperation {
        private final String name;
        private final Supplier<CompletableFuture<Void>> supplier;
        private long attempt = 1;
        private long retryDelayMs;
        private CompletableFuture<Void> source;
        private RetryDelay retryDelay;
        private boolean completed;

        private RetryingOperation(
                String name,
                Supplier<CompletableFuture<Void>> supplier,
                long retryDelayMs) {
            this.name = name;
            this.supplier = supplier;
            this.retryDelayMs = retryDelayMs;
        }
    }

    private static final class TopicOperationRunner {
        private final Uuid topicId;
        private final long leadershipGeneration;
        private DesiredTopicState currentDesired;
        private String currentContext;
        private DesiredTopicState pendingDesired;
        private String pendingContext;
        private long workflowGeneration;
        private List<RetryingOperation> operations = List.of();

        private TopicOperationRunner(Uuid topicId, long leadershipGeneration) {
            this.topicId = topicId;
            this.leadershipGeneration = leadershipGeneration;
        }
    }

    private record PendingCancellation(
            List<CompletableFuture<?>> sources,
            List<ScheduledFuture<?>> delays) {

        private static final PendingCancellation NONE =
                new PendingCancellation(List.of(), List.of());

        private PendingCancellation plus(PendingCancellation other) {
            if (sources.isEmpty() && delays.isEmpty()) {
                return other;
            }
            if (other.sources.isEmpty() && other.delays.isEmpty()) {
                return this;
            }
            List<CompletableFuture<?>> combinedSources = new ArrayList<>(sources);
            combinedSources.addAll(other.sources);
            List<ScheduledFuture<?>> combinedDelays = new ArrayList<>(delays);
            combinedDelays.addAll(other.delays);
            return new PendingCancellation(combinedSources, combinedDelays);
        }
    }

    private record PendingTimedOperation(
            CompletableFuture<?> source,
            CompletableFuture<?> timed,
            ScheduledFuture<?> timeout) {
    }

    private final int nodeId;
    private final DisklessTopicLifecycle topicLifecycle;
    private final DisklessProducerStateStore producerStateStore;
    private final BiConsumer<String, Throwable> faultHandler;
    private final long initialRetryDelayMs;
    private final long maxRetryDelayMs;
    private final int maxPassiveDeletionStates;
    private final long managedTopicInventoryIntervalMs;
    private final long managedTopicInventoryTimeoutMs;
    private final ScheduledExecutorService retryExecutor;
    private final Map<Uuid, DesiredTopicState> desiredStates = new HashMap<>();
    private final Set<Uuid> passiveDeletionStates = new LinkedHashSet<>();
    private final Set<Uuid> protectedPassiveDeletionStates = new LinkedHashSet<>();
    private final Map<Uuid, ScheduledOperation> scheduledOperations = new HashMap<>();
    private final Map<Uuid, TopicOperationRunner> topicRunners = new HashMap<>();
    private final Map<CompletableFuture<Void>, ScheduledFuture<?>> pendingRetryDelays = new HashMap<>();
    private final Map<CompletableFuture<?>, PendingTimedOperation> pendingTimedOperations = new HashMap<>();

    private boolean isActiveController = false;
    private boolean closed = false;
    private long leadershipGeneration = 0;
    private long nextDesiredRevision = 0;
    private long fullReconciliationGeneration = -1;
    private long managedTopicInventoryCycle = 0;
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
        this(
                nodeId,
                topicLifecycle,
                producerStateStore,
                faultHandler,
                initialRetryDelayMs,
                maxRetryDelayMs,
                maxPassiveDeletionStates,
                DEFAULT_MANAGED_TOPIC_INVENTORY_INTERVAL_MS,
                DEFAULT_MANAGED_TOPIC_INVENTORY_TIMEOUT_MS);
    }

    DisklessTopicLifecyclePublisher(
            int nodeId,
            DisklessTopicLifecycle topicLifecycle,
            DisklessProducerStateStore producerStateStore,
            BiConsumer<String, Throwable> faultHandler,
            long initialRetryDelayMs,
            long maxRetryDelayMs,
            int maxPassiveDeletionStates,
            long managedTopicInventoryIntervalMs,
            long managedTopicInventoryTimeoutMs) {
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
        if (managedTopicInventoryIntervalMs < 1) {
            throw new IllegalArgumentException("managedTopicInventoryIntervalMs must be at least 1");
        }
        if (managedTopicInventoryTimeoutMs < 1) {
            throw new IllegalArgumentException("managedTopicInventoryTimeoutMs must be at least 1");
        }
        this.initialRetryDelayMs = initialRetryDelayMs;
        this.maxRetryDelayMs = maxRetryDelayMs;
        this.maxPassiveDeletionStates = maxPassiveDeletionStates;
        this.managedTopicInventoryIntervalMs = managedTopicInventoryIntervalMs;
        this.managedTopicInventoryTimeoutMs = managedTopicInventoryTimeoutMs;
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
        List<PendingTimedOperation> timedOperationsToCancel;
        PendingCancellation topicOperationsToCancel;
        MetadataImage imageToReconcile = null;
        long generationToReconcile = -1;
        synchronized (this) {
            isActiveController = false;
            leadershipGeneration++;
            fullReconciliationGeneration = -1;
            managedTopicInventoryCycle++;
            scheduledOperations.clear();
            delaysToComplete = cancelPendingRetryDelays();
            timedOperationsToCancel = removePendingTimedOperations();
            if (!closed && !newLeaderAndEpoch.isLeader(nodeId)) {
                protectInFlightDeletionsForPassiveController();
            }
            topicOperationsToCancel = removeTopicRunners();
            if (!closed && newLeaderAndEpoch.isLeader(nodeId)) {
                isActiveController = true;
                passiveDeletionStates.clear();
                protectedPassiveDeletionStates.clear();
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
        cancelTimedOperations(timedOperationsToCancel);
        cancelPendingOperations(topicOperationsToCancel);
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

        MetadataImage oldImage = delta.image();
        long sourceRevision = newImage.highestOffsetAndEpoch().offset();
        String context = "MetadataDelta up to " + newImage.highestOffsetAndEpoch().offset();
        TopicsDelta topicsDelta = delta.topicsDelta();
        if (topicsDelta != null) {
            reconcileDeletedTopics(topicsDelta, oldImage, context, generation);
            reconcileCreatedTopics(topicsDelta, newImage, context, generation, sourceRevision);
            reconcilePartitionCountChanges(
                    topicsDelta, oldImage, newImage, context, generation, sourceRevision);
        }
        reconcileConfigChanges(delta.configsDelta(), newImage, context, generation, sourceRevision);
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
            long generation,
            long sourceRevision) {
        for (Uuid topicId : topicsDelta.createdTopicIds()) {
            TopicImage topicImage = newImage.topics().getTopic(topicId);
            if (topicImage == null) {
                continue;
            }
            Map<String, String> configs = topicConfigs(newImage, topicImage.name());
            if (!isDisklessTopic(configs)) {
                continue;
            }
            DesiredTopicState desired = rememberPresentTopic(topicImage, configs, sourceRevision);
            scheduleRegistrationIfActive(desired, context, generation);
        }
    }

    private void reconcilePartitionCountChanges(
            TopicsDelta topicsDelta,
            MetadataImage oldImage,
            MetadataImage newImage,
            String context,
            long generation,
            long sourceRevision) {
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
            DesiredTopicState desired = rememberPresentTopic(topicImage, configs, sourceRevision);
            scheduleRegistrationIfActive(desired, context, generation);
        }
    }

    private void reconcileConfigChanges(
            ConfigurationsDelta configurationsDelta,
            MetadataImage newImage,
            String context,
            long generation,
            long sourceRevision) {
        if (configurationsDelta == null) {
            return;
        }
        for (ConfigResource resource : configurationsDelta.changes().keySet()) {
            if (resource.type() != ConfigResource.Type.TOPIC) {
                continue;
            }
            TopicImage topicImage = newImage.topics().getTopic(resource.name());
            if (topicImage == null) {
                continue;
            }
            Map<String, String> configs = topicConfigs(newImage, topicImage.name());
            if (!isDisklessTopic(configs)) {
                continue;
            }
            DesiredTopicState desired = rememberPresentTopic(topicImage, configs, sourceRevision);
            scheduleRegistrationIfActive(desired, context, generation);
        }
    }

    @Override
    public void close() {
        List<CompletableFuture<Void>> delaysToComplete;
        List<PendingTimedOperation> timedOperationsToCancel;
        PendingCancellation topicOperationsToCancel;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            isActiveController = false;
            leadershipGeneration++;
            fullReconciliationGeneration = -1;
            managedTopicInventoryCycle++;
            scheduledOperations.clear();
            delaysToComplete = cancelPendingRetryDelays();
            timedOperationsToCancel = removePendingTimedOperations();
            topicOperationsToCancel = removeTopicRunners();
            retryExecutor.shutdownNow();
            desiredStates.clear();
            passiveDeletionStates.clear();
            protectedPassiveDeletionStates.clear();
        }
        delaysToComplete.forEach(delay -> delay.complete(null));
        cancelTimedOperations(timedOperationsToCancel);
        cancelPendingOperations(topicOperationsToCancel);
    }

    private void reconcileFullImage(MetadataImage image, long generation) {
        String context = "full MetadataImage at " + image.highestOffsetAndEpoch().offset();
        long sourceRevision = image.highestOffsetAndEpoch().offset();
        Map<Uuid, TopicImage> currentDisklessTopics = new HashMap<>();
        for (TopicImage topicImage : image.topics().topicsById().values()) {
            Map<String, String> configs = topicConfigs(image, topicImage.name());
            if (!isDisklessTopic(configs)) {
                continue;
            }
            currentDisklessTopics.put(topicImage.id(), topicImage);
        }

        List<DesiredTopicState> registrations = new ArrayList<>();
        List<DesiredTopicState> deletions = new ArrayList<>();
        boolean imageIsCurrent;
        synchronized (this) {
            imageIsCurrent = isReconciliationImageCurrent(image, generation);
            if (imageIsCurrent) {
                for (TopicImage topicImage : currentDisklessTopics.values()) {
                    registrations.add(rememberPresentTopicFromFullImage(
                            topicImage,
                            topicConfigs(image, topicImage.name()),
                            sourceRevision));
                }
                List<DesiredTopicState> rememberedStates = new ArrayList<>(desiredStates.values());
                for (DesiredTopicState desired : rememberedStates) {
                    if (desired.status() == DesiredStatus.PRESENT
                            && desired.sourceRevision() <= sourceRevision
                            && !currentDisklessTopics.containsKey(desired.topicId())) {
                        desired = rememberDeletedTopic(desired.topicId(), desired.topicName());
                    }
                    if (desired.status() == DesiredStatus.DELETED) {
                        deletions.add(desired);
                    }
                }
            }
        }
        if (!imageIsCurrent) {
            reconcileNewerImage(image, generation);
            return;
        }
        for (DesiredTopicState desired : registrations) {
            scheduleRegistrationIfActive(desired, context, generation);
        }
        for (DesiredTopicState desired : deletions) {
            scheduleDeletionIfActive(desired, context, generation);
        }
        startManagedTopicOrphanReconciliation(
                image,
                generation,
                Set.copyOf(currentDisklessTopics.keySet()),
                context);
    }

    private void startManagedTopicOrphanReconciliation(
            MetadataImage image,
            long generation,
            Set<Uuid> currentDisklessTopicIds,
            String context) {
        long cycle;
        List<PendingTimedOperation> timedOperationsToCancel;
        synchronized (this) {
            if (!isReconciliationImageCurrent(image, generation)) {
                cycle = -1;
                timedOperationsToCancel = List.of();
            } else {
                cycle = ++managedTopicInventoryCycle;
                timedOperationsToCancel = removePendingTimedOperations();
            }
        }
        cancelTimedOperations(timedOperationsToCancel);
        if (cycle < 0) {
            reconcileNewerImage(image, generation);
            return;
        }
        reconcileManagedTopicOrphans(
                image,
                generation,
                cycle,
                currentDisklessTopicIds,
                context,
                1,
                initialRetryDelayMs);
    }

    private void reconcileManagedTopicOrphans(
            MetadataImage image,
            long generation,
            long cycle,
            Set<Uuid> currentDisklessTopicIds,
            String context,
            long attempt,
            long retryDelayMs) {
        if (!isManagedTopicInventoryCurrent(image, generation, cycle)) {
            reconcileManagedTopicInventoryNewerImage(image, generation, cycle);
            return;
        }

        CompletableFuture<List<ManagedProducerStateTopic>> producerStateInventoryFuture;
        try {
            producerStateInventoryFuture = Objects.requireNonNull(
                    producerStateStore.listManagedTopics(),
                    "Diskless producer-state inventory returned null future");
        } catch (Throwable error) {
            producerStateInventoryFuture = CompletableFuture.failedFuture(error);
        }
        CompletableFuture<List<ManagedProducerStateTopic>> timedProducerStateInventory = withTimeout(
                producerStateInventoryFuture,
                managedTopicInventoryTimeoutMs,
                "Timed out listing managed diskless producer-state topics");
        timedProducerStateInventory.whenComplete((managedProducerStateTopics, producerStateError) -> {
            if (producerStateError != null) {
                retryManagedTopicInventory(
                        image,
                        generation,
                        cycle,
                        currentDisklessTopicIds,
                        context,
                        attempt,
                        retryDelayMs,
                        unwrap(producerStateError));
                return;
            }
            listCatalogInventoryAfterProducerState(
                    image,
                    generation,
                    cycle,
                    currentDisklessTopicIds,
                    context,
                    attempt,
                    retryDelayMs,
                    managedProducerStateTopics);
        });
    }

    private void listCatalogInventoryAfterProducerState(
            MetadataImage image,
            long generation,
            long cycle,
            Set<Uuid> currentDisklessTopicIds,
            String context,
            long attempt,
            long retryDelayMs,
            List<ManagedProducerStateTopic> managedProducerStateTopics) {
        if (!isManagedTopicInventoryCurrent(image, generation, cycle)) {
            reconcileManagedTopicInventoryNewerImage(image, generation, cycle);
            return;
        }
        CompletableFuture<List<ManagedTopic>> catalogInventoryFuture;
        try {
            catalogInventoryFuture = Objects.requireNonNull(
                    topicLifecycle.listManagedTopics(),
                    "Diskless topic lifecycle inventory returned null future");
        } catch (Throwable error) {
            catalogInventoryFuture = CompletableFuture.failedFuture(error);
        }
        withTimeout(
                catalogInventoryFuture,
                managedTopicInventoryTimeoutMs,
                "Timed out listing managed diskless topics")
                .whenComplete((managedTopics, error) -> {
                    if (error != null) {
                        retryManagedTopicInventory(
                                image,
                                generation,
                                cycle,
                                currentDisklessTopicIds,
                                context,
                                attempt,
                                retryDelayMs,
                                unwrap(error));
                        return;
                    }

                    List<DesiredTopicState> orphanStates;
                    try {
                        orphanStates = rememberManagedOrphans(
                                image,
                                generation,
                                cycle,
                                currentDisklessTopicIds,
                                managedTopics,
                                managedProducerStateTopics);
                    } catch (Throwable inventoryError) {
                        retryManagedTopicInventory(
                                image,
                                generation,
                                cycle,
                                currentDisklessTopicIds,
                                context,
                                attempt,
                                retryDelayMs,
                                inventoryError);
                        return;
                    }
                    if (orphanStates == null) {
                        reconcileManagedTopicInventoryNewerImage(image, generation, cycle);
                        return;
                    }
                    for (DesiredTopicState orphanState : orphanStates) {
                        scheduleDeletionIfActive(orphanState, context, generation);
                    }
                    scheduleNextManagedTopicInventory(
                            image,
                            generation,
                            cycle,
                            currentDisklessTopicIds,
                            context);
                });
    }

    private void retryManagedTopicInventory(
            MetadataImage image,
            long generation,
            long cycle,
            Set<Uuid> currentDisklessTopicIds,
            String context,
            long attempt,
            long retryDelayMs,
            Throwable error) {
        if (!isManagedTopicInventoryCurrent(image, generation, cycle)) {
            reconcileManagedTopicInventoryNewerImage(image, generation, cycle);
            return;
        }
        reportFailure("list managed diskless topics", context, error, attempt);
        delayBeforeManagedTopicInventory(image, generation, cycle, retryDelayMs)
                .thenRun(() -> reconcileManagedTopicOrphans(
                        image,
                        generation,
                        cycle,
                        currentDisklessTopicIds,
                        context,
                        attempt + 1,
                        nextRetryDelayMs(retryDelayMs)));
    }

    private void scheduleNextManagedTopicInventory(
            MetadataImage image,
            long generation,
            long cycle,
            Set<Uuid> currentDisklessTopicIds,
            String context) {
        if (!isManagedTopicInventoryCurrent(image, generation, cycle)) {
            reconcileManagedTopicInventoryNewerImage(image, generation, cycle);
            return;
        }
        delayBeforeManagedTopicInventory(
                image, generation, cycle, managedTopicInventoryIntervalMs)
                .thenRun(() -> reconcileManagedTopicOrphans(
                        image,
                        generation,
                        cycle,
                        currentDisklessTopicIds,
                        context,
                        1,
                        initialRetryDelayMs));
    }

    private synchronized List<DesiredTopicState> rememberManagedOrphans(
            MetadataImage image,
            long generation,
            long cycle,
            Set<Uuid> currentDisklessTopicIds,
            List<ManagedTopic> managedTopics,
            List<ManagedProducerStateTopic> managedProducerStateTopics) {
        if (!isManagedTopicInventoryCurrent(image, generation, cycle)) {
            return null;
        }
        Objects.requireNonNull(managedTopics, "managedTopics must not be null");
        Objects.requireNonNull(
                managedProducerStateTopics,
                "managedProducerStateTopics must not be null");
        long imageOffset = image.highestOffsetAndEpoch().offset();
        Map<Uuid, Long> newestSourceRevisions = new HashMap<>();
        Map<Uuid, String> topicNames = new HashMap<>();
        Set<Uuid> catalogTopicIds = new LinkedHashSet<>();
        Set<Uuid> producerStateTopicIds = new LinkedHashSet<>();
        for (ManagedTopic managedTopic : managedTopics) {
            Objects.requireNonNull(managedTopic, "managedTopics must not contain null");
            catalogTopicIds.add(managedTopic.topicId());
            topicNames.put(managedTopic.topicId(), managedTopic.topicName());
            newestSourceRevisions.merge(
                    managedTopic.topicId(), managedTopic.sourceRevision(), Math::max);
        }
        for (ManagedProducerStateTopic managedTopic : managedProducerStateTopics) {
            Objects.requireNonNull(
                    managedTopic,
                    "managedProducerStateTopics must not contain null");
            producerStateTopicIds.add(managedTopic.topicId());
            topicNames.putIfAbsent(managedTopic.topicId(), managedTopic.topicName());
            newestSourceRevisions.merge(
                    managedTopic.topicId(), managedTopic.sourceRevision(), Math::max);
        }
        Map<Uuid, DesiredTopicState> orphanStates = new HashMap<>();
        for (Map.Entry<Uuid, Long> entry : newestSourceRevisions.entrySet()) {
            Uuid topicId = entry.getKey();
            if (entry.getValue() <= imageOffset && !currentDisklessTopicIds.contains(topicId)) {
                boolean deleteCatalog = catalogTopicIds.contains(topicId);
                boolean deleteProducerState = deleteCatalog || producerStateTopicIds.contains(topicId);
                orphanStates.put(topicId, rememberDeletedTopic(
                        topicId,
                        topicNames.get(topicId),
                        deleteCatalog,
                        deleteProducerState));
            }
        }
        return List.copyOf(orphanStates.values());
    }

    private synchronized boolean isManagedTopicInventoryCurrent(
            MetadataImage image,
            long generation,
            long cycle) {
        return managedTopicInventoryCycle == cycle
                && isReconciliationImageCurrent(image, generation);
    }

    private void reconcileManagedTopicInventoryNewerImage(
            MetadataImage previousImage,
            long generation,
            long cycle) {
        boolean cycleIsCurrent;
        synchronized (this) {
            cycleIsCurrent = managedTopicInventoryCycle == cycle;
        }
        if (cycleIsCurrent) {
            reconcileNewerImage(previousImage, generation);
        }
    }

    private <T> CompletableFuture<T> withTimeout(
            CompletableFuture<T> source,
            long timeoutMs,
            String description) {
        CompletableFuture<T> timed = new CompletableFuture<>();
        ScheduledFuture<?> timeout;
        synchronized (this) {
            if (closed) {
                return CompletableFuture.failedFuture(
                        new CancellationException("Diskless topic lifecycle publisher is closed"));
            }
            try {
                timeout = retryExecutor.schedule(
                        () -> timeoutTimedOperation(timed, description),
                        timeoutMs,
                        TimeUnit.MILLISECONDS);
            } catch (RuntimeException scheduleError) {
                return CompletableFuture.failedFuture(scheduleError);
            }
            pendingTimedOperations.put(
                    timed,
                    new PendingTimedOperation(source, timed, timeout));
        }
        source.whenComplete((value, error) -> completeTimedOperation(timed, value, error));
        return timed;
    }

    private <T> void completeTimedOperation(
            CompletableFuture<T> timed,
            T value,
            Throwable error) {
        PendingTimedOperation operation;
        synchronized (this) {
            operation = pendingTimedOperations.remove(timed);
        }
        if (operation == null) {
            return;
        }
        operation.timeout().cancel(false);
        if (error == null) {
            timed.complete(value);
        } else {
            timed.completeExceptionally(error);
        }
    }

    private void timeoutTimedOperation(CompletableFuture<?> timed, String description) {
        PendingTimedOperation operation;
        synchronized (this) {
            operation = pendingTimedOperations.remove(timed);
        }
        if (operation == null) {
            return;
        }
        timed.completeExceptionally(new TimeoutException(description));
        operation.source().cancel(true);
    }

    private DesiredTopicState rememberPresentTopic(
            TopicImage topicImage,
            Map<String, String> configs,
            long sourceRevision) {
        return rememberPresentTopic(topicImage, configs, sourceRevision, false);
    }

    private DesiredTopicState rememberPresentTopicFromFullImage(
            TopicImage topicImage,
            Map<String, String> configs,
            long sourceRevision) {
        return rememberPresentTopic(topicImage, configs, sourceRevision, true);
    }

    private DesiredTopicState rememberPresentTopic(
            TopicImage topicImage,
            Map<String, String> configs,
            long sourceRevision,
            boolean retainRevisionForSameContents) {
        Map<String, String> immutableConfigs = Map.copyOf(configs);
        synchronized (this) {
            passiveDeletionStates.remove(topicImage.id());
            protectedPassiveDeletionStates.remove(topicImage.id());
            DesiredTopicState current = desiredStates.get(topicImage.id());
            boolean sameContents = current != null
                    && current.status() == DesiredStatus.PRESENT
                    && current.topicName().equals(topicImage.name())
                    && current.partitions() == topicImage.partitions().size()
                    && current.configs().equals(immutableConfigs);
            if (sameContents && (retainRevisionForSameContents
                            || current.sourceRevision() == sourceRevision)) {
                return current;
            }
            DesiredTopicState updated = new DesiredTopicState(
                    ++nextDesiredRevision,
                    topicImage.id(),
                    topicImage.name(),
                    DesiredStatus.PRESENT,
                    topicImage.partitions().size(),
                    immutableConfigs,
                    sourceRevision,
                    false,
                    false);
            desiredStates.put(topicImage.id(), updated);
            return updated;
        }
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
        return rememberDeletedTopic(topicId, topicName, true, true);
    }

    private DesiredTopicState rememberDeletedTopic(
            Uuid topicId,
            String topicName,
            boolean deleteCatalog,
            boolean deleteProducerState) {
        if (!deleteCatalog && !deleteProducerState) {
            throw new IllegalArgumentException("A deleted topic must have at least one cleanup target");
        }
        synchronized (this) {
            DesiredTopicState current = desiredStates.get(topicId);
            boolean mergedDeleteCatalog = mergeCatalogCleanup(deleteCatalog, current);
            boolean mergedDeleteProducerState = mergeProducerStateCleanup(
                    deleteProducerState, current);
            String mergedTopicName = mergeDeletedTopicName(deleteCatalog, topicName, current);
            if (matchesDeletedState(
                    current,
                    mergedTopicName,
                    mergedDeleteCatalog,
                    mergedDeleteProducerState)) {
                return current;
            }
            DesiredTopicState updated = new DesiredTopicState(
                    ++nextDesiredRevision,
                    topicId,
                    mergedTopicName,
                    DesiredStatus.DELETED,
                    0,
                    Map.of(),
                    -1,
                    mergedDeleteCatalog,
                    mergedDeleteProducerState);
            desiredStates.put(topicId, updated);
            if (!isActiveController) {
                passiveDeletionStates.add(topicId);
                trimPassiveDeletionStates();
            }
            return updated;
        }
    }

    private static boolean mergeCatalogCleanup(
            boolean deleteCatalog,
            DesiredTopicState current) {
        return deleteCatalog || isDeletedState(current) && current.deleteCatalog();
    }

    private static boolean mergeProducerStateCleanup(
            boolean deleteProducerState,
            DesiredTopicState current) {
        return deleteProducerState || isDeletedState(current) && current.deleteProducerState();
    }

    private static String mergeDeletedTopicName(
            boolean deleteCatalog,
            String topicName,
            DesiredTopicState current) {
        return deleteCatalog || !isDeletedState(current) ? topicName : current.topicName();
    }

    private static boolean matchesDeletedState(
            DesiredTopicState current,
            String topicName,
            boolean deleteCatalog,
            boolean deleteProducerState) {
        return isDeletedState(current)
                && current.topicName().equals(topicName)
                && current.deleteCatalog() == deleteCatalog
                && current.deleteProducerState() == deleteProducerState;
    }

    private static boolean isDeletedState(DesiredTopicState state) {
        return state != null && state.status() == DesiredStatus.DELETED;
    }

    private void scheduleRegistrationIfActive(
            DesiredTopicState desired,
            String context,
            long generation) {
        scheduleTopicOperationIfActive(desired, context, generation);
    }

    private void scheduleDeletionIfActive(
            DesiredTopicState desired,
            String context,
            long generation) {
        scheduleTopicOperationIfActive(desired, context, generation);
    }

    private void scheduleTopicOperationIfActive(
            DesiredTopicState desired,
            String context,
            long generation) {
        PendingCancellation cancellation = PendingCancellation.NONE;
        TopicOperationRunner runnerToStart = null;
        synchronized (this) {
            ScheduledOperation scheduled = claimOperation(desired, generation);
            if (scheduled == null) {
                return;
            }
            TopicOperationRunner runner = topicRunners.get(desired.topicId());
            if (runner == null || runner.leadershipGeneration != generation) {
                if (runner != null) {
                    cancellation = abandonCurrentWorkflow(runner);
                }
                runner = new TopicOperationRunner(desired.topicId(), generation);
                topicRunners.put(desired.topicId(), runner);
            }
            runner.pendingDesired = desired;
            runner.pendingContext = context;
            if (runner.currentDesired == null) {
                runnerToStart = runner;
            } else if (desired.status() == DesiredStatus.DELETED
                    && runner.currentDesired.status() == DesiredStatus.PRESENT) {
                cancellation = cancellation.plus(abandonCurrentWorkflow(runner));
                runnerToStart = runner;
            }
        }
        cancelPendingOperations(cancellation);
        if (runnerToStart != null) {
            startPendingWorkflow(runnerToStart);
        }
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

    private void startPendingWorkflow(TopicOperationRunner runner) {
        List<RetryingOperation> operations;
        long workflowGeneration;
        synchronized (this) {
            if (!isCurrentRunner(runner) || runner.currentDesired != null) {
                return;
            }
            DesiredTopicState desired = runner.pendingDesired;
            if (desired == null) {
                topicRunners.remove(runner.topicId, runner);
                return;
            }
            runner.pendingDesired = null;
            runner.currentDesired = desired;
            runner.currentContext = runner.pendingContext;
            runner.pendingContext = null;
            workflowGeneration = ++runner.workflowGeneration;
            if (desired.status() == DesiredStatus.PRESENT) {
                operations = List.of(new RetryingOperation(
                        "register diskless topic " + desired.topicName()
                                + " (" + desired.topicId() + ")",
                        () -> reconcilePresentTopic(desired),
                        initialRetryDelayMs));
            } else {
                List<RetryingOperation> deletionOperations = new ArrayList<>();
                if (desired.deleteCatalog()) {
                    deletionOperations.add(new RetryingOperation(
                            "unregister diskless topic " + desired.topicName()
                                    + " (" + desired.topicId() + ")",
                            () -> topicLifecycle.unregisterTopic(
                                    desired.topicName(), desired.topicId()),
                            initialRetryDelayMs));
                }
                if (desired.deleteProducerState()) {
                    deletionOperations.add(new RetryingOperation(
                            "delete producer-state snapshots for topic " + desired.topicName()
                                    + " (" + desired.topicId() + ")",
                            () -> producerStateStore.deleteTopicSnapshots(desired.topicId()),
                            initialRetryDelayMs));
                }
                operations = List.copyOf(deletionOperations);
            }
            runner.operations = operations;
        }
        for (RetryingOperation operation : operations) {
            startOperationAttempt(runner, workflowGeneration, operation);
        }
    }

    private CompletableFuture<Void> reconcilePresentTopic(DesiredTopicState desired) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        AtomicReference<CompletableFuture<Void>> manifestSource = new AtomicReference<>();
        AtomicReference<CompletableFuture<Void>> catalogSource = new AtomicReference<>();
        propagatePresentTopicCancellation(result, manifestSource, catalogSource);

        CompletableFuture<Void> manifest = startProducerStateReconciliation(desired, result);
        if (manifest == null) {
            return result;
        }
        manifestSource.set(manifest);
        if (result.isCancelled()) {
            cancelFuture(manifest);
            return result;
        }
        manifest.whenComplete((ignored, error) -> completeProducerStateReconciliation(
                desired, result, catalogSource, error));
        return result;
    }

    private static void propagatePresentTopicCancellation(
            CompletableFuture<Void> result,
            AtomicReference<CompletableFuture<Void>> manifestSource,
            AtomicReference<CompletableFuture<Void>> catalogSource) {
        result.whenComplete((ignored, error) -> {
            if (result.isCancelled()) {
                cancelFuture(manifestSource.get());
                cancelFuture(catalogSource.get());
            }
        });
    }

    private CompletableFuture<Void> startProducerStateReconciliation(
            DesiredTopicState desired,
            CompletableFuture<Void> result) {
        try {
            return Objects.requireNonNull(
                    producerStateStore.reconcileTopic(
                            desired.topicName(),
                            desired.topicId(),
                            desired.sourceRevision()),
                    "Diskless producer-state reconciliation returned null future");
        } catch (Throwable error) {
            result.completeExceptionally(error);
            return null;
        }
    }

    private void completeProducerStateReconciliation(
            DesiredTopicState desired,
            CompletableFuture<Void> result,
            AtomicReference<CompletableFuture<Void>> catalogSource,
            Throwable error) {
        if (error != null) {
            result.completeExceptionally(error);
        } else if (!result.isDone()) {
            startCatalogReconciliation(desired, result, catalogSource);
        }
    }

    private void startCatalogReconciliation(
            DesiredTopicState desired,
            CompletableFuture<Void> result,
            AtomicReference<CompletableFuture<Void>> catalogSource) {
        CompletableFuture<Void> catalog;
        try {
            catalog = Objects.requireNonNull(
                    topicLifecycle.registerTopic(
                            desired.topicName(),
                            desired.topicId(),
                            desired.partitions(),
                            desired.configs(),
                            desired.sourceRevision()),
                    "Diskless topic lifecycle registration returned null future");
        } catch (Throwable error) {
            result.completeExceptionally(error);
            return;
        }
        catalogSource.set(catalog);
        if (result.isCancelled()) {
            cancelFuture(catalog);
            return;
        }
        catalog.whenComplete((ignored, error) -> completeResult(result, error));
    }

    private static void completeResult(CompletableFuture<Void> result, Throwable error) {
        if (error == null) {
            result.complete(null);
        } else {
            result.completeExceptionally(error);
        }
    }

    private void startOperationAttempt(
            TopicOperationRunner runner,
            long workflowGeneration,
            RetryingOperation operation) {
        synchronized (this) {
            if (!isCurrentRunnerWorkflow(runner, workflowGeneration)
                    || operation.completed
                    || operation.source != null
                    || operation.retryDelay != null) {
                return;
            }
        }

        CompletableFuture<Void> source;
        try {
            source = Objects.requireNonNull(
                    operation.supplier.get(), "Diskless lifecycle operation returned null future");
        } catch (Throwable error) {
            source = CompletableFuture.failedFuture(error);
        }

        boolean accepted;
        synchronized (this) {
            accepted = isCurrentRunnerWorkflow(runner, workflowGeneration)
                    && !operation.completed
                    && operation.source == null
                    && operation.retryDelay == null;
            if (accepted) {
                operation.source = source;
            }
        }
        if (!accepted) {
            cancelFuture(source);
            return;
        }
        CompletableFuture<Void> acceptedSource = source;
        source.whenComplete((ignored, error) -> completeOperationAttempt(
                runner, workflowGeneration, operation, acceptedSource, error));
    }

    private void completeOperationAttempt(
            TopicOperationRunner runner,
            long workflowGeneration,
            RetryingOperation operation,
            CompletableFuture<Void> source,
            Throwable error) {
        PendingCancellation cancellation = PendingCancellation.NONE;
        TopicOperationRunner runnerToStart = null;
        Throwable failureToReport = null;
        long failedAttempt = -1;
        String context = "";
        synchronized (this) {
            if (!isCurrentRunnerWorkflow(runner, workflowGeneration)
                    || operation.source != source) {
                return;
            }
            operation.source = null;
            if (error == null) {
                operation.completed = true;
                if (runner.operations.stream().allMatch(candidate -> candidate.completed)) {
                    runnerToStart = completeCurrentWorkflow(runner);
                }
            } else if (runner.pendingDesired != null) {
                cancellation = abandonCurrentWorkflow(runner);
                runnerToStart = runner;
            } else {
                failureToReport = unwrap(error);
                failedAttempt = operation.attempt++;
                context = runner.currentContext;
                long retryDelayMs = operation.retryDelayMs;
                operation.retryDelayMs = nextRetryDelayMs(retryDelayMs);
                RetryDelay retryDelay = new RetryDelay();
                operation.retryDelay = retryDelay;
                try {
                    retryDelay.future = retryExecutor.schedule(
                            () -> retryOperation(runner, workflowGeneration, operation, retryDelay),
                            retryDelayMs,
                            TimeUnit.MILLISECONDS);
                } catch (RuntimeException scheduleError) {
                    operation.retryDelay = null;
                    failureToReport = scheduleError;
                }
            }
        }
        if (failureToReport != null) {
            reportFailure(operation.name, context, failureToReport, failedAttempt);
        }
        cancelPendingOperations(cancellation);
        if (runnerToStart != null) {
            startPendingWorkflow(runnerToStart);
        }
    }

    private void retryOperation(
            TopicOperationRunner runner,
            long workflowGeneration,
            RetryingOperation operation,
            RetryDelay retryDelay) {
        synchronized (this) {
            if (!isCurrentRunnerWorkflow(runner, workflowGeneration)
                    || operation.retryDelay != retryDelay) {
                return;
            }
            operation.retryDelay = null;
        }
        startOperationAttempt(runner, workflowGeneration, operation);
    }

    private CompletableFuture<Void> delayBeforeManagedTopicInventory(
            MetadataImage image,
            long generation,
            long cycle,
            long delayMs) {
        CompletableFuture<Void> delay = new CompletableFuture<>();
        synchronized (this) {
            if (!isManagedTopicInventoryCurrent(image, generation, cycle)) {
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

    private synchronized List<PendingTimedOperation> removePendingTimedOperations() {
        List<PendingTimedOperation> operations = new ArrayList<>(pendingTimedOperations.values());
        pendingTimedOperations.clear();
        operations.forEach(operation -> operation.timeout().cancel(false));
        return operations;
    }

    private static void cancelTimedOperations(List<PendingTimedOperation> operations) {
        for (PendingTimedOperation operation : operations) {
            operation.timed().cancel(false);
            operation.source().cancel(true);
        }
    }

    private synchronized PendingCancellation removeTopicRunners() {
        List<CompletableFuture<?>> sources = new ArrayList<>();
        List<ScheduledFuture<?>> delays = new ArrayList<>();
        for (TopicOperationRunner runner : topicRunners.values()) {
            PendingCancellation cancellation = abandonCurrentWorkflow(runner);
            sources.addAll(cancellation.sources());
            delays.addAll(cancellation.delays());
        }
        topicRunners.clear();
        return sources.isEmpty() && delays.isEmpty()
                ? PendingCancellation.NONE
                : new PendingCancellation(sources, delays);
    }

    private PendingCancellation abandonCurrentWorkflow(TopicOperationRunner runner) {
        List<CompletableFuture<?>> sources = new ArrayList<>();
        List<ScheduledFuture<?>> delays = new ArrayList<>();
        for (RetryingOperation operation : runner.operations) {
            if (operation.source != null) {
                sources.add(operation.source);
                operation.source = null;
            }
            if (operation.retryDelay != null) {
                if (operation.retryDelay.future != null) {
                    delays.add(operation.retryDelay.future);
                }
                operation.retryDelay = null;
            }
        }
        runner.currentDesired = null;
        runner.currentContext = null;
        runner.operations = List.of();
        runner.workflowGeneration++;
        return sources.isEmpty() && delays.isEmpty()
                ? PendingCancellation.NONE
                : new PendingCancellation(sources, delays);
    }

    private TopicOperationRunner completeCurrentWorkflow(TopicOperationRunner runner) {
        DesiredTopicState completedDesired = runner.currentDesired;
        runner.currentDesired = null;
        runner.currentContext = null;
        runner.operations = List.of();
        runner.workflowGeneration++;
        if (completedDesired.status() == DesiredStatus.DELETED) {
            ScheduledOperation scheduled = scheduledOperations.get(completedDesired.topicId());
            if (desiredStates.get(completedDesired.topicId()) == completedDesired
                    && scheduled != null
                    && scheduled.leadershipGeneration() == runner.leadershipGeneration
                    && scheduled.desiredRevision() == completedDesired.revision()) {
                desiredStates.remove(completedDesired.topicId());
                passiveDeletionStates.remove(completedDesired.topicId());
                protectedPassiveDeletionStates.remove(completedDesired.topicId());
                scheduledOperations.remove(completedDesired.topicId());
            }
        }
        if (runner.pendingDesired != null) {
            return runner;
        }
        topicRunners.remove(runner.topicId, runner);
        return null;
    }

    private boolean isCurrentRunner(TopicOperationRunner runner) {
        return !closed
                && isActiveController
                && leadershipGeneration == runner.leadershipGeneration
                && topicRunners.get(runner.topicId) == runner;
    }

    private boolean isCurrentRunnerWorkflow(
            TopicOperationRunner runner,
            long workflowGeneration) {
        return isCurrentRunner(runner)
                && runner.currentDesired != null
                && runner.workflowGeneration == workflowGeneration;
    }

    private static void cancelPendingOperations(PendingCancellation cancellation) {
        for (ScheduledFuture<?> delay : cancellation.delays()) {
            try {
                delay.cancel(false);
            } catch (Throwable ignored) {
                // Continue cancelling every operation owned by the retired controller generation.
            }
        }
        for (CompletableFuture<?> source : cancellation.sources()) {
            cancelFuture(source);
        }
    }

    private static void cancelFuture(CompletableFuture<?> future) {
        if (future == null) {
            return;
        }
        try {
            future.cancel(true);
        } catch (Throwable ignored) {
            // A provider cancellation failure must not prevent the remaining sources from draining.
        }
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

    synchronized int activeTopicRunnerCountForTesting() {
        return topicRunners.size();
    }

    synchronized int pendingTopicRevisionCountForTesting(Uuid topicId) {
        TopicOperationRunner runner = topicRunners.get(topicId);
        return runner != null && runner.pendingDesired != null ? 1 : 0;
    }

    synchronized int pendingSourceAttemptCountForTesting() {
        int count = 0;
        for (TopicOperationRunner runner : topicRunners.values()) {
            for (RetryingOperation operation : runner.operations) {
                if (operation.source != null) {
                    count++;
                }
            }
        }
        return count;
    }

    synchronized int pendingTopicRetryDelayCountForTesting() {
        int count = 0;
        for (TopicOperationRunner runner : topicRunners.values()) {
            for (RetryingOperation operation : runner.operations) {
                if (operation.retryDelay != null) {
                    count++;
                }
            }
        }
        return count;
    }

    private void rememberDeletionsForPassiveController() {
        for (DesiredTopicState desired : desiredStates.values()) {
            if (desired.status() == DesiredStatus.DELETED
                    && !protectedPassiveDeletionStates.contains(desired.topicId())) {
                passiveDeletionStates.add(desired.topicId());
            }
        }
        trimPassiveDeletionStates();
    }

    private void protectInFlightDeletionsForPassiveController() {
        for (TopicOperationRunner runner : topicRunners.values()) {
            if ((runner.currentDesired != null
                            && runner.currentDesired.status() == DesiredStatus.DELETED)
                    || (runner.pendingDesired != null
                            && runner.pendingDesired.status() == DesiredStatus.DELETED)) {
                protectedPassiveDeletionStates.add(runner.topicId);
            }
        }
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
