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

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.image.AclsImage;
import org.apache.kafka.image.ClientQuotasImage;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.ConfigurationImage;
import org.apache.kafka.image.ConfigurationsImage;
import org.apache.kafka.image.DelegationTokenImage;
import org.apache.kafka.image.FeaturesImage;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.ProducerIdsImage;
import org.apache.kafka.image.ScramImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.loader.LoaderManifestType;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.storage.diskless.DisklessProducerStateStore;
import org.apache.kafka.storage.diskless.DisklessTopicLifecycle;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DisklessTopicLifecyclePublisherTest {

    private record Registration(
            String topicName,
            Uuid topicId,
            int partitions,
            Map<String, String> properties) {
    }

    private static final class RecordingLifecycle implements DisklessTopicLifecycle {
        final List<String> registeredTopics = new CopyOnWriteArrayList<>();
        final List<Registration> registrations = new CopyOnWriteArrayList<>();
        final List<String> unregisteredTopics = new CopyOnWriteArrayList<>();
        final AtomicInteger registerAttempts = new AtomicInteger();
        final AtomicInteger unregisterAttempts = new AtomicInteger();
        IntFunction<CompletableFuture<Void>> registerBehavior = attempt -> CompletableFuture.completedFuture(null);
        IntFunction<CompletableFuture<Void>> unregisterBehavior = attempt -> CompletableFuture.completedFuture(null);

        @Override
        public CompletableFuture<Void> registerTopic(
                String topicName,
                Uuid topicId,
                int partitions,
                Map<String, String> properties) {
            registeredTopics.add(topicName + ":" + topicId + ":" + partitions);
            registrations.add(new Registration(topicName, topicId, partitions, Map.copyOf(properties)));
            return registerBehavior.apply(registerAttempts.incrementAndGet());
        }

        @Override
        public CompletableFuture<Void> unregisterTopic(String topicName, Uuid topicId) {
            unregisteredTopics.add(topicName + ":" + topicId);
            return unregisterBehavior.apply(unregisterAttempts.incrementAndGet());
        }

        @Override
        public void close() {
        }
    }

    private static final class RecordingProducerStateStore implements DisklessProducerStateStore {
        final List<String> deletedTopics = new CopyOnWriteArrayList<>();
        final AtomicInteger deleteAttempts = new AtomicInteger();
        IntFunction<CompletableFuture<Void>> deleteBehavior = attempt -> CompletableFuture.completedFuture(null);

        @Override
        public CompletableFuture<Void> deleteTopicSnapshots(Uuid topicId) {
            deletedTopics.add(topicId.toString());
            return deleteBehavior.apply(deleteAttempts.incrementAndGet());
        }

        @Override
        public void close() {
        }
    }

    @Test
    void testLatestImageIsReconciledWhenBecomingLeader() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        int partitions = 3;

        MetadataImage newImage = new MetadataImage(
                new MetadataProvenance(10, 0, 0, true),
                FeaturesImage.EMPTY, ClusterImage.EMPTY,
                topicsImage(disklessTopic, topicId, partitions),
                configsImageWithDisklessEnabled(disklessTopic),
                ClientQuotasImage.EMPTY, ProducerIdsImage.EMPTY,
                AclsImage.EMPTY, ScramImage.EMPTY, DelegationTokenImage.EMPTY);

        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> { });

        MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        delta.replay(new TopicRecord().setName(disklessTopic).setTopicId(topicId));
        for (int p = 0; p < partitions; p++) {
            delta.replay(partitionRecord(topicId, p));
        }
        delta.replay(disklessConfigRecord(disklessTopic));

        publisher.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));
        assertTrue(lifecycle.registeredTopics.isEmpty(), "No registration expected before becoming leader");
        assertTrue(lifecycle.unregisteredTopics.isEmpty(), "No cleanup expected before becoming leader");
        assertTrue(producerStateStore.deletedTopics.isEmpty(), "No calls expected before becoming leader");

        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        TestUtils.waitForCondition(
                () -> lifecycle.registrations.size() == 1,
                5_000, "Expected full image registration after becoming leader");
        assertEquals(
                new Registration(
                        disklessTopic,
                        topicId,
                        partitions,
                        Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true")),
                lifecycle.registrations.get(0));
        publisher.close();
    }

    @Test
    void testCommittedDisklessTopicCreationReconcilesCompleteRegistration() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        int partitions = 3;
        Map<String, String> configs = Map.of(
                TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true",
                TopicConfig.RETENTION_MS_CONFIG, "12345");

        MetadataDelta delta = topicCreationDelta(MetadataImage.EMPTY, disklessTopic, topicId, partitions, configs);
        MetadataImage newImage = delta.apply(new MetadataProvenance(10, 0, 0, true));

        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> { });
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));

        TestUtils.waitForCondition(
                () -> lifecycle.registrations.size() == 1,
                5_000, "Expected committed diskless topic registration");
        assertEquals(
                new Registration(disklessTopic, topicId, partitions, configs),
                lifecycle.registrations.get(0));
        assertTrue(lifecycle.unregisteredTopics.isEmpty());
        assertTrue(producerStateStore.deletedTopics.isEmpty());
        publisher.close();
    }

    @Test
    void testCreatePartitionsReregistersCompletePartitionCountWithoutDuplicatingCreation() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        Map<String, String> configs = Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true");
        MetadataDelta creationDelta = topicCreationDelta(
                MetadataImage.EMPTY, disklessTopic, topicId, 1, configs);
        MetadataImage createdImage = creationDelta.apply(new MetadataProvenance(10, 0, 0, true));

        RecordingLifecycle lifecycle = new RecordingLifecycle();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, new RecordingProducerStateStore(), (message, cause) -> { });
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(creationDelta, createdImage, loaderManifest(createdImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.registrations.size() == 1,
                5_000, "Expected exactly one registration for topic creation");

        MetadataDelta expansionDelta = new MetadataDelta.Builder().setImage(createdImage).build();
        expansionDelta.replay(partitionRecord(topicId, 1));
        expansionDelta.replay(partitionRecord(topicId, 2));
        MetadataImage expandedImage = expansionDelta.apply(new MetadataProvenance(11, 0, 0, true));
        publisher.onMetadataUpdate(expansionDelta, expandedImage, loaderManifest(expandedImage.provenance()));

        TestUtils.waitForCondition(
                () -> lifecycle.registrations.size() == 2,
                5_000, "Expected CreatePartitions to refresh catalog registration");
        assertEquals(List.of(1, 3), lifecycle.registrations.stream()
                .map(Registration::partitions)
                .toList());

        MetadataDelta unchangedCountDelta = new MetadataDelta.Builder().setImage(expandedImage).build();
        unchangedCountDelta.replay(partitionRecord(topicId, 0));
        MetadataImage unchangedCountImage = unchangedCountDelta.apply(
                new MetadataProvenance(12, 0, 0, true));
        publisher.onMetadataUpdate(
                unchangedCountDelta,
                unchangedCountImage,
                loaderManifest(unchangedCountImage.provenance()));
        Thread.sleep(50);
        assertEquals(2, lifecycle.registrations.size(),
                "Partition metadata updates with an unchanged count must not re-register");
        publisher.close();
    }

    @Test
    void testCommittedNonDisklessTopicCreationIgnored() {
        String topicName = "normal-topic";
        Uuid topicId = Uuid.randomUuid();
        Map<String, String> configs = Map.of(TopicConfig.RETENTION_MS_CONFIG, "12345");
        MetadataDelta delta = topicCreationDelta(MetadataImage.EMPTY, topicName, topicId, 1, configs);
        MetadataImage newImage = delta.apply(new MetadataProvenance(10, 0, 0, true));

        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> { });
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));

        assertTrue(lifecycle.registeredTopics.isEmpty());
        publisher.close();
    }

    @Test
    void testDeletionSupersedesBlockedRegistration() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        Map<String, String> configs = Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true");
        MetadataDelta creationDelta = topicCreationDelta(
                MetadataImage.EMPTY, disklessTopic, topicId, 1, configs);
        MetadataImage createdImage = creationDelta.apply(new MetadataProvenance(1, 0, 0, true));
        MetadataDelta deletionDelta = new MetadataDelta.Builder().setImage(createdImage).build();
        deletionDelta.replay(new RemoveTopicRecord().setTopicId(topicId));
        MetadataImage deletedImage = deletionDelta.apply(new MetadataProvenance(2, 0, 0, true));

        CompletableFuture<Void> registrationGate = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.registerBehavior = attempt -> registrationGate;
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> { });
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        publisher.onMetadataUpdate(creationDelta, createdImage, loaderManifest(createdImage.provenance()));
        publisher.onMetadataUpdate(deletionDelta, deletedImage, loaderManifest(deletedImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.registrations.size() == 1,
                5_000, "Expected registration to start");
        TestUtils.waitForCondition(
                () -> lifecycle.unregisteredTopics.size() == 1
                        && producerStateStore.deletedTopics.size() == 1,
                5_000, "Expected both deletion branches to complete while registration remains blocked");
        assertTrue(!registrationGate.isDone(), "Registration must still be blocked");
        assertEquals(List.of(disklessTopic + ":" + topicId), lifecycle.unregisteredTopics);
        assertEquals(List.of(topicId.toString()), producerStateStore.deletedTopics);

        registrationGate.completeExceptionally(new RuntimeException("late registration failure"));
        TestUtils.waitForCondition(
                () -> lifecycle.unregisterAttempts.get() == 2
                        && producerStateStore.deleteAttempts.get() == 2,
                5_000, "Expected compensating deletion after the superseded registration settled");
        assertEquals(1, lifecycle.registerAttempts.get(), "Superseded registration must not retry");

        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(2), 2));
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 3));
        Thread.sleep(50);
        assertEquals(2, lifecycle.unregisterAttempts.get(), "Late registration must not restore deleted state");
        assertEquals(2, producerStateStore.deleteAttempts.get());
        publisher.close();
    }

    @Test
    void testDeletionCompensatesForLateSuccessfulRegistration() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        Map<String, String> configs = Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true");
        MetadataDelta creationDelta = topicCreationDelta(
                MetadataImage.EMPTY, disklessTopic, topicId, 1, configs);
        MetadataImage createdImage = creationDelta.apply(new MetadataProvenance(1, 0, 0, true));
        MetadataDelta deletionDelta = new MetadataDelta.Builder().setImage(createdImage).build();
        deletionDelta.replay(new RemoveTopicRecord().setTopicId(topicId));
        MetadataImage deletedImage = deletionDelta.apply(new MetadataProvenance(2, 0, 0, true));

        CompletableFuture<Void> registrationGate = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.registerBehavior = attempt -> registrationGate;
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> { });
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        publisher.onMetadataUpdate(creationDelta, createdImage, loaderManifest(createdImage.provenance()));
        publisher.onMetadataUpdate(deletionDelta, deletedImage, loaderManifest(deletedImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.unregisterAttempts.get() == 1
                        && producerStateStore.deleteAttempts.get() == 1,
                5_000, "Expected immediate deletion while registration remains blocked");

        registrationGate.complete(null);
        TestUtils.waitForCondition(
                () -> lifecycle.unregisterAttempts.get() == 2
                        && producerStateStore.deleteAttempts.get() == 2,
                5_000, "Expected deletion to run again after late registration success");
        assertEquals(1, lifecycle.registerAttempts.get());

        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(2), 2));
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 3));
        Thread.sleep(50);
        assertEquals(2, lifecycle.unregisterAttempts.get());
        assertEquals(2, producerStateStore.deleteAttempts.get());
        publisher.close();
    }

    @Test
    void testBlockedTopicDoesNotBlockAnotherTopic() throws Exception {
        String firstTopic = "first-topic";
        Uuid firstTopicId = Uuid.randomUuid();
        Map<String, String> configs = Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true");
        MetadataDelta firstDelta = topicCreationDelta(
                MetadataImage.EMPTY, firstTopic, firstTopicId, 1, configs);
        MetadataImage firstImage = firstDelta.apply(new MetadataProvenance(1, 0, 0, true));

        CompletableFuture<Void> firstRegistration = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.registerBehavior = attempt -> attempt == 1
                ? firstRegistration
                : CompletableFuture.completedFuture(null);
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, new RecordingProducerStateStore(), (message, cause) -> { }, 5, 10);
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(firstDelta, firstImage, loaderManifest(firstImage.provenance()));

        String secondTopic = "second-topic";
        Uuid secondTopicId = Uuid.randomUuid();
        MetadataDelta secondDelta = topicCreationDelta(firstImage, secondTopic, secondTopicId, 1, configs);
        MetadataImage secondImage = secondDelta.apply(new MetadataProvenance(2, 0, 0, true));
        publisher.onMetadataUpdate(secondDelta, secondImage, loaderManifest(secondImage.provenance()));

        TestUtils.waitForCondition(
                () -> lifecycle.registrations.size() == 2,
                5_000, "Expected second topic to register while first topic remains blocked");
        assertEquals(List.of(firstTopic, secondTopic), lifecycle.registrations.stream()
                .map(Registration::topicName)
                .toList());
        firstRegistration.complete(null);
        publisher.close();
    }

    @Test
    void testRegistrationKeepsRetryingBeyondThreeAttempts() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        MetadataDelta delta = topicCreationDelta(
                MetadataImage.EMPTY,
                disklessTopic,
                topicId,
                1,
                Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true"));
        MetadataImage newImage = delta.apply(new MetadataProvenance(10, 0, 0, true));

        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.registerBehavior = attempt -> attempt < 5
                ? CompletableFuture.failedFuture(new RuntimeException("transient failure"))
                : CompletableFuture.completedFuture(null);
        List<Throwable> faults = new CopyOnWriteArrayList<>();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1,
                lifecycle,
                new RecordingProducerStateStore(),
                (message, cause) -> faults.add(cause),
                5,
                10);
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));

        TestUtils.waitForCondition(
                () -> lifecycle.registerAttempts.get() == 5,
                5_000, "Expected registration retry to recover");
        assertEquals(4, faults.size());
        publisher.close();
    }

    @Test
    void testLeadershipLossFencesRetryAndLaterLeadershipCanReconcile() throws Exception {
        String firstTopic = "first-topic";
        Uuid firstTopicId = Uuid.randomUuid();
        Map<String, String> configs = Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true");
        MetadataDelta firstDelta = topicCreationDelta(
                MetadataImage.EMPTY, firstTopic, firstTopicId, 1, configs);
        MetadataImage firstImage = firstDelta.apply(new MetadataProvenance(1, 0, 0, true));

        CompletableFuture<Void> firstAttempt = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.registerBehavior = attempt -> attempt == 1
                ? firstAttempt
                : CompletableFuture.completedFuture(null);
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> { }, 10, 20);
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(firstDelta, firstImage, loaderManifest(firstImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.registerAttempts.get() == 1,
                5_000, "Expected first registration attempt");

        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(2), 2));
        firstAttempt.completeExceptionally(new RuntimeException("old controller attempt failed"));

        String secondTopic = "second-topic";
        Uuid secondTopicId = Uuid.randomUuid();
        MetadataDelta secondDelta = topicCreationDelta(firstImage, secondTopic, secondTopicId, 2, configs);
        MetadataImage secondImage = secondDelta.apply(new MetadataProvenance(2, 0, 0, true));
        publisher.onMetadataUpdate(secondDelta, secondImage, loaderManifest(secondImage.provenance()));
        Thread.sleep(50);
        assertEquals(1, lifecycle.registerAttempts.get(), "Old leadership must not start a retry");

        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 3));

        TestUtils.waitForCondition(
                () -> lifecycle.registrations.size() == 3,
                5_000, "Expected new leadership generation to reconcile");
        assertEquals(2, lifecycle.registrations.stream()
                .filter(registration -> registration.topicName().equals(firstTopic))
                .count());
        assertEquals(1, lifecycle.registrations.stream()
                .filter(registration -> registration.topicName().equals(secondTopic))
                .count());
        publisher.close();
    }

    @Test
    void testCloseFencesRetry() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        MetadataDelta delta = topicCreationDelta(
                MetadataImage.EMPTY,
                disklessTopic,
                topicId,
                1,
                Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true"));
        MetadataImage newImage = delta.apply(new MetadataProvenance(10, 0, 0, true));

        CompletableFuture<Void> firstAttempt = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.registerBehavior = attempt -> firstAttempt;
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, new RecordingProducerStateStore(), (message, cause) -> { }, 10, 20);
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.registerAttempts.get() == 1,
                5_000, "Expected first registration attempt");

        publisher.close();
        firstAttempt.completeExceptionally(new RuntimeException("closed publisher attempt failed"));
        Thread.sleep(50);
        assertEquals(1, lifecycle.registerAttempts.get());
    }

    @Test
    void testDisklessTopicDeletionUnregistersCatalogAndCleansProducerState() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        int partitions = 2;

        MetadataImage oldImage = new MetadataImage(
                new MetadataProvenance(1, 0, 0, true),
                FeaturesImage.EMPTY, ClusterImage.EMPTY,
                topicsImage(disklessTopic, topicId, partitions),
                configsImageWithDisklessEnabled(disklessTopic),
                ClientQuotasImage.EMPTY, ProducerIdsImage.EMPTY,
                AclsImage.EMPTY, ScramImage.EMPTY, DelegationTokenImage.EMPTY);

        MetadataDelta delta = new MetadataDelta.Builder().setImage(oldImage).build();
        delta.replay(new RemoveTopicRecord().setTopicId(topicId));
        MetadataImage newImage = delta.apply(new MetadataProvenance(2, 0, 0, true));

        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> { });
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));

        TestUtils.waitForCondition(
                () -> producerStateStore.deletedTopics.size() == 1,
                5_000, "Expected diskless topic cleanup");
        assertEquals(List.of(disklessTopic + ":" + topicId), lifecycle.unregisteredTopics);
        assertEquals(List.of(topicId.toString()), producerStateStore.deletedTopics);
        publisher.close();
    }

    @Test
    void testPassiveDeletionIsRememberedAndReconciledAfterBecomingLeader() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        MetadataImage oldImage = new MetadataImage(
                new MetadataProvenance(1, 0, 0, true),
                FeaturesImage.EMPTY, ClusterImage.EMPTY,
                topicsImage(disklessTopic, topicId, 1),
                configsImageWithDisklessEnabled(disklessTopic),
                ClientQuotasImage.EMPTY, ProducerIdsImage.EMPTY,
                AclsImage.EMPTY, ScramImage.EMPTY, DelegationTokenImage.EMPTY);
        MetadataDelta delta = new MetadataDelta.Builder().setImage(oldImage).build();
        delta.replay(new RemoveTopicRecord().setTopicId(topicId));
        MetadataImage newImage = delta.apply(new MetadataProvenance(2, 0, 0, true));

        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> { });
        publisher.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));
        assertTrue(lifecycle.unregisteredTopics.isEmpty());
        assertTrue(producerStateStore.deletedTopics.isEmpty());

        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        TestUtils.waitForCondition(
                () -> producerStateStore.deletedTopics.size() == 1,
                5_000, "Expected passive deletion to reconcile after becoming leader");
        assertEquals(List.of(disklessTopic + ":" + topicId), lifecycle.unregisteredTopics);
        publisher.close();
    }

    @Test
    void testPassiveDeletionHistoryIsBoundedAndRetainsMostRecentEntries() throws Exception {
        List<String> topicNames = List.of("first-topic", "second-topic", "third-topic");
        List<Uuid> topicIds = List.of(Uuid.randomUuid(), Uuid.randomUuid(), Uuid.randomUuid());
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1,
                lifecycle,
                producerStateStore,
                (message, cause) -> { },
                5,
                10,
                2);

        for (int index = 0; index < topicIds.size(); index++) {
            MetadataImage oldImage = new MetadataImage(
                    new MetadataProvenance(index * 2L + 1, 0, 0, true),
                    FeaturesImage.EMPTY, ClusterImage.EMPTY,
                    topicsImage(topicNames.get(index), topicIds.get(index), 1),
                    configsImageWithDisklessEnabled(topicNames.get(index)),
                    ClientQuotasImage.EMPTY, ProducerIdsImage.EMPTY,
                    AclsImage.EMPTY, ScramImage.EMPTY, DelegationTokenImage.EMPTY);
            MetadataDelta deletionDelta = new MetadataDelta.Builder().setImage(oldImage).build();
            deletionDelta.replay(new RemoveTopicRecord().setTopicId(topicIds.get(index)));
            MetadataImage deletedImage = deletionDelta.apply(
                    new MetadataProvenance(index * 2L + 2, 0, 0, true));
            publisher.onMetadataUpdate(
                    deletionDelta,
                    deletedImage,
                    loaderManifest(deletedImage.provenance()));
        }

        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        TestUtils.waitForCondition(
                () -> producerStateStore.deletedTopics.size() == 2,
                5_000, "Expected only the bounded passive deletion history to reconcile");
        assertEquals(
                Set.of(topicIds.get(1).toString(), topicIds.get(2).toString()),
                Set.copyOf(producerStateStore.deletedTopics));
        assertEquals(
                Set.of(
                        topicNames.get(1) + ":" + topicIds.get(1),
                        topicNames.get(2) + ":" + topicIds.get(2)),
                Set.copyOf(lifecycle.unregisteredTopics));
        publisher.close();
    }

    @Test
    void testInFlightActiveDeletionSurvivesBoundedPassiveHistoryAndLeadershipFailover() throws Exception {
        String activeTopic = "active-topic";
        Uuid activeTopicId = Uuid.randomUuid();
        MetadataImage activeOldImage = new MetadataImage(
                new MetadataProvenance(1, 0, 0, true),
                FeaturesImage.EMPTY, ClusterImage.EMPTY,
                topicsImage(activeTopic, activeTopicId, 1),
                configsImageWithDisklessEnabled(activeTopic),
                ClientQuotasImage.EMPTY, ProducerIdsImage.EMPTY,
                AclsImage.EMPTY, ScramImage.EMPTY, DelegationTokenImage.EMPTY);
        MetadataDelta activeDeletionDelta = new MetadataDelta.Builder().setImage(activeOldImage).build();
        activeDeletionDelta.replay(new RemoveTopicRecord().setTopicId(activeTopicId));
        MetadataImage activeDeletedImage = activeDeletionDelta.apply(
                new MetadataProvenance(2, 0, 0, true));

        CompletableFuture<Void> oldLeadershipCatalogAttempt = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.unregisterBehavior = attempt -> attempt == 1
                ? oldLeadershipCatalogAttempt
                : CompletableFuture.completedFuture(null);
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1,
                lifecycle,
                producerStateStore,
                (message, cause) -> { },
                5,
                10,
                1);
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(
                activeDeletionDelta,
                activeDeletedImage,
                loaderManifest(activeDeletedImage.provenance()));
        TestUtils.waitForCondition(
                () -> producerStateStore.deletedTopics.contains(activeTopicId.toString()),
                5_000, "Expected active snapshot deletion to proceed independently");

        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(2), 2));
        for (int index = 0; index < 2; index++) {
            String passiveTopic = "passive-topic-" + index;
            Uuid passiveTopicId = Uuid.randomUuid();
            MetadataImage passiveOldImage = new MetadataImage(
                    new MetadataProvenance(index * 2L + 3, 0, 0, true),
                    FeaturesImage.EMPTY, ClusterImage.EMPTY,
                    topicsImage(passiveTopic, passiveTopicId, 1),
                    configsImageWithDisklessEnabled(passiveTopic),
                    ClientQuotasImage.EMPTY, ProducerIdsImage.EMPTY,
                    AclsImage.EMPTY, ScramImage.EMPTY, DelegationTokenImage.EMPTY);
            MetadataDelta passiveDeletionDelta = new MetadataDelta.Builder()
                    .setImage(passiveOldImage)
                    .build();
            passiveDeletionDelta.replay(new RemoveTopicRecord().setTopicId(passiveTopicId));
            MetadataImage passiveDeletedImage = passiveDeletionDelta.apply(
                    new MetadataProvenance(index * 2L + 4, 0, 0, true));
            publisher.onMetadataUpdate(
                    passiveDeletionDelta,
                    passiveDeletedImage,
                    loaderManifest(passiveDeletedImage.provenance()));
        }

        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 3));
        oldLeadershipCatalogAttempt.completeExceptionally(new RuntimeException("old leadership failed"));
        TestUtils.waitForCondition(
                () -> lifecycle.unregisteredTopics.stream()
                        .filter(entry -> entry.equals(activeTopic + ":" + activeTopicId))
                        .count() == 2,
                5_000, "Expected new leadership to retry the protected active deletion");
        publisher.close();
    }

    @Test
    void testNonDisklessTopicDeletionIgnored() throws Exception {
        String normalTopic = "normal-topic";
        Uuid topicId = Uuid.randomUuid();
        int partitions = 1;

        MetadataImage oldImage = new MetadataImage(
                new MetadataProvenance(1, 0, 0, true),
                FeaturesImage.EMPTY, ClusterImage.EMPTY,
                topicsImage(normalTopic, topicId, partitions),
                ConfigurationsImage.EMPTY,
                ClientQuotasImage.EMPTY, ProducerIdsImage.EMPTY,
                AclsImage.EMPTY, ScramImage.EMPTY, DelegationTokenImage.EMPTY);

        MetadataDelta delta = new MetadataDelta.Builder().setImage(oldImage).build();
        delta.replay(new RemoveTopicRecord().setTopicId(topicId));
        MetadataImage newImage = delta.apply(new MetadataProvenance(2, 0, 0, true));

        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> { });
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));

        assertTrue(lifecycle.unregisteredTopics.isEmpty());
        assertTrue(producerStateStore.deletedTopics.isEmpty());
        publisher.close();
    }

    @Test
    void testDeltaIgnoredWhenNotActiveController() {
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> { });

        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        MetadataImage image = new MetadataImage(
                new MetadataProvenance(10, 0, 0, true),
                FeaturesImage.EMPTY, ClusterImage.EMPTY,
                topicsImage(disklessTopic, topicId, 1),
                configsImageWithDisklessEnabled(disklessTopic),
                ClientQuotasImage.EMPTY, ProducerIdsImage.EMPTY,
                AclsImage.EMPTY, ScramImage.EMPTY, DelegationTokenImage.EMPTY);

        MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        publisher.onMetadataUpdate(delta, image, loaderManifest(image.provenance()));

        assertTrue(lifecycle.unregisteredTopics.isEmpty(), "No calls expected when not active controller");
        assertTrue(producerStateStore.deletedTopics.isEmpty(), "No calls expected when not active controller");
        publisher.close();
    }

    @Test
    void testCatalogUnregisterAndProducerStateCleanupProceedIndependently() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        int partitions = 2;
        MetadataImage oldImage = new MetadataImage(
                new MetadataProvenance(1, 0, 0, true),
                FeaturesImage.EMPTY, ClusterImage.EMPTY,
                topicsImage(disklessTopic, topicId, partitions),
                configsImageWithDisklessEnabled(disklessTopic),
                ClientQuotasImage.EMPTY, ProducerIdsImage.EMPTY,
                AclsImage.EMPTY, ScramImage.EMPTY, DelegationTokenImage.EMPTY);
        MetadataDelta delta = new MetadataDelta.Builder().setImage(oldImage).build();
        delta.replay(new RemoveTopicRecord().setTopicId(topicId));
        MetadataImage newImage = delta.apply(new MetadataProvenance(2, 0, 0, true));

        RecordingLifecycle lifecycle = new RecordingLifecycle();
        CompletableFuture<Void> catalogGate = new CompletableFuture<>();
        lifecycle.unregisterBehavior = attempt -> catalogGate;
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        producerStateStore.deleteBehavior = attempt -> attempt == 1
                ? CompletableFuture.failedFuture(new RuntimeException("producer state unavailable"))
                : CompletableFuture.completedFuture(null);
        List<String> faults = new CopyOnWriteArrayList<>();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> faults.add(message), 5, 10);
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));

        TestUtils.waitForCondition(
                () -> producerStateStore.deleteAttempts.get() == 2,
                5_000, "Expected producer-state cleanup to retry while catalog unregister remains blocked");
        assertEquals(1, lifecycle.unregisterAttempts.get());
        assertEquals(1, faults.size());
        assertTrue(faults.get(0).contains("delete producer-state snapshots"));
        catalogGate.complete(null);

        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(2), 2));
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 3));
        Thread.sleep(50);
        assertEquals(1, lifecycle.unregisterAttempts.get(),
                "Completed deletion must be forgotten before the next leadership generation");
        assertEquals(2, producerStateStore.deleteAttempts.get());
        publisher.close();
    }

    @Test
    void testSynchronousCatalogFailureStillRunsProducerStateCleanup() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        MetadataImage oldImage = new MetadataImage(
                new MetadataProvenance(1, 0, 0, true),
                FeaturesImage.EMPTY, ClusterImage.EMPTY,
                topicsImage(disklessTopic, topicId, 1),
                configsImageWithDisklessEnabled(disklessTopic),
                ClientQuotasImage.EMPTY, ProducerIdsImage.EMPTY,
                AclsImage.EMPTY, ScramImage.EMPTY, DelegationTokenImage.EMPTY);
        MetadataDelta delta = new MetadataDelta.Builder().setImage(oldImage).build();
        delta.replay(new RemoveTopicRecord().setTopicId(topicId));
        MetadataImage newImage = delta.apply(new MetadataProvenance(2, 0, 0, true));

        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.unregisterBehavior = attempt -> {
            if (attempt == 1) {
                throw new RuntimeException("synchronous catalog failure");
            }
            return CompletableFuture.completedFuture(null);
        };
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        List<String> faults = new CopyOnWriteArrayList<>();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> faults.add(message), 5, 10);
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));

        TestUtils.waitForCondition(
                () -> producerStateStore.deletedTopics.size() == 1,
                5_000, "Expected producer-state cleanup after synchronous catalog failure");
        TestUtils.waitForCondition(
                () -> lifecycle.unregisterAttempts.get() == 2,
                5_000, "Expected catalog unregister to retry independently");
        assertEquals(1, faults.size());
        publisher.close();
    }

    @Test
    void testProducerStateFailureIsReportedIndependently() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        MetadataImage oldImage = new MetadataImage(
                new MetadataProvenance(1, 0, 0, true),
                FeaturesImage.EMPTY, ClusterImage.EMPTY,
                topicsImage(disklessTopic, topicId, 1),
                configsImageWithDisklessEnabled(disklessTopic),
                ClientQuotasImage.EMPTY, ProducerIdsImage.EMPTY,
                AclsImage.EMPTY, ScramImage.EMPTY, DelegationTokenImage.EMPTY);
        MetadataDelta delta = new MetadataDelta.Builder().setImage(oldImage).build();
        delta.replay(new RemoveTopicRecord().setTopicId(topicId));
        MetadataImage newImage = delta.apply(new MetadataProvenance(2, 0, 0, true));

        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        producerStateStore.deleteBehavior = attempt -> attempt == 1
                ? CompletableFuture.failedFuture(new RuntimeException("producer state unavailable"))
                : CompletableFuture.completedFuture(null);
        List<String> faults = new CopyOnWriteArrayList<>();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> faults.add(message), 5, 10);
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));

        TestUtils.waitForCondition(
                () -> producerStateStore.deleteAttempts.get() == 2,
                5_000, "Expected producer-state cleanup retry");
        assertEquals(List.of(disklessTopic + ":" + topicId), lifecycle.unregisteredTopics);
        assertEquals(1, faults.size());
        assertTrue(faults.get(0).contains("delete producer-state snapshots"));
        publisher.close();
    }

    private static MetadataDelta topicCreationDelta(
            MetadataImage oldImage,
            String topicName,
            Uuid topicId,
            int partitions,
            Map<String, String> configs) {
        MetadataDelta delta = new MetadataDelta.Builder().setImage(oldImage).build();
        delta.replay(new TopicRecord().setName(topicName).setTopicId(topicId));
        for (int partition = 0; partition < partitions; partition++) {
            delta.replay(partitionRecord(topicId, partition));
        }
        configs.forEach((name, value) -> delta.replay(configRecord(topicName, name, value)));
        return delta;
    }

    private static TopicsImage topicsImage(String topicName, Uuid topicId, int partitions) {
        return TopicsImage.EMPTY.including(new TopicImage(topicName, topicId, partitionMap(partitions)));
    }

    private static Map<Integer, PartitionRegistration> partitionMap(int partitions) {
        Map<Integer, PartitionRegistration> partitionMap = new HashMap<>();
        for (int partition = 0; partition < partitions; partition++) {
            partitionMap.put(partition, new PartitionRegistration.Builder()
                    .setReplicas(new int[]{0})
                    .setDirectories(DirectoryId.migratingArray(1))
                    .setIsr(new int[]{0})
                    .setLeader(0)
                    .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
                    .setLeaderEpoch(0)
                    .setPartitionEpoch(0)
                    .build());
        }
        return partitionMap;
    }

    private static PartitionRecord partitionRecord(Uuid topicId, int partition) {
        return new PartitionRecord()
                .setTopicId(topicId)
                .setPartitionId(partition)
                .setReplicas(List.of(0))
                .setIsr(List.of(0))
                .setRemovingReplicas(List.of())
                .setAddingReplicas(List.of())
                .setLeader(0)
                .setLeaderEpoch(0)
                .setPartitionEpoch(0);
    }

    private static ConfigRecord disklessConfigRecord(String topicName) {
        return configRecord(topicName, TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true");
    }

    private static ConfigRecord configRecord(String topicName, String name, String value) {
        return new ConfigRecord()
                .setResourceType(ConfigResource.Type.TOPIC.id())
                .setResourceName(topicName)
                .setName(name)
                .setValue(value);
    }

    private static ConfigurationsImage configsImageWithDisklessEnabled(String topicName) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        Map<String, String> configs = Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true");
        return new ConfigurationsImage(Map.of(resource, new ConfigurationImage(resource, configs)));
    }

    private static LoaderManifest loaderManifest(MetadataProvenance provenance) {
        return new LoaderManifest() {
            @Override
            public LoaderManifestType type() {
                return LoaderManifestType.LOG_DELTA;
            }

            @Override
            public MetadataProvenance provenance() {
                return provenance;
            }
        };
    }
}
