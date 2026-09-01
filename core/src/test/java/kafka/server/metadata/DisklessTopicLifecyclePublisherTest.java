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
import org.apache.kafka.storage.diskless.DisklessProducerStateStore.ManagedProducerStateTopic;
import org.apache.kafka.storage.diskless.DisklessTopicLifecycle;
import org.apache.kafka.storage.diskless.DisklessTopicLifecycle.ManagedTopic;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DisklessTopicLifecyclePublisherTest {

    private record Registration(
            String topicName,
            Uuid topicId,
            int partitions,
            Map<String, String> properties,
            long sourceRevision) {
    }

    private static final class RecordingLifecycle implements DisklessTopicLifecycle {
        final List<String> registeredTopics = new CopyOnWriteArrayList<>();
        final List<Registration> registrations = new CopyOnWriteArrayList<>();
        final List<String> unregisteredTopics = new CopyOnWriteArrayList<>();
        final AtomicInteger registerAttempts = new AtomicInteger();
        final AtomicInteger unregisterAttempts = new AtomicInteger();
        final AtomicInteger listAttempts = new AtomicInteger();
        IntFunction<CompletableFuture<List<ManagedTopic>>> listBehavior =
                attempt -> CompletableFuture.completedFuture(List.of());
        IntFunction<CompletableFuture<Void>> registerBehavior = attempt -> CompletableFuture.completedFuture(null);
        IntFunction<CompletableFuture<Void>> unregisterBehavior = attempt -> CompletableFuture.completedFuture(null);

        @Override
        public CompletableFuture<List<ManagedTopic>> listManagedTopics() {
            return listBehavior.apply(listAttempts.incrementAndGet());
        }

        @Override
        public CompletableFuture<Void> registerTopic(
                String topicName,
                Uuid topicId,
                int partitions,
                Map<String, String> properties,
                long sourceRevision) {
            registeredTopics.add(topicName + ":" + topicId + ":" + partitions);
            registrations.add(new Registration(
                    topicName, topicId, partitions, Map.copyOf(properties), sourceRevision));
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
        final List<ManagedProducerStateTopic> reconciledTopics = new CopyOnWriteArrayList<>();
        final List<String> deletedTopics = new CopyOnWriteArrayList<>();
        final AtomicInteger listAttempts = new AtomicInteger();
        final AtomicInteger reconcileAttempts = new AtomicInteger();
        final AtomicInteger deleteAttempts = new AtomicInteger();
        IntFunction<CompletableFuture<List<ManagedProducerStateTopic>>> listBehavior =
                attempt -> CompletableFuture.completedFuture(List.of());
        IntFunction<CompletableFuture<Void>> reconcileBehavior =
                attempt -> CompletableFuture.completedFuture(null);
        IntFunction<CompletableFuture<Void>> deleteBehavior = attempt -> CompletableFuture.completedFuture(null);

        @Override
        public CompletableFuture<List<ManagedProducerStateTopic>> listManagedTopics() {
            return listBehavior.apply(listAttempts.incrementAndGet());
        }

        @Override
        public CompletableFuture<Void> reconcileTopic(
                String topicName,
                Uuid topicId,
                long sourceRevision) {
            reconciledTopics.add(new ManagedProducerStateTopic(topicName, topicId, sourceRevision));
            return reconcileBehavior.apply(reconcileAttempts.incrementAndGet());
        }

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
                        Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true"),
                        10),
                lifecycle.registrations.get(0));
        publisher.close();
    }

    @Test
    void testFullImageDeletesCatalogTopicMissingFromKRaft() throws Exception {
        String currentTopic = "current-topic";
        Uuid currentTopicId = Uuid.randomUuid();
        String orphanTopic = "orphan-topic";
        Uuid orphanTopicId = Uuid.randomUuid();
        MetadataImage image = new MetadataImage(
                new MetadataProvenance(10, 0, 0, true),
                FeaturesImage.EMPTY, ClusterImage.EMPTY,
                topicsImage(currentTopic, currentTopicId, 1),
                configsImageWithDisklessEnabled(currentTopic),
                ClientQuotasImage.EMPTY, ProducerIdsImage.EMPTY,
                AclsImage.EMPTY, ScramImage.EMPTY, DelegationTokenImage.EMPTY);

        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.listBehavior = attempt -> CompletableFuture.completedFuture(List.of(
                new ManagedTopic(currentTopic, currentTopicId, 10),
                new ManagedTopic(orphanTopic, orphanTopicId, 10)));
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> { });
        publisher.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                image,
                loaderManifest(image.provenance()));

        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        TestUtils.waitForCondition(
                () -> producerStateStore.deletedTopics.contains(orphanTopicId.toString()),
                5_000,
                "Expected cold-start reconciliation to delete the Catalog orphan");
        assertEquals(List.of(orphanTopic + ":" + orphanTopicId), lifecycle.unregisteredTopics);
        assertEquals(List.of(orphanTopicId.toString()), producerStateStore.deletedTopics);
        assertTrue(lifecycle.unregisteredTopics.stream()
                .noneMatch(topic -> topic.endsWith(currentTopicId.toString())));
        publisher.close();
    }

    @Test
    void testFullImageDeletesProducerStateOrphanAfterCatalogIsGone() throws Exception {
        String orphanTopic = "producer-state-orphan";
        Uuid orphanTopicId = Uuid.randomUuid();
        MetadataImage image = emptyImage(10);
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        producerStateStore.listBehavior = attempt -> CompletableFuture.completedFuture(List.of(
                new ManagedProducerStateTopic(orphanTopic, orphanTopicId, 10)));
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> { });
        publisher.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                image,
                loaderManifest(image.provenance()));

        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        TestUtils.waitForCondition(
                () -> producerStateStore.deletedTopics.contains(orphanTopicId.toString()),
                5_000,
                "Expected cold-start reconciliation to delete orphan producer state");
        assertTrue(lifecycle.unregisteredTopics.isEmpty(),
                "A producer-state-only orphan must not fabricate a Catalog deletion");
        publisher.close();
    }

    @Test
    void testLaggingControllerDefersProducerStateOrphanFromNewerRevision() throws Exception {
        String topicName = "newer-producer-state-topic";
        Uuid topicId = Uuid.randomUuid();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        producerStateStore.listBehavior = attempt -> CompletableFuture.completedFuture(List.of(
                new ManagedProducerStateTopic(topicName, topicId, 11)));
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1,
                lifecycle,
                producerStateStore,
                (message, cause) -> { },
                10,
                100,
                10_000,
                10,
                1_000);
        MetadataImage staleImage = emptyImage(10);
        publisher.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                staleImage,
                loaderManifest(staleImage.provenance()));
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        TestUtils.waitForCondition(
                () -> producerStateStore.listAttempts.get() == 1,
                5_000,
                "Expected producer-state inventory for the stale image");
        assertTrue(producerStateStore.deletedTopics.isEmpty(),
                "A lagging controller must not delete producer state from a newer revision");

        MetadataImage caughtUpImage = emptyImage(11);
        publisher.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(staleImage).build(),
                caughtUpImage,
                loaderManifest(caughtUpImage.provenance()));
        TestUtils.waitForCondition(
                () -> producerStateStore.deletedTopics.contains(topicId.toString()),
                5_000,
                "Expected cleanup after the controller reaches the producer-state source revision");
        assertTrue(lifecycle.unregisteredTopics.isEmpty());
        publisher.close();
    }

    @Test
    void testCurrentDisklessTopicIsProtectedFromProducerStateInventory() throws Exception {
        String topicName = "current-producer-state-topic";
        Uuid topicId = Uuid.randomUuid();
        MetadataImage image = new MetadataImage(
                new MetadataProvenance(10, 0, 0, true),
                FeaturesImage.EMPTY, ClusterImage.EMPTY,
                topicsImage(topicName, topicId, 1),
                configsImageWithDisklessEnabled(topicName),
                ClientQuotasImage.EMPTY, ProducerIdsImage.EMPTY,
                AclsImage.EMPTY, ScramImage.EMPTY, DelegationTokenImage.EMPTY);
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        producerStateStore.listBehavior = attempt -> CompletableFuture.completedFuture(List.of(
                new ManagedProducerStateTopic(topicName, topicId, 10)));
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> { });
        publisher.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                image,
                loaderManifest(image.provenance()));
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        TestUtils.waitForCondition(
                () -> lifecycle.registerAttempts.get() == 1
                        && producerStateStore.listAttempts.get() == 1,
                5_000,
                "Expected registration and producer-state inventory");
        assertTrue(producerStateStore.deletedTopics.isEmpty());
        assertTrue(lifecycle.unregisteredTopics.isEmpty());
        publisher.close();
    }

    @Test
    void testProducerStateManifestCompletesBeforeCatalogRegistration() throws Exception {
        String topicName = "manifest-first-topic";
        Uuid topicId = Uuid.randomUuid();
        CompletableFuture<Void> pendingManifest = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        producerStateStore.reconcileBehavior = attempt -> pendingManifest;
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> { });
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        MetadataDelta delta = topicCreationDelta(
                MetadataImage.EMPTY,
                topicName,
                topicId,
                1,
                Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true"));
        MetadataImage image = delta.apply(new MetadataProvenance(10, 0, 0, true));

        publisher.onMetadataUpdate(delta, image, loaderManifest(image.provenance()));

        TestUtils.waitForCondition(
                () -> producerStateStore.reconcileAttempts.get() == 1,
                5_000,
                "Expected producer-state manifest reconciliation");
        assertEquals(0, lifecycle.registerAttempts.get(),
                "Catalog registration must wait for the durable producer-state manifest");
        pendingManifest.complete(null);
        TestUtils.waitForCondition(
                () -> lifecycle.registerAttempts.get() == 1,
                5_000,
                "Expected Catalog registration after the manifest becomes durable");
        publisher.close();
    }

    @Test
    void testCloseCancelsHungProducerStateInventory() throws Exception {
        CompletableFuture<List<ManagedProducerStateTopic>> hungInventory = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        producerStateStore.listBehavior = attempt -> hungInventory;
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> { });
        MetadataImage image = emptyImage(10);
        publisher.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                image,
                loaderManifest(image.provenance()));
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        TestUtils.waitForCondition(
                () -> producerStateStore.listAttempts.get() == 1,
                5_000,
                "Expected a producer-state inventory attempt");

        publisher.close();

        assertTrue(hungInventory.isCancelled(),
                "Close must cancel a hung producer-state inventory");
        assertEquals(0, lifecycle.listAttempts.get(),
                "Catalog inventory must not start before producer-state inventory completes");
    }

    @Test
    void testStaleCatalogInventoryCannotDeleteTopicFromNewerImage() throws Exception {
        String topicName = "new-topic";
        Uuid topicId = Uuid.randomUuid();
        CompletableFuture<List<ManagedTopic>> oldImageInventory = new CompletableFuture<>();
        CompletableFuture<List<ManagedTopic>> newerImageInventory = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.listBehavior = attempt -> attempt == 1
                ? oldImageInventory
                : newerImageInventory;
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> { });
        MetadataImage initialImage = emptyImage(0);
        publisher.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                initialImage,
                loaderManifest(initialImage.provenance()));
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        TestUtils.waitForCondition(
                () -> lifecycle.listAttempts.get() == 1,
                5_000,
                "Expected inventory for the initial image");

        MetadataDelta creationDelta = topicCreationDelta(
                initialImage,
                topicName,
                topicId,
                1,
                Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true"));
        MetadataImage createdImage = creationDelta.apply(new MetadataProvenance(1, 0, 0, true));
        publisher.onMetadataUpdate(
                creationDelta,
                createdImage,
                loaderManifest(createdImage.provenance()));
        oldImageInventory.complete(List.of(new ManagedTopic(topicName, topicId, 1)));

        TestUtils.waitForCondition(
                () -> lifecycle.listAttempts.get() == 2,
                5_000,
                "Expected stale result to trigger inventory against the newer image");
        newerImageInventory.complete(List.of(new ManagedTopic(topicName, topicId, 1)));
        assertTrue(lifecycle.unregisteredTopics.isEmpty(), "Stale inventory must not delete a current topic");
        assertTrue(producerStateStore.deletedTopics.isEmpty());
        publisher.close();
    }

    @Test
    void testUnrelatedMetadataRevisionDoesNotReregisterEveryDisklessTopic() throws Exception {
        String topicName = "stable-topic";
        Uuid topicId = Uuid.randomUuid();
        CompletableFuture<List<ManagedTopic>> firstInventory = new CompletableFuture<>();
        CompletableFuture<List<ManagedTopic>> secondInventory = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.listBehavior = attempt -> attempt == 1 ? firstInventory : secondInventory;
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, new RecordingProducerStateStore(), (message, cause) -> { });
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        MetadataDelta creationDelta = topicCreationDelta(
                MetadataImage.EMPTY,
                topicName,
                topicId,
                1,
                Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true"));
        MetadataImage createdImage = creationDelta.apply(new MetadataProvenance(1, 0, 0, true));
        publisher.onMetadataUpdate(
                creationDelta,
                createdImage,
                loaderManifest(createdImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.registrations.size() == 1 && lifecycle.listAttempts.get() == 1,
                5_000,
                "Expected initial registration and inventory");

        MetadataDelta unrelatedDelta = new MetadataDelta.Builder().setImage(createdImage).build();
        MetadataImage unrelatedImage = unrelatedDelta.apply(new MetadataProvenance(2, 0, 0, true));
        publisher.onMetadataUpdate(
                unrelatedDelta,
                unrelatedImage,
                loaderManifest(unrelatedImage.provenance()));
        firstInventory.complete(List.of(new ManagedTopic(topicName, topicId, 1)));
        TestUtils.waitForCondition(
                () -> lifecycle.listAttempts.get() == 2,
                5_000,
                "Expected inventory to restart against the newer image");
        secondInventory.complete(List.of(new ManagedTopic(topicName, topicId, 1)));

        assertEquals(1, lifecycle.registrations.size(),
                "An unrelated image revision must not rewrite every Catalog topic");
        assertEquals(1, lifecycle.registrations.get(0).sourceRevision());
        publisher.close();
    }

    @Test
    void testLaggingControllerDefersCatalogOrphanFromNewerRevision() throws Exception {
        String topicName = "newer-catalog-topic";
        Uuid topicId = Uuid.randomUuid();
        MetadataImage staleImage = emptyImage(10);
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.listBehavior = attempt -> CompletableFuture.completedFuture(List.of(
                new ManagedTopic(topicName, topicId, 11)));
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> { });
        publisher.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                staleImage,
                loaderManifest(staleImage.provenance()));

        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        assertEquals(1, lifecycle.listAttempts.get());
        assertTrue(lifecycle.unregisteredTopics.isEmpty(),
                "A lagging controller must not delete Catalog state from a newer KRaft revision");
        assertTrue(producerStateStore.deletedTopics.isEmpty());

        MetadataImage caughtUpImage = emptyImage(11);
        publisher.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(staleImage).build(),
                caughtUpImage,
                loaderManifest(caughtUpImage.provenance()));
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(2), 2));
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 3));

        TestUtils.waitForCondition(
                () -> producerStateStore.deletedTopics.contains(topicId.toString()),
                5_000,
                "Expected deletion once the controller image reaches the Catalog source revision");
        assertEquals(List.of(topicName + ":" + topicId), lifecycle.unregisteredTopics);
        publisher.close();
    }

    @Test
    void testStaleFullImageDoesNotDeleteRememberedTopicFromNewerRevision() throws Exception {
        String topicName = "newer-image-topic";
        Uuid topicId = Uuid.randomUuid();
        MetadataImage initialImage = emptyImage(0);
        MetadataDelta creationDelta = topicCreationDelta(
                initialImage,
                topicName,
                topicId,
                1,
                Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true"));
        MetadataImage newerImage = creationDelta.apply(new MetadataProvenance(11, 0, 0, true));
        MetadataImage staleImage = emptyImage(10);

        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> { });

        // Seed the state which can result when revision 11 arrives while a revision 10 full-image
        // reconciliation is still being prepared, then run the stale snapshot deterministically.
        publisher.onMetadataUpdate(
                creationDelta,
                newerImage,
                loaderManifest(newerImage.provenance()));
        publisher.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(newerImage).build(),
                staleImage,
                loaderManifest(staleImage.provenance()));

        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        assertTrue(lifecycle.unregisteredTopics.isEmpty(),
                "A stale full image must not turn a newer remembered registration into a deletion");
        assertTrue(producerStateStore.deletedTopics.isEmpty());

        MetadataImage caughtUpImage = emptyImage(11);
        publisher.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(staleImage).build(),
                caughtUpImage,
                loaderManifest(caughtUpImage.provenance()));
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(2), 2));
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 3));

        TestUtils.waitForCondition(
                () -> producerStateStore.deletedTopics.contains(topicId.toString()),
                5_000,
                "Expected deletion once a full image reaches the remembered source revision");
        publisher.close();
    }

    @Test
    void testCatalogInventoryFailureRetriesWhileImageRemainsCurrent() throws Exception {
        String orphanTopic = "orphan-topic";
        Uuid orphanTopicId = Uuid.randomUuid();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.listBehavior = attempt -> attempt == 1
                ? CompletableFuture.failedFuture(new RuntimeException("temporary inventory failure"))
                : CompletableFuture.completedFuture(List.of(new ManagedTopic(orphanTopic, orphanTopicId, 10)));
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        List<Throwable> faults = new CopyOnWriteArrayList<>();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1,
                lifecycle,
                producerStateStore,
                (message, cause) -> faults.add(cause),
                5,
                10);
        MetadataImage image = emptyImage(10);
        publisher.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                image,
                loaderManifest(image.provenance()));
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        TestUtils.waitForCondition(
                () -> producerStateStore.deletedTopics.contains(orphanTopicId.toString()),
                5_000,
                "Expected inventory retry to discover and delete the orphan");
        assertEquals(2, lifecycle.listAttempts.get());
        assertEquals(1, faults.size());
        publisher.close();
    }

    @Test
    void testPeriodicInventoryDeletesOrphanCreatedAfterEmptyScan() throws Exception {
        String orphanTopic = "late-orphan";
        Uuid orphanTopicId = Uuid.randomUuid();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.listBehavior = attempt -> attempt == 2
                ? CompletableFuture.completedFuture(List.of(new ManagedTopic(orphanTopic, orphanTopicId, 10)))
                : CompletableFuture.completedFuture(List.of());
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1,
                lifecycle,
                producerStateStore,
                (message, cause) -> { },
                5,
                10,
                10,
                10,
                100);
        MetadataImage image = emptyImage(10);
        publisher.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                image,
                loaderManifest(image.provenance()));
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        TestUtils.waitForCondition(
                () -> producerStateStore.deletedTopics.contains(orphanTopicId.toString()),
                5_000,
                "Expected the next active-controller inventory to delete a late orphan");
        assertTrue(lifecycle.listAttempts.get() >= 2);
        assertTrue(lifecycle.unregisteredTopics.contains(orphanTopic + ":" + orphanTopicId));
        publisher.close();
    }

    @Test
    void testTimedOutInventoryDoesNotBlockLaterScan() throws Exception {
        String orphanTopic = "orphan-after-timeout";
        Uuid orphanTopicId = Uuid.randomUuid();
        CompletableFuture<List<ManagedTopic>> hungInventory = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.listBehavior = attempt -> {
            if (attempt == 1) {
                return hungInventory;
            }
            if (attempt == 2) {
                return CompletableFuture.completedFuture(List.of(
                        new ManagedTopic(orphanTopic, orphanTopicId, 10)));
            }
            return CompletableFuture.completedFuture(List.of());
        };
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        List<Throwable> faults = new CopyOnWriteArrayList<>();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1,
                lifecycle,
                producerStateStore,
                (message, cause) -> faults.add(cause),
                5,
                10,
                10,
                10,
                20);
        MetadataImage image = emptyImage(10);
        publisher.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                image,
                loaderManifest(image.provenance()));
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        TestUtils.waitForCondition(
                () -> producerStateStore.deletedTopics.contains(orphanTopicId.toString()),
                5_000,
                "Expected inventory to recover after one timed-out call");
        assertTrue(lifecycle.listAttempts.get() >= 2);
        assertTrue(faults.stream().anyMatch(TimeoutException.class::isInstance));
        assertTrue(hungInventory.isCancelled(), "Timed-out inventory source must be cancelled");
        publisher.close();
    }

    @Test
    void testCloseCancelsHungCatalogInventory() throws Exception {
        CompletableFuture<List<ManagedTopic>> hungInventory = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.listBehavior = attempt -> hungInventory;
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, new RecordingProducerStateStore(), (message, cause) -> { });
        MetadataImage image = emptyImage(10);
        publisher.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                image,
                loaderManifest(image.provenance()));
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        TestUtils.waitForCondition(
                () -> lifecycle.listAttempts.get() == 1,
                5_000,
                "Expected a Catalog inventory attempt");

        publisher.close();

        assertTrue(hungInventory.isCancelled(), "Close must cancel a hung Catalog inventory");
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
                new Registration(disklessTopic, topicId, partitions, configs, 10),
                lifecycle.registrations.get(0));
        assertTrue(lifecycle.unregisteredTopics.isEmpty());
        assertTrue(producerStateStore.deletedTopics.isEmpty());
        publisher.close();
    }

    @Test
    void testCommittedTopicConfigChangeReconcilesAtMetadataRevision() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        Map<String, String> initialConfigs = Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true");
        MetadataDelta creationDelta = topicCreationDelta(
                MetadataImage.EMPTY, disklessTopic, topicId, 1, initialConfigs);
        MetadataImage createdImage = creationDelta.apply(new MetadataProvenance(10, 0, 0, true));

        RecordingLifecycle lifecycle = new RecordingLifecycle();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, new RecordingProducerStateStore(), (message, cause) -> { });
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(creationDelta, createdImage, loaderManifest(createdImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.registrations.size() == 1,
                5_000,
                "Expected initial registration");

        MetadataDelta configDelta = new MetadataDelta.Builder().setImage(createdImage).build();
        configDelta.replay(configRecord(disklessTopic, TopicConfig.RETENTION_MS_CONFIG, "12345"));
        MetadataImage updatedImage = configDelta.apply(new MetadataProvenance(11, 0, 0, true));
        publisher.onMetadataUpdate(configDelta, updatedImage, loaderManifest(updatedImage.provenance()));

        TestUtils.waitForCondition(
                () -> lifecycle.registrations.size() == 2,
                5_000,
                "Expected config change registration");
        assertEquals(
                new Registration(
                        disklessTopic,
                        topicId,
                        1,
                        Map.of(
                                TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true",
                                TopicConfig.RETENTION_MS_CONFIG, "12345"),
                        11),
                lifecycle.registrations.get(1));
        publisher.close();
    }

    @Test
    void testConfigChangeWhileInitialRegistrationIsPendingRunsLatestRegistration()
            throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        MetadataDelta creationDelta = topicCreationDelta(
                MetadataImage.EMPTY,
                disklessTopic,
                topicId,
                1,
                Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true"));
        MetadataImage createdImage = creationDelta.apply(
                new MetadataProvenance(10, 0, 0, true));

        CompletableFuture<Void> initialRegistration = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.registerBehavior = attempt -> attempt == 1
                ? initialRegistration
                : CompletableFuture.completedFuture(null);
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, new RecordingProducerStateStore(), (message, cause) -> { });
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(
                creationDelta, createdImage, loaderManifest(createdImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.registerAttempts.get() == 1,
                5_000,
                "Expected initial registration to remain pending");

        MetadataDelta configDelta = new MetadataDelta.Builder().setImage(createdImage).build();
        configDelta.replay(configRecord(
                disklessTopic, TopicConfig.RETENTION_MS_CONFIG, "12345"));
        MetadataImage updatedImage = configDelta.apply(
                new MetadataProvenance(11, 0, 0, true));
        publisher.onMetadataUpdate(
                configDelta, updatedImage, loaderManifest(updatedImage.provenance()));

        assertEquals(1, lifecycle.registerAttempts.get(),
                "The newer registration must remain serialized behind the initial call");
        initialRegistration.complete(null);
        TestUtils.waitForCondition(
                () -> lifecycle.registerAttempts.get() == 2,
                5_000,
                "Expected the latest config snapshot after initial registration completed");
        assertEquals(
                new Registration(
                        disklessTopic,
                        topicId,
                        1,
                        Map.of(
                                TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true",
                                TopicConfig.RETENTION_MS_CONFIG, "12345"),
                        11),
                lifecycle.registrations.get(1));
        publisher.close();
    }

    @Test
    void testManyQueuedRevisionsCoalesceToLatestRegistration()
            throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        Map<String, String> configs = Map.of(
                TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true");
        MetadataDelta creationDelta = topicCreationDelta(
                MetadataImage.EMPTY, disklessTopic, topicId, 1, configs);
        MetadataImage createdImage = creationDelta.apply(
                new MetadataProvenance(10, 0, 0, true));

        CompletableFuture<Void> initialRegistration = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.registerBehavior = attempt -> attempt == 1
                ? initialRegistration
                : CompletableFuture.completedFuture(null);
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, new RecordingProducerStateStore(), (message, cause) -> { });
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(
                creationDelta, createdImage, loaderManifest(createdImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.registerAttempts.get() == 1,
                5_000,
                "Expected initial registration to remain pending");

        MetadataImage updatedImage = createdImage;
        for (int revision = 1; revision <= 1_000; revision++) {
            MetadataDelta configDelta = new MetadataDelta.Builder().setImage(updatedImage).build();
            configDelta.replay(configRecord(
                    disklessTopic, TopicConfig.RETENTION_MS_CONFIG, Integer.toString(revision)));
            updatedImage = configDelta.apply(
                    new MetadataProvenance(10L + revision, 0, 0, true));
            publisher.onMetadataUpdate(
                    configDelta, updatedImage, loaderManifest(updatedImage.provenance()));
        }

        assertEquals(1, lifecycle.registerAttempts.get(),
                "Only the current source attempt may be in flight");
        assertEquals(1, publisher.activeTopicRunnerCountForTesting());
        assertEquals(1, publisher.pendingTopicRevisionCountForTesting(topicId),
                "All queued revisions must collapse into one latest desired state");
        assertEquals(1, publisher.pendingSourceAttemptCountForTesting());

        initialRegistration.complete(null);
        TestUtils.waitForCondition(
                () -> lifecycle.registerAttempts.get() == 2,
                5_000,
                "Expected only the latest queued registration supplier to start");
        assertEquals(2, lifecycle.registrations.size());
        assertEquals(
                new Registration(
                        disklessTopic,
                        topicId,
                        1,
                        Map.of(
                                TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true",
                                TopicConfig.RETENTION_MS_CONFIG, "1000"),
                        1_010),
                lifecycle.registrations.get(1));
        TestUtils.waitForCondition(
                () -> publisher.activeTopicRunnerCountForTesting() == 0,
                5_000,
                "Expected the bounded runner state to retire after the latest revision completed");
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
        assertTrue(registrationGate.isCancelled(),
                "Deletion must cancel the superseded registration source");
        assertEquals(List.of(disklessTopic + ":" + topicId), lifecycle.unregisteredTopics);
        assertEquals(List.of(topicId.toString()), producerStateStore.deletedTopics);

        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(2), 2));
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 3));
        Thread.sleep(50);
        assertEquals(1, lifecycle.unregisterAttempts.get(),
                "A completed durable unregister must retire the desired deletion state");
        assertEquals(1, producerStateStore.deleteAttempts.get());
        publisher.close();
    }

    @Test
    void testDeletionFenceMakesLateRegistrationCompletionHarmless() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        Map<String, String> configs = Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true");
        MetadataDelta creationDelta = topicCreationDelta(
                MetadataImage.EMPTY, disklessTopic, topicId, 1, configs);
        MetadataImage createdImage = creationDelta.apply(new MetadataProvenance(1, 0, 0, true));
        MetadataDelta deletionDelta = new MetadataDelta.Builder().setImage(createdImage).build();
        deletionDelta.replay(new RemoveTopicRecord().setTopicId(topicId));
        MetadataImage deletedImage = deletionDelta.apply(new MetadataProvenance(2, 0, 0, true));

        CompletableFuture<Void> registrationGate = new CompletableFuture<>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return false;
            }
        };
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
        Thread.sleep(50);
        assertEquals(1, lifecycle.registerAttempts.get());

        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(2), 2));
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 3));
        Thread.sleep(50);
        assertEquals(1, lifecycle.unregisterAttempts.get(),
                "The lifecycle provider owns fencing late registration completion");
        assertEquals(1, producerStateStore.deleteAttempts.get());
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
    void testRepeatedSynchronousRegistrationFailuresKeepOneOperationSlot() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        MetadataDelta delta = topicCreationDelta(
                MetadataImage.EMPTY,
                disklessTopic,
                topicId,
                1,
                Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true"));
        MetadataImage image = delta.apply(new MetadataProvenance(10, 0, 0, true));

        CompletableFuture<Void> terminalAttempt = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.registerBehavior = attempt -> {
            if (attempt <= 100) {
                throw new RuntimeException("synchronous failure " + attempt);
            }
            return terminalAttempt;
        };
        List<Throwable> faults = new CopyOnWriteArrayList<>();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1,
                lifecycle,
                new RecordingProducerStateStore(),
                (message, cause) -> faults.add(cause),
                1,
                1);
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(delta, image, loaderManifest(image.provenance()));

        TestUtils.waitForCondition(
                () -> lifecycle.registerAttempts.get() == 101,
                5_000,
                "Expected the bounded runner to reach the pending terminal attempt");
        assertEquals(1, publisher.activeTopicRunnerCountForTesting());
        assertEquals(0, publisher.pendingTopicRevisionCountForTesting(topicId));
        assertEquals(1, publisher.pendingSourceAttemptCountForTesting());
        assertEquals(0, publisher.pendingTopicRetryDelayCountForTesting());
        assertEquals(100, faults.size());

        terminalAttempt.complete(null);
        TestUtils.waitForCondition(
                () -> publisher.activeTopicRunnerCountForTesting() == 0,
                5_000,
                "Expected the runner to retire after recovery");
        assertEquals(0, publisher.pendingSourceAttemptCountForTesting());
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

        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.registerBehavior = attempt -> attempt == 1
                ? CompletableFuture.failedFuture(new RuntimeException("old controller attempt failed"))
                : CompletableFuture.completedFuture(null);
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> { }, 10_000, 10_000);
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(firstDelta, firstImage, loaderManifest(firstImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.registerAttempts.get() == 1
                        && publisher.pendingTopicRetryDelayCountForTesting() == 1,
                5_000, "Expected the old generation to wait on one retry delay");

        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(2), 2));
        assertEquals(0, publisher.activeTopicRunnerCountForTesting());
        assertEquals(0, publisher.pendingTopicRetryDelayCountForTesting(),
                "Leadership loss must detach and cancel the old retry delay");

        String secondTopic = "second-topic";
        Uuid secondTopicId = Uuid.randomUuid();
        MetadataDelta secondDelta = topicCreationDelta(firstImage, secondTopic, secondTopicId, 2, configs);
        MetadataImage secondImage = secondDelta.apply(new MetadataProvenance(2, 0, 0, true));
        publisher.onMetadataUpdate(secondDelta, secondImage, loaderManifest(secondImage.provenance()));
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
    void testLeadershipLossCancelsPendingRegistrationAndNewLeaderReconciles() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        Map<String, String> configs = Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true");
        MetadataDelta delta = topicCreationDelta(
                MetadataImage.EMPTY, disklessTopic, topicId, 1, configs);
        MetadataImage image = delta.apply(new MetadataProvenance(1, 0, 0, true));

        List<CompletableFuture<Void>> registrationSources = new CopyOnWriteArrayList<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.registerBehavior = attempt -> {
            CompletableFuture<Void> source = new CompletableFuture<>();
            registrationSources.add(source);
            return source;
        };
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, new RecordingProducerStateStore(), (message, cause) -> { });
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(delta, image, loaderManifest(image.provenance()));

        for (int generation = 0; generation < 3; generation++) {
            int expectedAttempts = generation + 1;
            TestUtils.waitForCondition(
                    () -> lifecycle.registerAttempts.get() == expectedAttempts,
                    5_000,
                    "Expected a pending source for leadership generation " + generation);
            publisher.onControllerChange(new LeaderAndEpoch(
                    OptionalInt.of(2), generation * 2 + 2));
            assertTrue(registrationSources.get(generation).isCancelled(),
                    "Leadership loss must cancel each retired generation's source");
            assertEquals(0, publisher.activeTopicRunnerCountForTesting());
            if (generation < 2) {
                publisher.onControllerChange(new LeaderAndEpoch(
                        OptionalInt.of(1), generation * 2 + 3));
            }
        }

        assertEquals(3, lifecycle.registerAttempts.get());
        publisher.close();
    }

    @Test
    void testLeadershipLossCancelsPendingDeletionSources() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        MetadataImage oldImage = new MetadataImage(
                new MetadataProvenance(1, 0, 0, true),
                FeaturesImage.EMPTY, ClusterImage.EMPTY,
                topicsImage(disklessTopic, topicId, 1),
                configsImageWithDisklessEnabled(disklessTopic),
                ClientQuotasImage.EMPTY, ProducerIdsImage.EMPTY,
                AclsImage.EMPTY, ScramImage.EMPTY, DelegationTokenImage.EMPTY);
        MetadataDelta deletionDelta = new MetadataDelta.Builder().setImage(oldImage).build();
        deletionDelta.replay(new RemoveTopicRecord().setTopicId(topicId));
        MetadataImage deletedImage = deletionDelta.apply(new MetadataProvenance(2, 0, 0, true));

        CompletableFuture<Void> catalogDeletion = new CompletableFuture<>();
        CompletableFuture<Void> snapshotDeletion = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.unregisterBehavior = attempt -> catalogDeletion;
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        producerStateStore.deleteBehavior = attempt -> snapshotDeletion;
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> { });
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(
                deletionDelta,
                deletedImage,
                loaderManifest(deletedImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.unregisterAttempts.get() == 1
                        && producerStateStore.deleteAttempts.get() == 1,
                5_000,
                "Expected both deletion branches to remain pending");

        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(2), 2));

        assertTrue(catalogDeletion.isCancelled(),
                "Leadership loss must cancel the pending Catalog source");
        assertTrue(snapshotDeletion.isCancelled(),
                "Leadership loss must cancel the pending producer-state source");
        assertEquals(0, publisher.activeTopicRunnerCountForTesting());
        publisher.close();
    }

    @Test
    void testCloseCancelsPendingRegistrationAndFencesRetry() throws Exception {
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
        assertTrue(firstAttempt.isCancelled(), "Close must cancel the current source attempt");
        assertEquals(1, lifecycle.registerAttempts.get());
    }

    @Test
    void testCloseCancelsPendingRegistrationRetryDelay() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        MetadataDelta delta = topicCreationDelta(
                MetadataImage.EMPTY,
                disklessTopic,
                topicId,
                1,
                Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true"));
        MetadataImage image = delta.apply(new MetadataProvenance(10, 0, 0, true));

        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.registerBehavior = attempt -> CompletableFuture.failedFuture(
                new RuntimeException("registration unavailable"));
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1,
                lifecycle,
                new RecordingProducerStateStore(),
                (message, cause) -> { },
                10_000,
                10_000);
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(delta, image, loaderManifest(image.provenance()));
        TestUtils.waitForCondition(
                () -> publisher.pendingTopicRetryDelayCountForTesting() == 1,
                5_000,
                "Expected one pending retry delay before close");

        publisher.close();

        assertEquals(0, publisher.activeTopicRunnerCountForTesting());
        assertEquals(0, publisher.pendingTopicRetryDelayCountForTesting());
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
    void testOldLeadershipDeletionCallbackCannotForgetNewGeneration() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        MetadataImage oldImage = new MetadataImage(
                new MetadataProvenance(1, 0, 0, true),
                FeaturesImage.EMPTY, ClusterImage.EMPTY,
                topicsImage(disklessTopic, topicId, 1),
                configsImageWithDisklessEnabled(disklessTopic),
                ClientQuotasImage.EMPTY, ProducerIdsImage.EMPTY,
                AclsImage.EMPTY, ScramImage.EMPTY, DelegationTokenImage.EMPTY);
        MetadataDelta deletionDelta = new MetadataDelta.Builder().setImage(oldImage).build();
        deletionDelta.replay(new RemoveTopicRecord().setTopicId(topicId));
        MetadataImage deletedImage = deletionDelta.apply(new MetadataProvenance(2, 0, 0, true));

        CompletableFuture<Void> oldDeletion = new CompletableFuture<>();
        CompletableFuture<Void> newDeletion = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.unregisterBehavior = attempt -> {
            if (attempt == 1) {
                return oldDeletion;
            }
            if (attempt == 2) {
                return newDeletion;
            }
            return CompletableFuture.completedFuture(null);
        };
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1,
                lifecycle,
                new RecordingProducerStateStore(),
                (message, cause) -> { },
                5,
                10);
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(
                deletionDelta,
                deletedImage,
                loaderManifest(deletedImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.unregisterAttempts.get() == 1,
                5_000,
                "Expected the old leadership deletion to remain pending");

        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(2), 2));
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 3));
        TestUtils.waitForCondition(
                () -> lifecycle.unregisterAttempts.get() == 2,
                5_000,
                "Expected a deletion attempt owned by the new generation");

        oldDeletion.complete(null);
        newDeletion.completeExceptionally(new RuntimeException("new generation retry"));
        TestUtils.waitForCondition(
                () -> lifecycle.unregisterAttempts.get() == 3,
                5_000,
                "Expected the new generation desired state to survive the old callback");
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

    private static MetadataImage emptyImage(long offset) {
        return new MetadataImage(
                new MetadataProvenance(offset, 0, 0, true),
                FeaturesImage.EMPTY,
                ClusterImage.EMPTY,
                TopicsImage.EMPTY,
                ConfigurationsImage.EMPTY,
                ClientQuotasImage.EMPTY,
                ProducerIdsImage.EMPTY,
                AclsImage.EMPTY,
                ScramImage.EMPTY,
                DelegationTokenImage.EMPTY);
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
