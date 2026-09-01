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
import org.apache.kafka.storage.diskless.DisklessProducerStateLifecycle;
import org.apache.kafka.storage.diskless.DisklessProducerStateLifecycle.ManagedProducerStateTopic;
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

class DisklessTopicLifecycleReconcilerTest {

    private record Reconciliation(
            String topicName,
            Uuid topicId,
            int partitions,
            Map<String, String> properties,
            long sourceRevision) {
    }

    private static final class RecordingLifecycle implements DisklessTopicLifecycle {
        final List<Reconciliation> reconciliations = new CopyOnWriteArrayList<>();
        final List<String> deletedTopics = new CopyOnWriteArrayList<>();
        final AtomicInteger reconcileAttempts = new AtomicInteger();
        final AtomicInteger deleteAttempts = new AtomicInteger();
        final AtomicInteger listAttempts = new AtomicInteger();
        IntFunction<CompletableFuture<List<ManagedTopic>>> listBehavior =
                attempt -> CompletableFuture.completedFuture(List.of());
        IntFunction<CompletableFuture<Void>> reconcileBehavior = attempt -> CompletableFuture.completedFuture(null);
        IntFunction<CompletableFuture<Void>> deleteBehavior = attempt -> CompletableFuture.completedFuture(null);

        @Override
        public CompletableFuture<List<ManagedTopic>> listManagedTopics() {
            return listBehavior.apply(listAttempts.incrementAndGet());
        }

        @Override
        public CompletableFuture<Void> reconcileTopic(
                String topicName,
                Uuid topicId,
                int partitions,
                Map<String, String> properties,
                long sourceRevision) {
            reconciliations.add(new Reconciliation(
                    topicName, topicId, partitions, Map.copyOf(properties), sourceRevision));
            return reconcileBehavior.apply(reconcileAttempts.incrementAndGet());
        }

        @Override
        public CompletableFuture<Void> deleteTopic(String topicName, Uuid topicId) {
            deletedTopics.add(topicName + ":" + topicId);
            return deleteBehavior.apply(deleteAttempts.incrementAndGet());
        }

        @Override
        public void close() {
        }
    }

    private static final class RecordingProducerStateLifecycle implements DisklessProducerStateLifecycle {
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
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> { });

        MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        delta.replay(new TopicRecord().setName(disklessTopic).setTopicId(topicId));
        for (int p = 0; p < partitions; p++) {
            delta.replay(partitionRecord(topicId, p));
        }
        delta.replay(disklessConfigRecord(disklessTopic));

        reconciler.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));
        assertTrue(lifecycle.reconciliations.isEmpty(), "No reconciliation expected before becoming leader");
        assertTrue(lifecycle.deletedTopics.isEmpty(), "No cleanup expected before becoming leader");
        assertTrue(producerStateLifecycle.deletedTopics.isEmpty(), "No calls expected before becoming leader");

        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        TestUtils.waitForCondition(
                () -> lifecycle.reconciliations.size() == 1,
                5_000, "Expected full image reconciliation after becoming leader");
        assertEquals(
                new Reconciliation(
                        disklessTopic,
                        topicId,
                        partitions,
                        Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true"),
                        10),
                lifecycle.reconciliations.get(0));
        reconciler.close();
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
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> { });
        reconciler.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                image,
                loaderManifest(image.provenance()));

        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        TestUtils.waitForCondition(
                () -> producerStateLifecycle.deletedTopics.contains(orphanTopicId.toString()),
                5_000,
                "Expected cold-start reconciliation to delete the Catalog orphan");
        assertEquals(List.of(orphanTopic + ":" + orphanTopicId), lifecycle.deletedTopics);
        assertEquals(List.of(orphanTopicId.toString()), producerStateLifecycle.deletedTopics);
        assertTrue(lifecycle.deletedTopics.stream()
                .noneMatch(topic -> topic.endsWith(currentTopicId.toString())));
        reconciler.close();
    }

    @Test
    void testFullImageDeletesProducerStateOrphanAfterCatalogIsGone() throws Exception {
        String orphanTopic = "producer-state-orphan";
        Uuid orphanTopicId = Uuid.randomUuid();
        MetadataImage image = emptyImage(10);
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        producerStateLifecycle.listBehavior = attempt -> CompletableFuture.completedFuture(List.of(
                new ManagedProducerStateTopic(orphanTopic, orphanTopicId, 10)));
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> { });
        reconciler.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                image,
                loaderManifest(image.provenance()));

        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        TestUtils.waitForCondition(
                () -> producerStateLifecycle.deletedTopics.contains(orphanTopicId.toString()),
                5_000,
                "Expected cold-start reconciliation to delete orphan producer state");
        assertTrue(lifecycle.deletedTopics.isEmpty(),
                "A producer-state-only orphan must not fabricate a Catalog deletion");
        reconciler.close();
    }

    @Test
    void testLaggingControllerDefersProducerStateOrphanFromNewerRevision() throws Exception {
        String topicName = "newer-producer-state-topic";
        Uuid topicId = Uuid.randomUuid();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        producerStateLifecycle.listBehavior = attempt -> CompletableFuture.completedFuture(List.of(
                new ManagedProducerStateTopic(topicName, topicId, 11)));
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1,
                lifecycle,
                producerStateLifecycle,
                (message, cause) -> { },
                10,
                100,
                10_000,
                10,
                1_000);
        MetadataImage staleImage = emptyImage(10);
        reconciler.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                staleImage,
                loaderManifest(staleImage.provenance()));
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        TestUtils.waitForCondition(
                () -> producerStateLifecycle.listAttempts.get() == 1,
                5_000,
                "Expected producer-state inventory for the stale image");
        assertTrue(producerStateLifecycle.deletedTopics.isEmpty(),
                "A lagging controller must not delete producer state from a newer revision");

        MetadataImage caughtUpImage = emptyImage(11);
        reconciler.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(staleImage).build(),
                caughtUpImage,
                loaderManifest(caughtUpImage.provenance()));
        TestUtils.waitForCondition(
                () -> producerStateLifecycle.deletedTopics.contains(topicId.toString()),
                5_000,
                "Expected cleanup after the controller reaches the producer-state source revision");
        assertTrue(lifecycle.deletedTopics.isEmpty());
        reconciler.close();
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
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        producerStateLifecycle.listBehavior = attempt -> CompletableFuture.completedFuture(List.of(
                new ManagedProducerStateTopic(topicName, topicId, 10)));
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> { });
        reconciler.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                image,
                loaderManifest(image.provenance()));
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        TestUtils.waitForCondition(
                () -> lifecycle.reconcileAttempts.get() == 1
                        && producerStateLifecycle.listAttempts.get() == 1,
                5_000,
                "Expected reconciliation and producer-state inventory");
        assertTrue(producerStateLifecycle.deletedTopics.isEmpty());
        assertTrue(lifecycle.deletedTopics.isEmpty());
        reconciler.close();
    }

    @Test
    void testProducerStateManifestCompletesBeforeCatalogReconciliation() throws Exception {
        String topicName = "manifest-first-topic";
        Uuid topicId = Uuid.randomUuid();
        CompletableFuture<Void> pendingManifest = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        producerStateLifecycle.reconcileBehavior = attempt -> pendingManifest;
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> { });
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        MetadataDelta delta = topicCreationDelta(
                MetadataImage.EMPTY,
                topicName,
                topicId,
                1,
                Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true"));
        MetadataImage image = delta.apply(new MetadataProvenance(10, 0, 0, true));

        reconciler.onMetadataUpdate(delta, image, loaderManifest(image.provenance()));

        TestUtils.waitForCondition(
                () -> producerStateLifecycle.reconcileAttempts.get() == 1,
                5_000,
                "Expected producer-state manifest reconciliation");
        assertEquals(0, lifecycle.reconcileAttempts.get(),
                "Catalog reconciliation must wait for the durable producer-state manifest");
        pendingManifest.complete(null);
        TestUtils.waitForCondition(
                () -> lifecycle.reconcileAttempts.get() == 1,
                5_000,
                "Expected Catalog reconciliation after the manifest becomes durable");
        reconciler.close();
    }

    @Test
    void testCloseCancelsHungProducerStateInventory() throws Exception {
        CompletableFuture<List<ManagedProducerStateTopic>> hungInventory = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        producerStateLifecycle.listBehavior = attempt -> hungInventory;
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> { });
        MetadataImage image = emptyImage(10);
        reconciler.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                image,
                loaderManifest(image.provenance()));
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        TestUtils.waitForCondition(
                () -> producerStateLifecycle.listAttempts.get() == 1,
                5_000,
                "Expected a producer-state inventory attempt");

        reconciler.close();

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
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> { });
        MetadataImage initialImage = emptyImage(0);
        reconciler.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                initialImage,
                loaderManifest(initialImage.provenance()));
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
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
        reconciler.onMetadataUpdate(
                creationDelta,
                createdImage,
                loaderManifest(createdImage.provenance()));
        oldImageInventory.complete(List.of(new ManagedTopic(topicName, topicId, 1)));

        TestUtils.waitForCondition(
                () -> lifecycle.listAttempts.get() == 2,
                5_000,
                "Expected stale result to trigger inventory against the newer image");
        newerImageInventory.complete(List.of(new ManagedTopic(topicName, topicId, 1)));
        assertTrue(lifecycle.deletedTopics.isEmpty(), "Stale inventory must not delete a current topic");
        assertTrue(producerStateLifecycle.deletedTopics.isEmpty());
        reconciler.close();
    }

    @Test
    void testUnrelatedMetadataRevisionDoesNotReconcileEveryDisklessTopic() throws Exception {
        String topicName = "stable-topic";
        Uuid topicId = Uuid.randomUuid();
        CompletableFuture<List<ManagedTopic>> firstInventory = new CompletableFuture<>();
        CompletableFuture<List<ManagedTopic>> secondInventory = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.listBehavior = attempt -> attempt == 1 ? firstInventory : secondInventory;
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, new RecordingProducerStateLifecycle(), (message, cause) -> { });
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        MetadataDelta creationDelta = topicCreationDelta(
                MetadataImage.EMPTY,
                topicName,
                topicId,
                1,
                Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true"));
        MetadataImage createdImage = creationDelta.apply(new MetadataProvenance(1, 0, 0, true));
        reconciler.onMetadataUpdate(
                creationDelta,
                createdImage,
                loaderManifest(createdImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.reconciliations.size() == 1 && lifecycle.listAttempts.get() == 1,
                5_000,
                "Expected initial reconciliation and inventory");

        MetadataDelta unrelatedDelta = new MetadataDelta.Builder().setImage(createdImage).build();
        MetadataImage unrelatedImage = unrelatedDelta.apply(new MetadataProvenance(2, 0, 0, true));
        reconciler.onMetadataUpdate(
                unrelatedDelta,
                unrelatedImage,
                loaderManifest(unrelatedImage.provenance()));
        firstInventory.complete(List.of(new ManagedTopic(topicName, topicId, 1)));
        TestUtils.waitForCondition(
                () -> lifecycle.listAttempts.get() == 2,
                5_000,
                "Expected inventory to restart against the newer image");
        secondInventory.complete(List.of(new ManagedTopic(topicName, topicId, 1)));

        assertEquals(1, lifecycle.reconciliations.size(),
                "An unrelated image revision must not rewrite every Catalog topic");
        assertEquals(1, lifecycle.reconciliations.get(0).sourceRevision());
        reconciler.close();
    }

    @Test
    void testLaggingControllerDefersCatalogOrphanFromNewerRevision() throws Exception {
        String topicName = "newer-catalog-topic";
        Uuid topicId = Uuid.randomUuid();
        MetadataImage staleImage = emptyImage(10);
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.listBehavior = attempt -> CompletableFuture.completedFuture(List.of(
                new ManagedTopic(topicName, topicId, 11)));
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> { });
        reconciler.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                staleImage,
                loaderManifest(staleImage.provenance()));

        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        assertEquals(1, lifecycle.listAttempts.get());
        assertTrue(lifecycle.deletedTopics.isEmpty(),
                "A lagging controller must not delete Catalog state from a newer KRaft revision");
        assertTrue(producerStateLifecycle.deletedTopics.isEmpty());

        MetadataImage caughtUpImage = emptyImage(11);
        reconciler.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(staleImage).build(),
                caughtUpImage,
                loaderManifest(caughtUpImage.provenance()));
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(2), 2));
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 3));

        TestUtils.waitForCondition(
                () -> producerStateLifecycle.deletedTopics.contains(topicId.toString()),
                5_000,
                "Expected deletion once the controller image reaches the Catalog source revision");
        assertEquals(List.of(topicName + ":" + topicId), lifecycle.deletedTopics);
        reconciler.close();
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
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> { });

        // Seed the state which can result when revision 11 arrives while a revision 10 full-image
        // reconciliation is still being prepared, then run the stale snapshot deterministically.
        reconciler.onMetadataUpdate(
                creationDelta,
                newerImage,
                loaderManifest(newerImage.provenance()));
        reconciler.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(newerImage).build(),
                staleImage,
                loaderManifest(staleImage.provenance()));

        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        assertTrue(lifecycle.deletedTopics.isEmpty(),
                "A stale full image must not turn a newer remembered reconciliation into a deletion");
        assertTrue(producerStateLifecycle.deletedTopics.isEmpty());

        MetadataImage caughtUpImage = emptyImage(11);
        reconciler.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(staleImage).build(),
                caughtUpImage,
                loaderManifest(caughtUpImage.provenance()));
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(2), 2));
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 3));

        TestUtils.waitForCondition(
                () -> producerStateLifecycle.deletedTopics.contains(topicId.toString()),
                5_000,
                "Expected deletion once a full image reaches the remembered source revision");
        reconciler.close();
    }

    @Test
    void testCatalogInventoryFailureRetriesWhileImageRemainsCurrent() throws Exception {
        String orphanTopic = "orphan-topic";
        Uuid orphanTopicId = Uuid.randomUuid();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.listBehavior = attempt -> attempt == 1
                ? CompletableFuture.failedFuture(new RuntimeException("temporary inventory failure"))
                : CompletableFuture.completedFuture(List.of(new ManagedTopic(orphanTopic, orphanTopicId, 10)));
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        List<Throwable> faults = new CopyOnWriteArrayList<>();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1,
                lifecycle,
                producerStateLifecycle,
                (message, cause) -> faults.add(cause),
                5,
                10);
        MetadataImage image = emptyImage(10);
        reconciler.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                image,
                loaderManifest(image.provenance()));
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        TestUtils.waitForCondition(
                () -> producerStateLifecycle.deletedTopics.contains(orphanTopicId.toString()),
                5_000,
                "Expected inventory retry to discover and delete the orphan");
        assertEquals(2, lifecycle.listAttempts.get());
        assertEquals(1, faults.size());
        reconciler.close();
    }

    @Test
    void testPeriodicInventoryDeletesOrphanCreatedAfterEmptyScan() throws Exception {
        String orphanTopic = "late-orphan";
        Uuid orphanTopicId = Uuid.randomUuid();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.listBehavior = attempt -> attempt == 2
                ? CompletableFuture.completedFuture(List.of(new ManagedTopic(orphanTopic, orphanTopicId, 10)))
                : CompletableFuture.completedFuture(List.of());
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1,
                lifecycle,
                producerStateLifecycle,
                (message, cause) -> { },
                5,
                10,
                10,
                10,
                100);
        MetadataImage image = emptyImage(10);
        reconciler.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                image,
                loaderManifest(image.provenance()));
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        TestUtils.waitForCondition(
                () -> producerStateLifecycle.deletedTopics.contains(orphanTopicId.toString()),
                5_000,
                "Expected the next active-controller inventory to delete a late orphan");
        assertTrue(lifecycle.listAttempts.get() >= 2);
        assertTrue(lifecycle.deletedTopics.contains(orphanTopic + ":" + orphanTopicId));
        reconciler.close();
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
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        List<Throwable> faults = new CopyOnWriteArrayList<>();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1,
                lifecycle,
                producerStateLifecycle,
                (message, cause) -> faults.add(cause),
                5,
                10,
                10,
                10,
                20);
        MetadataImage image = emptyImage(10);
        reconciler.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                image,
                loaderManifest(image.provenance()));
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        TestUtils.waitForCondition(
                () -> producerStateLifecycle.deletedTopics.contains(orphanTopicId.toString()),
                5_000,
                "Expected inventory to recover after one timed-out call");
        assertTrue(lifecycle.listAttempts.get() >= 2);
        assertTrue(faults.stream().anyMatch(TimeoutException.class::isInstance));
        assertTrue(hungInventory.isCancelled(), "Timed-out inventory source must be cancelled");
        reconciler.close();
    }

    @Test
    void testCloseCancelsHungCatalogInventory() throws Exception {
        CompletableFuture<List<ManagedTopic>> hungInventory = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.listBehavior = attempt -> hungInventory;
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, new RecordingProducerStateLifecycle(), (message, cause) -> { });
        MetadataImage image = emptyImage(10);
        reconciler.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                image,
                loaderManifest(image.provenance()));
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        TestUtils.waitForCondition(
                () -> lifecycle.listAttempts.get() == 1,
                5_000,
                "Expected a Catalog inventory attempt");

        reconciler.close();

        assertTrue(hungInventory.isCancelled(), "Close must cancel a hung Catalog inventory");
    }

    @Test
    void testCommittedDisklessTopicCreationReconcilesCompleteTopicState() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        int partitions = 3;
        Map<String, String> configs = Map.of(
                TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true",
                TopicConfig.RETENTION_MS_CONFIG, "12345");

        MetadataDelta delta = topicCreationDelta(MetadataImage.EMPTY, disklessTopic, topicId, partitions, configs);
        MetadataImage newImage = delta.apply(new MetadataProvenance(10, 0, 0, true));

        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> { });
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        reconciler.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));

        TestUtils.waitForCondition(
                () -> lifecycle.reconciliations.size() == 1,
                5_000, "Expected committed diskless topic reconciliation");
        assertEquals(
                new Reconciliation(disklessTopic, topicId, partitions, configs, 10),
                lifecycle.reconciliations.get(0));
        assertTrue(lifecycle.deletedTopics.isEmpty());
        assertTrue(producerStateLifecycle.deletedTopics.isEmpty());
        reconciler.close();
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
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, new RecordingProducerStateLifecycle(), (message, cause) -> { });
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        reconciler.onMetadataUpdate(creationDelta, createdImage, loaderManifest(createdImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.reconciliations.size() == 1,
                5_000,
                "Expected initial reconciliation");

        MetadataDelta configDelta = new MetadataDelta.Builder().setImage(createdImage).build();
        configDelta.replay(configRecord(disklessTopic, TopicConfig.RETENTION_MS_CONFIG, "12345"));
        MetadataImage updatedImage = configDelta.apply(new MetadataProvenance(11, 0, 0, true));
        reconciler.onMetadataUpdate(configDelta, updatedImage, loaderManifest(updatedImage.provenance()));

        TestUtils.waitForCondition(
                () -> lifecycle.reconciliations.size() == 2,
                5_000,
                "Expected config change reconciliation");
        assertEquals(
                new Reconciliation(
                        disklessTopic,
                        topicId,
                        1,
                        Map.of(
                                TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true",
                                TopicConfig.RETENTION_MS_CONFIG, "12345"),
                        11),
                lifecycle.reconciliations.get(1));
        reconciler.close();
    }

    @Test
    void testConfigChangeWhileInitialReconciliationIsPendingRunsLatestReconciliation()
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

        CompletableFuture<Void> initialReconciliation = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.reconcileBehavior = attempt -> attempt == 1
                ? initialReconciliation
                : CompletableFuture.completedFuture(null);
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, new RecordingProducerStateLifecycle(), (message, cause) -> { });
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        reconciler.onMetadataUpdate(
                creationDelta, createdImage, loaderManifest(createdImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.reconcileAttempts.get() == 1,
                5_000,
                "Expected initial reconciliation to remain pending");

        MetadataDelta configDelta = new MetadataDelta.Builder().setImage(createdImage).build();
        configDelta.replay(configRecord(
                disklessTopic, TopicConfig.RETENTION_MS_CONFIG, "12345"));
        MetadataImage updatedImage = configDelta.apply(
                new MetadataProvenance(11, 0, 0, true));
        reconciler.onMetadataUpdate(
                configDelta, updatedImage, loaderManifest(updatedImage.provenance()));

        assertEquals(1, lifecycle.reconcileAttempts.get(),
                "The newer reconciliation must remain serialized behind the initial call");
        initialReconciliation.complete(null);
        TestUtils.waitForCondition(
                () -> lifecycle.reconcileAttempts.get() == 2,
                5_000,
                "Expected the latest config snapshot after initial reconciliation completed");
        assertEquals(
                new Reconciliation(
                        disklessTopic,
                        topicId,
                        1,
                        Map.of(
                                TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true",
                                TopicConfig.RETENTION_MS_CONFIG, "12345"),
                        11),
                lifecycle.reconciliations.get(1));
        reconciler.close();
    }

    @Test
    void testManyQueuedRevisionsCoalesceToLatestReconciliation()
            throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        Map<String, String> configs = Map.of(
                TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true");
        MetadataDelta creationDelta = topicCreationDelta(
                MetadataImage.EMPTY, disklessTopic, topicId, 1, configs);
        MetadataImage createdImage = creationDelta.apply(
                new MetadataProvenance(10, 0, 0, true));

        CompletableFuture<Void> initialReconciliation = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.reconcileBehavior = attempt -> attempt == 1
                ? initialReconciliation
                : CompletableFuture.completedFuture(null);
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, new RecordingProducerStateLifecycle(), (message, cause) -> { });
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        reconciler.onMetadataUpdate(
                creationDelta, createdImage, loaderManifest(createdImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.reconcileAttempts.get() == 1,
                5_000,
                "Expected initial reconciliation to remain pending");

        MetadataImage updatedImage = createdImage;
        for (int revision = 1; revision <= 1_000; revision++) {
            MetadataDelta configDelta = new MetadataDelta.Builder().setImage(updatedImage).build();
            configDelta.replay(configRecord(
                    disklessTopic, TopicConfig.RETENTION_MS_CONFIG, Integer.toString(revision)));
            updatedImage = configDelta.apply(
                    new MetadataProvenance(10L + revision, 0, 0, true));
            reconciler.onMetadataUpdate(
                    configDelta, updatedImage, loaderManifest(updatedImage.provenance()));
        }

        assertEquals(1, lifecycle.reconcileAttempts.get(),
                "Only the current source attempt may be in flight");
        assertEquals(1, reconciler.activeTopicRunnerCountForTesting());
        assertEquals(1, reconciler.pendingTopicRevisionCountForTesting(topicId),
                "All queued revisions must collapse into one latest desired state");
        assertEquals(1, reconciler.pendingSourceAttemptCountForTesting());

        initialReconciliation.complete(null);
        TestUtils.waitForCondition(
                () -> lifecycle.reconcileAttempts.get() == 2,
                5_000,
                "Expected only the latest queued reconciliation supplier to start");
        assertEquals(2, lifecycle.reconciliations.size());
        assertEquals(
                new Reconciliation(
                        disklessTopic,
                        topicId,
                        1,
                        Map.of(
                                TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true",
                                TopicConfig.RETENTION_MS_CONFIG, "1000"),
                        1_010),
                lifecycle.reconciliations.get(1));
        TestUtils.waitForCondition(
                () -> reconciler.activeTopicRunnerCountForTesting() == 0,
                5_000,
                "Expected the bounded runner state to retire after the latest revision completed");
        reconciler.close();
    }

    @Test
    void testCreatePartitionsReconcilesCompletePartitionCountWithoutDuplicatingCreation() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        Map<String, String> configs = Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true");
        MetadataDelta creationDelta = topicCreationDelta(
                MetadataImage.EMPTY, disklessTopic, topicId, 1, configs);
        MetadataImage createdImage = creationDelta.apply(new MetadataProvenance(10, 0, 0, true));

        RecordingLifecycle lifecycle = new RecordingLifecycle();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, new RecordingProducerStateLifecycle(), (message, cause) -> { });
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        reconciler.onMetadataUpdate(creationDelta, createdImage, loaderManifest(createdImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.reconciliations.size() == 1,
                5_000, "Expected exactly one reconciliation for topic creation");

        MetadataDelta expansionDelta = new MetadataDelta.Builder().setImage(createdImage).build();
        expansionDelta.replay(partitionRecord(topicId, 1));
        expansionDelta.replay(partitionRecord(topicId, 2));
        MetadataImage expandedImage = expansionDelta.apply(new MetadataProvenance(11, 0, 0, true));
        reconciler.onMetadataUpdate(expansionDelta, expandedImage, loaderManifest(expandedImage.provenance()));

        TestUtils.waitForCondition(
                () -> lifecycle.reconciliations.size() == 2,
                5_000, "Expected CreatePartitions to refresh catalog reconciliation");
        assertEquals(List.of(1, 3), lifecycle.reconciliations.stream()
                .map(Reconciliation::partitions)
                .toList());

        MetadataDelta unchangedCountDelta = new MetadataDelta.Builder().setImage(expandedImage).build();
        unchangedCountDelta.replay(partitionRecord(topicId, 0));
        MetadataImage unchangedCountImage = unchangedCountDelta.apply(
                new MetadataProvenance(12, 0, 0, true));
        reconciler.onMetadataUpdate(
                unchangedCountDelta,
                unchangedCountImage,
                loaderManifest(unchangedCountImage.provenance()));
        Thread.sleep(50);
        assertEquals(2, lifecycle.reconciliations.size(),
                "Partition metadata updates with an unchanged count must not reconcile");
        reconciler.close();
    }

    @Test
    void testCommittedNonDisklessTopicCreationIgnored() {
        String topicName = "normal-topic";
        Uuid topicId = Uuid.randomUuid();
        Map<String, String> configs = Map.of(TopicConfig.RETENTION_MS_CONFIG, "12345");
        MetadataDelta delta = topicCreationDelta(MetadataImage.EMPTY, topicName, topicId, 1, configs);
        MetadataImage newImage = delta.apply(new MetadataProvenance(10, 0, 0, true));

        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> { });
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        reconciler.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));

        assertTrue(lifecycle.reconciliations.isEmpty());
        reconciler.close();
    }

    @Test
    void testDeletionSupersedesBlockedReconciliation() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        Map<String, String> configs = Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true");
        MetadataDelta creationDelta = topicCreationDelta(
                MetadataImage.EMPTY, disklessTopic, topicId, 1, configs);
        MetadataImage createdImage = creationDelta.apply(new MetadataProvenance(1, 0, 0, true));
        MetadataDelta deletionDelta = new MetadataDelta.Builder().setImage(createdImage).build();
        deletionDelta.replay(new RemoveTopicRecord().setTopicId(topicId));
        MetadataImage deletedImage = deletionDelta.apply(new MetadataProvenance(2, 0, 0, true));

        CompletableFuture<Void> reconciliationGate = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.reconcileBehavior = attempt -> reconciliationGate;
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> { });
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        reconciler.onMetadataUpdate(creationDelta, createdImage, loaderManifest(createdImage.provenance()));
        reconciler.onMetadataUpdate(deletionDelta, deletedImage, loaderManifest(deletedImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.reconciliations.size() == 1,
                5_000, "Expected reconciliation to start");
        TestUtils.waitForCondition(
                () -> lifecycle.deletedTopics.size() == 1
                        && producerStateLifecycle.deletedTopics.size() == 1,
                5_000, "Expected both deletion branches to complete while reconciliation remains blocked");
        assertTrue(reconciliationGate.isCancelled(),
                "Deletion must cancel the superseded reconciliation source");
        assertEquals(List.of(disklessTopic + ":" + topicId), lifecycle.deletedTopics);
        assertEquals(List.of(topicId.toString()), producerStateLifecycle.deletedTopics);

        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(2), 2));
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 3));
        Thread.sleep(50);
        assertEquals(1, lifecycle.deleteAttempts.get(),
                "A completed durable delete must retire the desired deletion state");
        assertEquals(1, producerStateLifecycle.deleteAttempts.get());
        reconciler.close();
    }

    @Test
    void testDeletionFenceMakesLateReconciliationCompletionHarmless() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        Map<String, String> configs = Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true");
        MetadataDelta creationDelta = topicCreationDelta(
                MetadataImage.EMPTY, disklessTopic, topicId, 1, configs);
        MetadataImage createdImage = creationDelta.apply(new MetadataProvenance(1, 0, 0, true));
        MetadataDelta deletionDelta = new MetadataDelta.Builder().setImage(createdImage).build();
        deletionDelta.replay(new RemoveTopicRecord().setTopicId(topicId));
        MetadataImage deletedImage = deletionDelta.apply(new MetadataProvenance(2, 0, 0, true));

        CompletableFuture<Void> reconciliationGate = new CompletableFuture<>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return false;
            }
        };
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.reconcileBehavior = attempt -> reconciliationGate;
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> { });
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        reconciler.onMetadataUpdate(creationDelta, createdImage, loaderManifest(createdImage.provenance()));
        reconciler.onMetadataUpdate(deletionDelta, deletedImage, loaderManifest(deletedImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.deleteAttempts.get() == 1
                        && producerStateLifecycle.deleteAttempts.get() == 1,
                5_000, "Expected immediate deletion while reconciliation remains blocked");

        reconciliationGate.complete(null);
        Thread.sleep(50);
        assertEquals(1, lifecycle.reconcileAttempts.get());

        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(2), 2));
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 3));
        Thread.sleep(50);
        assertEquals(1, lifecycle.deleteAttempts.get(),
                "The lifecycle provider owns fencing late reconciliation completion");
        assertEquals(1, producerStateLifecycle.deleteAttempts.get());
        reconciler.close();
    }

    @Test
    void testBlockedTopicDoesNotBlockAnotherTopic() throws Exception {
        String firstTopic = "first-topic";
        Uuid firstTopicId = Uuid.randomUuid();
        Map<String, String> configs = Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true");
        MetadataDelta firstDelta = topicCreationDelta(
                MetadataImage.EMPTY, firstTopic, firstTopicId, 1, configs);
        MetadataImage firstImage = firstDelta.apply(new MetadataProvenance(1, 0, 0, true));

        CompletableFuture<Void> firstReconciliation = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.reconcileBehavior = attempt -> attempt == 1
                ? firstReconciliation
                : CompletableFuture.completedFuture(null);
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, new RecordingProducerStateLifecycle(), (message, cause) -> { }, 5, 10);
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        reconciler.onMetadataUpdate(firstDelta, firstImage, loaderManifest(firstImage.provenance()));

        String secondTopic = "second-topic";
        Uuid secondTopicId = Uuid.randomUuid();
        MetadataDelta secondDelta = topicCreationDelta(firstImage, secondTopic, secondTopicId, 1, configs);
        MetadataImage secondImage = secondDelta.apply(new MetadataProvenance(2, 0, 0, true));
        reconciler.onMetadataUpdate(secondDelta, secondImage, loaderManifest(secondImage.provenance()));

        TestUtils.waitForCondition(
                () -> lifecycle.reconciliations.size() == 2,
                5_000, "Expected second topic to reconcile while first topic remains blocked");
        assertEquals(List.of(firstTopic, secondTopic), lifecycle.reconciliations.stream()
                .map(Reconciliation::topicName)
                .toList());
        firstReconciliation.complete(null);
        reconciler.close();
    }

    @Test
    void testReconciliationKeepsRetryingBeyondThreeAttempts() throws Exception {
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
        lifecycle.reconcileBehavior = attempt -> attempt < 5
                ? CompletableFuture.failedFuture(new RuntimeException("transient failure"))
                : CompletableFuture.completedFuture(null);
        List<Throwable> faults = new CopyOnWriteArrayList<>();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1,
                lifecycle,
                new RecordingProducerStateLifecycle(),
                (message, cause) -> faults.add(cause),
                5,
                10);
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        reconciler.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));

        TestUtils.waitForCondition(
                () -> lifecycle.reconcileAttempts.get() == 5,
                5_000, "Expected reconciliation retry to recover");
        assertEquals(4, faults.size());
        reconciler.close();
    }

    @Test
    void testRepeatedSynchronousReconciliationFailuresKeepOneOperationSlot() throws Exception {
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
        lifecycle.reconcileBehavior = attempt -> {
            if (attempt <= 100) {
                throw new RuntimeException("synchronous failure " + attempt);
            }
            return terminalAttempt;
        };
        List<Throwable> faults = new CopyOnWriteArrayList<>();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1,
                lifecycle,
                new RecordingProducerStateLifecycle(),
                (message, cause) -> faults.add(cause),
                1,
                1);
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        reconciler.onMetadataUpdate(delta, image, loaderManifest(image.provenance()));

        TestUtils.waitForCondition(
                () -> lifecycle.reconcileAttempts.get() == 101,
                5_000,
                "Expected the bounded runner to reach the pending terminal attempt");
        assertEquals(1, reconciler.activeTopicRunnerCountForTesting());
        assertEquals(0, reconciler.pendingTopicRevisionCountForTesting(topicId));
        assertEquals(1, reconciler.pendingSourceAttemptCountForTesting());
        assertEquals(0, reconciler.pendingTopicRetryDelayCountForTesting());
        assertEquals(100, faults.size());

        terminalAttempt.complete(null);
        TestUtils.waitForCondition(
                () -> reconciler.activeTopicRunnerCountForTesting() == 0,
                5_000,
                "Expected the runner to retire after recovery");
        assertEquals(0, reconciler.pendingSourceAttemptCountForTesting());
        reconciler.close();
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
        lifecycle.reconcileBehavior = attempt -> attempt == 1
                ? CompletableFuture.failedFuture(new RuntimeException("old controller attempt failed"))
                : CompletableFuture.completedFuture(null);
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> { }, 10_000, 10_000);
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        reconciler.onMetadataUpdate(firstDelta, firstImage, loaderManifest(firstImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.reconcileAttempts.get() == 1
                        && reconciler.pendingTopicRetryDelayCountForTesting() == 1,
                5_000, "Expected the old generation to wait on one retry delay");

        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(2), 2));
        assertEquals(0, reconciler.activeTopicRunnerCountForTesting());
        assertEquals(0, reconciler.pendingTopicRetryDelayCountForTesting(),
                "Leadership loss must detach and cancel the old retry delay");

        String secondTopic = "second-topic";
        Uuid secondTopicId = Uuid.randomUuid();
        MetadataDelta secondDelta = topicCreationDelta(firstImage, secondTopic, secondTopicId, 2, configs);
        MetadataImage secondImage = secondDelta.apply(new MetadataProvenance(2, 0, 0, true));
        reconciler.onMetadataUpdate(secondDelta, secondImage, loaderManifest(secondImage.provenance()));
        assertEquals(1, lifecycle.reconcileAttempts.get(), "Old leadership must not start a retry");

        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 3));

        TestUtils.waitForCondition(
                () -> lifecycle.reconciliations.size() == 3,
                5_000, "Expected new leadership generation to reconcile");
        assertEquals(2, lifecycle.reconciliations.stream()
                .filter(reconciliation -> reconciliation.topicName().equals(firstTopic))
                .count());
        assertEquals(1, lifecycle.reconciliations.stream()
                .filter(reconciliation -> reconciliation.topicName().equals(secondTopic))
                .count());
        reconciler.close();
    }

    @Test
    void testLeadershipLossCancelsPendingReconciliationAndNewLeaderReconciles() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        Map<String, String> configs = Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true");
        MetadataDelta delta = topicCreationDelta(
                MetadataImage.EMPTY, disklessTopic, topicId, 1, configs);
        MetadataImage image = delta.apply(new MetadataProvenance(1, 0, 0, true));

        List<CompletableFuture<Void>> reconciliationSources = new CopyOnWriteArrayList<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.reconcileBehavior = attempt -> {
            CompletableFuture<Void> source = new CompletableFuture<>();
            reconciliationSources.add(source);
            return source;
        };
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, new RecordingProducerStateLifecycle(), (message, cause) -> { });
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        reconciler.onMetadataUpdate(delta, image, loaderManifest(image.provenance()));

        for (int generation = 0; generation < 3; generation++) {
            int expectedAttempts = generation + 1;
            TestUtils.waitForCondition(
                    () -> lifecycle.reconcileAttempts.get() == expectedAttempts,
                    5_000,
                    "Expected a pending source for leadership generation " + generation);
            reconciler.onControllerChange(new LeaderAndEpoch(
                    OptionalInt.of(2), generation * 2 + 2));
            assertTrue(reconciliationSources.get(generation).isCancelled(),
                    "Leadership loss must cancel each retired generation's source");
            assertEquals(0, reconciler.activeTopicRunnerCountForTesting());
            if (generation < 2) {
                reconciler.onControllerChange(new LeaderAndEpoch(
                        OptionalInt.of(1), generation * 2 + 3));
            }
        }

        assertEquals(3, lifecycle.reconcileAttempts.get());
        reconciler.close();
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
        lifecycle.deleteBehavior = attempt -> catalogDeletion;
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        producerStateLifecycle.deleteBehavior = attempt -> snapshotDeletion;
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> { });
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        reconciler.onMetadataUpdate(
                deletionDelta,
                deletedImage,
                loaderManifest(deletedImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.deleteAttempts.get() == 1
                        && producerStateLifecycle.deleteAttempts.get() == 1,
                5_000,
                "Expected both deletion branches to remain pending");

        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(2), 2));

        assertTrue(catalogDeletion.isCancelled(),
                "Leadership loss must cancel the pending Catalog source");
        assertTrue(snapshotDeletion.isCancelled(),
                "Leadership loss must cancel the pending producer-state source");
        assertEquals(0, reconciler.activeTopicRunnerCountForTesting());
        reconciler.close();
    }

    @Test
    void testCloseCancelsPendingReconciliationAndFencesRetry() throws Exception {
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
        lifecycle.reconcileBehavior = attempt -> firstAttempt;
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, new RecordingProducerStateLifecycle(), (message, cause) -> { }, 10, 20);
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        reconciler.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.reconcileAttempts.get() == 1,
                5_000, "Expected first reconciliation attempt");

        reconciler.close();
        assertTrue(firstAttempt.isCancelled(), "Close must cancel the current source attempt");
        assertEquals(1, lifecycle.reconcileAttempts.get());
    }

    @Test
    void testCloseCancelsPendingReconciliationRetryDelay() throws Exception {
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
        lifecycle.reconcileBehavior = attempt -> CompletableFuture.failedFuture(
                new RuntimeException("reconciliation unavailable"));
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1,
                lifecycle,
                new RecordingProducerStateLifecycle(),
                (message, cause) -> { },
                10_000,
                10_000);
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        reconciler.onMetadataUpdate(delta, image, loaderManifest(image.provenance()));
        TestUtils.waitForCondition(
                () -> reconciler.pendingTopicRetryDelayCountForTesting() == 1,
                5_000,
                "Expected one pending retry delay before close");

        reconciler.close();

        assertEquals(0, reconciler.activeTopicRunnerCountForTesting());
        assertEquals(0, reconciler.pendingTopicRetryDelayCountForTesting());
        assertEquals(1, lifecycle.reconcileAttempts.get());
    }

    @Test
    void testDisklessTopicDeletionDeletesCatalogAndCleansProducerState() throws Exception {
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
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> { });
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        reconciler.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));

        TestUtils.waitForCondition(
                () -> producerStateLifecycle.deletedTopics.size() == 1,
                5_000, "Expected diskless topic cleanup");
        assertEquals(List.of(disklessTopic + ":" + topicId), lifecycle.deletedTopics);
        assertEquals(List.of(topicId.toString()), producerStateLifecycle.deletedTopics);
        reconciler.close();
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
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> { });
        reconciler.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));
        assertTrue(lifecycle.deletedTopics.isEmpty());
        assertTrue(producerStateLifecycle.deletedTopics.isEmpty());

        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        TestUtils.waitForCondition(
                () -> producerStateLifecycle.deletedTopics.size() == 1,
                5_000, "Expected passive deletion to reconcile after becoming leader");
        assertEquals(List.of(disklessTopic + ":" + topicId), lifecycle.deletedTopics);
        reconciler.close();
    }

    @Test
    void testPassiveDeletionHistoryIsBoundedAndRetainsMostRecentEntries() throws Exception {
        List<String> topicNames = List.of("first-topic", "second-topic", "third-topic");
        List<Uuid> topicIds = List.of(Uuid.randomUuid(), Uuid.randomUuid(), Uuid.randomUuid());
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1,
                lifecycle,
                producerStateLifecycle,
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
            reconciler.onMetadataUpdate(
                    deletionDelta,
                    deletedImage,
                    loaderManifest(deletedImage.provenance()));
        }

        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        TestUtils.waitForCondition(
                () -> producerStateLifecycle.deletedTopics.size() == 2,
                5_000, "Expected only the bounded passive deletion history to reconcile");
        assertEquals(
                Set.of(topicIds.get(1).toString(), topicIds.get(2).toString()),
                Set.copyOf(producerStateLifecycle.deletedTopics));
        assertEquals(
                Set.of(
                        topicNames.get(1) + ":" + topicIds.get(1),
                        topicNames.get(2) + ":" + topicIds.get(2)),
                Set.copyOf(lifecycle.deletedTopics));
        reconciler.close();
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
        lifecycle.deleteBehavior = attempt -> attempt == 1
                ? oldLeadershipCatalogAttempt
                : CompletableFuture.completedFuture(null);
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1,
                lifecycle,
                producerStateLifecycle,
                (message, cause) -> { },
                5,
                10,
                1);
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        reconciler.onMetadataUpdate(
                activeDeletionDelta,
                activeDeletedImage,
                loaderManifest(activeDeletedImage.provenance()));
        TestUtils.waitForCondition(
                () -> producerStateLifecycle.deletedTopics.contains(activeTopicId.toString()),
                5_000, "Expected active snapshot deletion to proceed independently");

        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(2), 2));
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
            reconciler.onMetadataUpdate(
                    passiveDeletionDelta,
                    passiveDeletedImage,
                    loaderManifest(passiveDeletedImage.provenance()));
        }

        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 3));
        oldLeadershipCatalogAttempt.completeExceptionally(new RuntimeException("old leadership failed"));
        TestUtils.waitForCondition(
                () -> lifecycle.deletedTopics.stream()
                        .filter(entry -> entry.equals(activeTopic + ":" + activeTopicId))
                        .count() == 2,
                5_000, "Expected new leadership to retry the protected active deletion");
        reconciler.close();
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
        lifecycle.deleteBehavior = attempt -> {
            if (attempt == 1) {
                return oldDeletion;
            }
            if (attempt == 2) {
                return newDeletion;
            }
            return CompletableFuture.completedFuture(null);
        };
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1,
                lifecycle,
                new RecordingProducerStateLifecycle(),
                (message, cause) -> { },
                5,
                10);
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        reconciler.onMetadataUpdate(
                deletionDelta,
                deletedImage,
                loaderManifest(deletedImage.provenance()));
        TestUtils.waitForCondition(
                () -> lifecycle.deleteAttempts.get() == 1,
                5_000,
                "Expected the old leadership deletion to remain pending");

        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(2), 2));
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 3));
        TestUtils.waitForCondition(
                () -> lifecycle.deleteAttempts.get() == 2,
                5_000,
                "Expected a deletion attempt owned by the new generation");

        oldDeletion.complete(null);
        newDeletion.completeExceptionally(new RuntimeException("new generation retry"));
        TestUtils.waitForCondition(
                () -> lifecycle.deleteAttempts.get() == 3,
                5_000,
                "Expected the new generation desired state to survive the old callback");
        reconciler.close();
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
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> { });
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        reconciler.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));

        assertTrue(lifecycle.deletedTopics.isEmpty());
        assertTrue(producerStateLifecycle.deletedTopics.isEmpty());
        reconciler.close();
    }

    @Test
    void testInternalTopicWithDisklessConfigIsNeverReconciled() throws Exception {
        String internalTopic = "__consumer_offsets";
        Uuid topicId = Uuid.randomUuid();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> { });
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        MetadataDelta delta = topicCreationDelta(
                MetadataImage.EMPTY,
                internalTopic,
                topicId,
                1,
                Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true"));
        MetadataImage image = delta.apply(new MetadataProvenance(10, 0, 0, true));
        reconciler.onMetadataUpdate(delta, image, loaderManifest(image.provenance()));

        Thread.sleep(50);
        assertEquals(0, producerStateLifecycle.reconcileAttempts.get());
        assertEquals(0, lifecycle.reconcileAttempts.get());
        reconciler.close();
    }

    @Test
    void testHungReconciliationIsCancelledAndRetriedAfterTimeout() throws Exception {
        String topicName = "timed-out-reconciliation";
        Uuid topicId = Uuid.randomUuid();
        CompletableFuture<Void> hungManifest = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        producerStateLifecycle.reconcileBehavior = attempt -> attempt == 1
                ? hungManifest
                : CompletableFuture.completedFuture(null);
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1,
                lifecycle,
                producerStateLifecycle,
                (message, cause) -> failures.add(cause),
                5,
                10,
                10_000,
                10_000,
                10_000,
                25);
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        MetadataDelta delta = topicCreationDelta(
                MetadataImage.EMPTY,
                topicName,
                topicId,
                1,
                Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true"));
        MetadataImage image = delta.apply(new MetadataProvenance(10, 0, 0, true));
        reconciler.onMetadataUpdate(delta, image, loaderManifest(image.provenance()));

        TestUtils.waitForCondition(
                () -> producerStateLifecycle.reconcileAttempts.get() >= 2
                        && lifecycle.reconcileAttempts.get() == 1,
                5_000,
                "Expected timed-out producer-state reconciliation to be retried");
        assertTrue(hungManifest.isCancelled());
        TestUtils.waitForCondition(
                () -> failures.stream().anyMatch(TimeoutException.class::isInstance),
                5_000,
                "Expected the reconciliation timeout to be reported");
        reconciler.close();
    }

    @Test
    void testHungDeletionIsCancelledAndRetriedAfterTimeout() throws Exception {
        String topicName = "timed-out-deletion";
        Uuid topicId = Uuid.randomUuid();
        MetadataImage oldImage = new MetadataImage(
                new MetadataProvenance(1, 0, 0, true),
                FeaturesImage.EMPTY,
                ClusterImage.EMPTY,
                topicsImage(topicName, topicId, 1),
                configsImageWithDisklessEnabled(topicName),
                ClientQuotasImage.EMPTY,
                ProducerIdsImage.EMPTY,
                AclsImage.EMPTY,
                ScramImage.EMPTY,
                DelegationTokenImage.EMPTY);
        MetadataDelta delta = new MetadataDelta.Builder().setImage(oldImage).build();
        delta.replay(new RemoveTopicRecord().setTopicId(topicId));
        MetadataImage image = delta.apply(new MetadataProvenance(2, 0, 0, true));
        CompletableFuture<Void> hungCatalogDeletion = new CompletableFuture<>();
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.deleteBehavior = attempt -> attempt == 1
                ? hungCatalogDeletion
                : CompletableFuture.completedFuture(null);
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1,
                lifecycle,
                producerStateLifecycle,
                (message, cause) -> failures.add(cause),
                5,
                10,
                10_000,
                10_000,
                10_000,
                25);
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));

        reconciler.onMetadataUpdate(delta, image, loaderManifest(image.provenance()));

        TestUtils.waitForCondition(
                () -> lifecycle.deleteAttempts.get() >= 2,
                5_000,
                "Expected timed-out catalog deletion to be retried");
        assertTrue(hungCatalogDeletion.isCancelled());
        TestUtils.waitForCondition(
                () -> failures.stream().anyMatch(TimeoutException.class::isInstance),
                5_000,
                "Expected the deletion timeout to be reported");
        reconciler.close();
    }

    @Test
    void testDeltaIgnoredWhenNotActiveController() {
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> { });

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
        reconciler.onMetadataUpdate(delta, image, loaderManifest(image.provenance()));

        assertTrue(lifecycle.deletedTopics.isEmpty(), "No calls expected when not active controller");
        assertTrue(producerStateLifecycle.deletedTopics.isEmpty(), "No calls expected when not active controller");
        reconciler.close();
    }

    @Test
    void testCatalogDeleteAndProducerStateCleanupProceedIndependently() throws Exception {
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
        lifecycle.deleteBehavior = attempt -> catalogGate;
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        producerStateLifecycle.deleteBehavior = attempt -> attempt == 1
                ? CompletableFuture.failedFuture(new RuntimeException("producer state unavailable"))
                : CompletableFuture.completedFuture(null);
        List<String> faults = new CopyOnWriteArrayList<>();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> faults.add(message), 5, 10);
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        reconciler.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));

        TestUtils.waitForCondition(
                () -> producerStateLifecycle.deleteAttempts.get() == 2,
                5_000, "Expected producer-state cleanup to retry while catalog delete remains blocked");
        assertEquals(1, lifecycle.deleteAttempts.get());
        assertEquals(1, faults.size());
        assertTrue(faults.get(0).contains("delete producer-state snapshots"));
        catalogGate.complete(null);

        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(2), 2));
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 3));
        Thread.sleep(50);
        assertEquals(1, lifecycle.deleteAttempts.get(),
                "Completed deletion must be forgotten before the next leadership generation");
        assertEquals(2, producerStateLifecycle.deleteAttempts.get());
        reconciler.close();
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
        lifecycle.deleteBehavior = attempt -> {
            if (attempt == 1) {
                throw new RuntimeException("synchronous catalog failure");
            }
            return CompletableFuture.completedFuture(null);
        };
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        List<String> faults = new CopyOnWriteArrayList<>();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> faults.add(message), 5, 10);
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        reconciler.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));

        TestUtils.waitForCondition(
                () -> producerStateLifecycle.deletedTopics.size() == 1,
                5_000, "Expected producer-state cleanup after synchronous catalog failure");
        TestUtils.waitForCondition(
                () -> lifecycle.deleteAttempts.get() == 2,
                5_000, "Expected catalog delete to retry independently");
        assertEquals(1, faults.size());
        reconciler.close();
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
        RecordingProducerStateLifecycle producerStateLifecycle = new RecordingProducerStateLifecycle();
        producerStateLifecycle.deleteBehavior = attempt -> attempt == 1
                ? CompletableFuture.failedFuture(new RuntimeException("producer state unavailable"))
                : CompletableFuture.completedFuture(null);
        List<String> faults = new CopyOnWriteArrayList<>();
        DisklessTopicLifecycleReconciler reconciler = new DisklessTopicLifecycleReconciler(
                1, lifecycle, producerStateLifecycle, (message, cause) -> faults.add(message), 5, 10);
        reconciler.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        reconciler.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));

        TestUtils.waitForCondition(
                () -> producerStateLifecycle.deleteAttempts.get() == 2,
                5_000, "Expected producer-state cleanup retry");
        assertEquals(List.of(disklessTopic + ":" + topicId), lifecycle.deletedTopics);
        assertEquals(1, faults.size());
        assertTrue(faults.get(0).contains("delete producer-state snapshots"));
        reconciler.close();
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
