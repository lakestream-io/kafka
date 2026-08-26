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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DisklessTopicLifecyclePublisherTest {

    private static final class RecordingLifecycle implements DisklessTopicLifecycle {
        final List<String> registeredTopics = new CopyOnWriteArrayList<>();
        final List<String> unregisteredTopics = new CopyOnWriteArrayList<>();
        CompletableFuture<Void> unregisterResult = CompletableFuture.completedFuture(null);
        RuntimeException unregisterThrow;

        @Override
        public CompletableFuture<Void> registerTopic(
                String topicName,
                Uuid topicId,
                int partitions,
                Map<String, String> properties) {
            registeredTopics.add(topicName + ":" + topicId + ":" + partitions);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> unregisterTopic(String topicName, Uuid topicId) {
            unregisteredTopics.add(topicName + ":" + topicId);
            if (unregisterThrow != null) {
                throw unregisterThrow;
            }
            return unregisterResult;
        }

        @Override
        public void close() {
        }
    }

    private static final class RecordingProducerStateStore implements DisklessProducerStateStore {
        final List<String> deletedTopics = new CopyOnWriteArrayList<>();
        CompletableFuture<Void> deleteResult = CompletableFuture.completedFuture(null);

        @Override
        public CompletableFuture<Void> deleteTopicSnapshots(Uuid topicId) {
            deletedTopics.add(topicId.toString());
            return deleteResult;
        }

        @Override
        public void close() {
        }
    }

    @Test
    void testDeltaIgnoredBeforeBecomingLeader() throws Exception {
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
        assertTrue(lifecycle.unregisteredTopics.isEmpty(), "No cleanup expected before becoming leader");
        assertTrue(producerStateStore.deletedTopics.isEmpty(), "No calls expected before becoming leader");
        publisher.close();
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
    void testProducerStateCleanupContinuesAfterCatalogUnregisterFailure() throws Exception {
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
        lifecycle.unregisterResult = CompletableFuture.failedFuture(new RuntimeException("catalog unavailable"));
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        List<String> faults = new CopyOnWriteArrayList<>();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> faults.add(message));
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));

        TestUtils.waitForCondition(
                () -> producerStateStore.deletedTopics.size() == 1,
                5_000, "Expected producer-state cleanup after catalog failure");
        assertEquals(1, faults.size());
        assertTrue(faults.get(0).contains("unregister diskless topic"));
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
        lifecycle.unregisterThrow = new RuntimeException("synchronous catalog failure");
        RecordingProducerStateStore producerStateStore = new RecordingProducerStateStore();
        List<String> faults = new CopyOnWriteArrayList<>();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> faults.add(message));
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));

        TestUtils.waitForCondition(
                () -> producerStateStore.deletedTopics.size() == 1,
                5_000, "Expected producer-state cleanup after synchronous catalog failure");
        assertEquals(1, faults.size());
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
        producerStateStore.deleteResult = CompletableFuture.failedFuture(
                new RuntimeException("producer state unavailable"));
        List<String> faults = new CopyOnWriteArrayList<>();
        DisklessTopicLifecyclePublisher publisher = new DisklessTopicLifecyclePublisher(
                1, lifecycle, producerStateStore, (message, cause) -> faults.add(message));
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));

        TestUtils.waitForCondition(
                () -> faults.size() == 1,
                5_000, "Expected producer-state failure to reach fault handler");
        assertEquals(List.of(disklessTopic + ":" + topicId), lifecycle.unregisteredTopics);
        assertTrue(faults.get(0).contains("delete producer-state snapshots"));
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
        return new ConfigRecord()
                .setResourceType(ConfigResource.Type.TOPIC.id())
                .setResourceName(topicName)
                .setName(TopicConfig.URSA_STORAGE_ENABLE_CONFIG)
                .setValue("true");
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
