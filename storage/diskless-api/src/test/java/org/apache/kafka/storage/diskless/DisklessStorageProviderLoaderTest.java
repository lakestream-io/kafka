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
package org.apache.kafka.storage.diskless;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.handlers.UrsaStorageConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DisklessStorageProviderLoaderTest {
    private static final String SERVICE_NAME = DisklessStorageProvider.class.getName();

    @TempDir
    Path tempDir;

    @BeforeEach
    void resetProviderState() {
        RecordingProvider.creationClassLoader = null;
        RecordingProvider.CREATED_COMPONENTS.set(0);
        RecordingProvider.CLOSED_COMPONENTS.set(0);
    }

    @Test
    void testAllComponentsAreCreatedThroughTheSingleDiscoveredProvider() throws Exception {
        writeProviders(RecordingProvider.class);
        UrsaStorageConfig config = ursaConfig();

        DisklessStorageEngine engine = DisklessStorageEngineLoader.load(
                null, 1, config, null, Map.of(), ignored -> Map.of());
        DisklessTopicLifecycle topicLifecycle = DisklessTopicLifecycleLoader.load(config);
        DisklessProducerStateLifecycle producerStateLifecycle =
                DisklessProducerStateLifecycleLoader.load(config);

        assertEquals(3, RecordingProvider.CREATED_COMPONENTS.get());
        ClassLoader providerClassLoader = RecordingProvider.creationClassLoader;
        assertNotSame(Thread.currentThread().getContextClassLoader(), providerClassLoader);

        engine.write(Map.of(), "").get();
        assertTrue(topicLifecycle.listManagedTopics().get().isEmpty());
        assertTrue(producerStateLifecycle.listManagedTopics().get().isEmpty());

        engine.close();
        topicLifecycle.close();
        producerStateLifecycle.close();
        assertEquals(3, RecordingProvider.CLOSED_COMPONENTS.get());

        assertLeaseWasReleased(providerClassLoader);
    }

    @Test
    void testMissingProviderFailsAndReleasesClassLoaderLease() throws Exception {
        URL[] urls = DisklessStorageEngineLoader.classPathUrls(tempDir.toString());
        ClassLoader parent = DisklessStorageProviderLoader.class.getClassLoader();
        DisklessClassLoaderRegistry.Lease retainedLease = DisklessClassLoaderRegistry.acquire(urls, parent);
        ClassLoader failedLoader = retainedLease.classLoader();

        KafkaException failure = assertThrows(
                KafkaException.class,
                () -> DisklessTopicLifecycleLoader.load(ursaConfig()));
        assertTrue(failure.getMessage().contains("No DisklessStorageProvider"));
        retainedLease.close();

        assertLeaseWasReleased(failedLoader);
    }

    @Test
    void testMultipleProvidersFailAndReleaseClassLoaderLease() throws Exception {
        writeProviders(RecordingProvider.class, SecondProvider.class);
        URL[] urls = DisklessStorageEngineLoader.classPathUrls(tempDir.toString());
        ClassLoader parent = DisklessStorageProviderLoader.class.getClassLoader();
        DisklessClassLoaderRegistry.Lease retainedLease = DisklessClassLoaderRegistry.acquire(urls, parent);
        ClassLoader failedLoader = retainedLease.classLoader();

        KafkaException failure = assertThrows(
                KafkaException.class,
                () -> DisklessProducerStateLifecycleLoader.load(ursaConfig()));
        assertTrue(failure.getMessage().contains("Multiple DisklessStorageProvider"));
        assertTrue(failure.getMessage().contains(RecordingProvider.class.getName()));
        assertTrue(failure.getMessage().contains(SecondProvider.class.getName()));
        retainedLease.close();

        assertLeaseWasReleased(failedLoader);
    }

    private void writeProviders(Class<?>... providers) throws IOException {
        Path serviceFile = tempDir.resolve("META-INF/services").resolve(SERVICE_NAME);
        Files.createDirectories(serviceFile.getParent());
        Files.writeString(
                serviceFile,
                String.join("\n", Arrays.stream(providers).map(Class::getName).toList()) + "\n");
    }

    private UrsaStorageConfig ursaConfig() throws Exception {
        return UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_STORAGE_CLASS_PATH_CONFIG, tempDir.toString()));
    }

    private void assertLeaseWasReleased(ClassLoader previousClassLoader) throws Exception {
        URL[] urls = DisklessStorageEngineLoader.classPathUrls(tempDir.toString());
        ClassLoader parent = DisklessStorageProviderLoader.class.getClassLoader();
        DisklessClassLoaderRegistry.Lease nextLease = DisklessClassLoaderRegistry.acquire(urls, parent);
        try {
            assertNotSame(previousClassLoader, nextLease.classLoader());
        } finally {
            nextLease.close();
        }
    }

    public static final class RecordingProvider implements DisklessStorageProvider {
        static final AtomicInteger CREATED_COMPONENTS = new AtomicInteger();
        static final AtomicInteger CLOSED_COMPONENTS = new AtomicInteger();
        static volatile ClassLoader creationClassLoader;

        @Override
        public DisklessStorageEngine createStorageEngine(StorageEngineContext context) {
            recordCreation();
            return new NoOpEngine();
        }

        @Override
        public DisklessTopicLifecycle createTopicLifecycle(UrsaStorageConfig config) {
            recordCreation();
            return new NoOpTopicLifecycle();
        }

        @Override
        public DisklessProducerStateLifecycle createProducerStateLifecycle(UrsaStorageConfig config) {
            recordCreation();
            return new NoOpProducerStateLifecycle();
        }

        private static void recordCreation() {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            if (creationClassLoader == null) {
                creationClassLoader = contextClassLoader;
            } else {
                assertSame(creationClassLoader, contextClassLoader);
            }
            CREATED_COMPONENTS.incrementAndGet();
        }
    }

    public static final class SecondProvider implements DisklessStorageProvider {
        @Override
        public DisklessStorageEngine createStorageEngine(StorageEngineContext context) {
            return new NoOpEngine();
        }

        @Override
        public DisklessTopicLifecycle createTopicLifecycle(UrsaStorageConfig config) {
            return new NoOpTopicLifecycle();
        }

        @Override
        public DisklessProducerStateLifecycle createProducerStateLifecycle(UrsaStorageConfig config) {
            return new NoOpProducerStateLifecycle();
        }
    }

    private static final class NoOpEngine implements DisklessStorageEngine {
        @Override
        public CompletableFuture<Map<TopicIdPartition, ProduceResponse.PartitionResponse>> write(
                Map<TopicIdPartition, MemoryRecords> entriesPerPartition,
                String zone) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch(
                FetchParams params,
                Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletableFuture<Map<TopicIdPartition, ListOffsetsPartitionResponse>> listOffsets(
                Map<TopicIdPartition, ListOffsetsPartitionRequest> requests) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public boolean cleanupPartition(TopicIdPartition tp, boolean deletePartition) {
            return true;
        }

        @Override
        public void applyTopicConfig(String topicName, Uuid topicId, Map<String, String> config) {
        }

        @Override
        public void fenceDeletedTopic(String topicName, Uuid topicId) {
        }

        @Override
        public Set<TopicIdPartition> snapshotTrackedPartitions() {
            return Set.of();
        }

        @Override
        public boolean cleanupNonOwnedProducerStates(
                TopicIdPartition tp,
                Set<String> retainedZones,
                boolean deleteSnapshot) {
            return true;
        }

        @Override
        public void close() {
            RecordingProvider.CLOSED_COMPONENTS.incrementAndGet();
        }
    }

    private static final class NoOpTopicLifecycle implements DisklessTopicLifecycle {
        @Override
        public CompletableFuture<List<ManagedTopic>> listManagedTopics() {
            return CompletableFuture.completedFuture(List.of());
        }

        @Override
        public CompletableFuture<Void> reconcileTopic(
                String topicName,
                Uuid topicId,
                int partitions,
                Map<String, String> properties,
                long sourceRevision) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> deleteTopic(String topicName, Uuid topicId) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
            RecordingProvider.CLOSED_COMPONENTS.incrementAndGet();
        }
    }

    private static final class NoOpProducerStateLifecycle implements DisklessProducerStateLifecycle {
        @Override
        public CompletableFuture<Void> reconcileTopic(String topicName, Uuid topicId, long sourceRevision) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<List<ManagedProducerStateTopic>> listManagedTopics() {
            return CompletableFuture.completedFuture(List.of());
        }

        @Override
        public CompletableFuture<Void> deleteTopicSnapshots(Uuid topicId) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
            RecordingProvider.CLOSED_COMPONENTS.incrementAndGet();
        }
    }
}
