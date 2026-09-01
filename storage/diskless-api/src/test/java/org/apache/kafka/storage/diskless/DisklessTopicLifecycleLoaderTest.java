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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.storage.diskless.handlers.UrsaStorageConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DisklessTopicLifecycleLoaderTest {

    @TempDir
    Path tempDir;

    @BeforeEach
    void reset() {
        RecordingLifecycle.CLOSED.set(false);
        RecordingLifecycle.reconciliation = null;
        RecordingLifecycle.classLoader = null;
        RecordingLifecycle.inventoryClassLoader = null;
        RecordingLifecycle.invocationClassLoader = null;
        RecordingLifecycle.closeClassLoader = null;
        RecordingProducerStateLifecycle.CLOSED.set(false);
        RecordingProducerStateLifecycle.deletedTopicId = null;
        RecordingProducerStateLifecycle.reconciliation = null;
        RecordingProducerStateLifecycle.classLoader = null;
        RecordingProducerStateLifecycle.inventoryClassLoader = null;
        RecordingProducerStateLifecycle.invocationClassLoader = null;
        RecordingProducerStateLifecycle.closeClassLoader = null;
        FailingCloseLifecycle.classLoader = null;
        FailingCloseLifecycle.FAIL_ON_CLOSE.set(true);
    }

    @Test
    void testLoadsLifecycleFromIsolatedRuntimeAndReleasesLease() throws Exception {
        UrsaStorageConfig config = ursaConfig(tempDir);
        URL[] urls = DisklessTopicLifecycleLoader.classPathUrls(tempDir.toString());
        ClassLoader parent = DisklessTopicLifecycleLoader.class.getClassLoader();
        Uuid topicId = Uuid.randomUuid();
        try (DisklessTopicLifecycle lifecycle = DisklessTopicLifecycleLoader.load(
                config, RecordingLifecycle.class.getName())) {
            assertTrue(lifecycle.listManagedTopics().get().isEmpty());
            lifecycle.reconcileTopic("orders", topicId, 3, Map.of("owner", "kafka"), 11).get();
        }

        assertEquals("orders:" + topicId + ":3", RecordingLifecycle.reconciliation);
        assertTrue(RecordingLifecycle.CLOSED.get());
        ClassLoader observedLoader = RecordingLifecycle.classLoader;
        assertSame(observedLoader, RecordingLifecycle.inventoryClassLoader);
        assertSame(observedLoader, RecordingLifecycle.invocationClassLoader);
        assertSame(observedLoader, RecordingLifecycle.closeClassLoader);
        DisklessClassLoaderRegistry.Lease nextLease = DisklessClassLoaderRegistry.acquire(urls, parent);
        try {
            assertNotSame(observedLoader, nextLease.classLoader());
        } finally {
            nextLease.close();
        }
    }

    @Test
    void testLoadsProducerStateLifecycleThroughSeparateSemanticSpi() throws Exception {
        UrsaStorageConfig config = ursaConfig(tempDir);
        Uuid topicId = Uuid.randomUuid();

        try (DisklessProducerStateLifecycle store = DisklessProducerStateLifecycleLoader.load(
                config, RecordingProducerStateLifecycle.class.getName())) {
            store.reconcileTopic("orders", topicId, 17).get();
            assertEquals(
                    List.of(new DisklessProducerStateLifecycle.ManagedProducerStateTopic(
                            "orders", topicId, 17)),
                    store.listManagedTopics().get());
            store.deleteTopicSnapshots(topicId).get();
        }

        assertEquals("orders:" + topicId + ":17", RecordingProducerStateLifecycle.reconciliation);
        assertEquals(topicId, RecordingProducerStateLifecycle.deletedTopicId);
        assertTrue(RecordingProducerStateLifecycle.CLOSED.get());
        assertSame(RecordingProducerStateLifecycle.classLoader, RecordingProducerStateLifecycle.inventoryClassLoader);
        assertSame(RecordingProducerStateLifecycle.classLoader, RecordingProducerStateLifecycle.invocationClassLoader);
        assertSame(RecordingProducerStateLifecycle.classLoader, RecordingProducerStateLifecycle.closeClassLoader);
    }

    @Test
    void testLifecycleLoaderReleasesLeaseWhenClassInitializationFails() throws Exception {
        UrsaStorageConfig config = ursaConfig(tempDir);
        URL[] urls = DisklessTopicLifecycleLoader.classPathUrls(tempDir.toString());
        ClassLoader parent = DisklessTopicLifecycleLoader.class.getClassLoader();
        DisklessClassLoaderRegistry.Lease retainedLease = DisklessClassLoaderRegistry.acquire(urls, parent);
        ClassLoader failedLoader = retainedLease.classLoader();

        assertThrows(ExceptionInInitializerError.class, () -> DisklessTopicLifecycleLoader.load(
                config, FailingLifecycle.class.getName()));
        retainedLease.close();

        DisklessClassLoaderRegistry.Lease nextLease = DisklessClassLoaderRegistry.acquire(urls, parent);
        try {
            assertNotSame(failedLoader, nextLease.classLoader());
        } finally {
            nextLease.close();
        }
    }

    @Test
    void testLifecycleAndProducerStateLifecycleShareRuntimeUntilBothClose() throws Exception {
        UrsaStorageConfig config = ursaConfig(tempDir);
        URL[] urls = DisklessTopicLifecycleLoader.classPathUrls(tempDir.toString());
        ClassLoader parent = DisklessTopicLifecycleLoader.class.getClassLoader();

        DisklessTopicLifecycle lifecycle = DisklessTopicLifecycleLoader.load(
                config, RecordingLifecycle.class.getName());
        DisklessProducerStateLifecycle store = DisklessProducerStateLifecycleLoader.load(
                config, RecordingProducerStateLifecycle.class.getName());
        ClassLoader sharedLoader = RecordingLifecycle.classLoader;
        assertSame(sharedLoader, RecordingProducerStateLifecycle.classLoader);

        lifecycle.close();
        store.deleteTopicSnapshots(Uuid.randomUuid()).get();
        store.close();

        DisklessClassLoaderRegistry.Lease nextLease = DisklessClassLoaderRegistry.acquire(urls, parent);
        try {
            assertNotSame(sharedLoader, nextLease.classLoader());
        } finally {
            nextLease.close();
        }
    }

    @Test
    void testLifecycleCloseFailureRetainsLeaseUntilRetrySucceeds() throws Exception {
        UrsaStorageConfig config = ursaConfig(tempDir);
        URL[] urls = DisklessTopicLifecycleLoader.classPathUrls(tempDir.toString());
        ClassLoader parent = DisklessTopicLifecycleLoader.class.getClassLoader();
        DisklessTopicLifecycle lifecycle = DisklessTopicLifecycleLoader.load(
                config, FailingCloseLifecycle.class.getName());
        ClassLoader failedLoader = FailingCloseLifecycle.classLoader;

        assertThrows(IOException.class, lifecycle::close);

        DisklessClassLoaderRegistry.Lease nextLease = DisklessClassLoaderRegistry.acquire(urls, parent);
        try {
            assertSame(failedLoader, nextLease.classLoader());
        } finally {
            nextLease.close();
        }

        FailingCloseLifecycle.FAIL_ON_CLOSE.set(false);
        lifecycle.close();

        DisklessClassLoaderRegistry.Lease afterSuccessfulClose =
                DisklessClassLoaderRegistry.acquire(urls, parent);
        try {
            assertNotSame(failedLoader, afterSuccessfulClose.classLoader());
        } finally {
            afterSuccessfulClose.close();
        }
    }

    @Test
    void testLazyLifecycleDoesNotLoadUntilUsedAndRetriesInitialization() throws Exception {
        AtomicInteger loadAttempts = new AtomicInteger();
        DisklessTopicLifecycle lifecycle = new LazyDisklessTopicLifecycle(() -> {
            if (loadAttempts.incrementAndGet() == 1) {
                throw new IllegalStateException("catalog unavailable");
            }
            return new RecordingLifecycle(null);
        });

        assertEquals(0, loadAttempts.get());
        ExecutionException firstFailure = assertThrows(
                ExecutionException.class,
                () -> lifecycle.listManagedTopics().get());
        assertTrue(firstFailure.getCause().getMessage().contains("catalog unavailable"));
        assertTrue(lifecycle.listManagedTopics().get().isEmpty());
        assertEquals(2, loadAttempts.get());
        lifecycle.close();
        assertTrue(RecordingLifecycle.CLOSED.get());
    }

    @Test
    void testLazyProducerStateLifecycleDoesNotLoadUntilUsedAndRetriesInitialization() throws Exception {
        AtomicInteger loadAttempts = new AtomicInteger();
        DisklessProducerStateLifecycle store = new LazyDisklessProducerStateLifecycle(() -> {
            if (loadAttempts.incrementAndGet() == 1) {
                throw new IllegalStateException("oxia unavailable");
            }
            return new RecordingProducerStateLifecycle(null);
        });

        assertEquals(0, loadAttempts.get());
        ExecutionException firstFailure = assertThrows(
                ExecutionException.class,
                () -> store.listManagedTopics().get());
        assertTrue(firstFailure.getCause().getMessage().contains("oxia unavailable"));
        assertTrue(store.listManagedTopics().get().isEmpty());
        assertEquals(2, loadAttempts.get());
        store.close();
        assertTrue(RecordingProducerStateLifecycle.CLOSED.get());
    }

    private static UrsaStorageConfig ursaConfig(Path classPath) throws Exception {
        return UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_STORAGE_CLASS_PATH_CONFIG, classPath.toString()));
    }

    public static final class RecordingLifecycle implements DisklessTopicLifecycle {
        static final AtomicBoolean CLOSED = new AtomicBoolean(false);
        static volatile String reconciliation;
        static volatile ClassLoader classLoader;
        static volatile ClassLoader inventoryClassLoader;
        static volatile ClassLoader invocationClassLoader;
        static volatile ClassLoader closeClassLoader;

        public RecordingLifecycle(UrsaStorageConfig config) {
            classLoader = Thread.currentThread().getContextClassLoader();
        }

        @Override
        public CompletableFuture<List<ManagedTopic>> listManagedTopics() {
            inventoryClassLoader = Thread.currentThread().getContextClassLoader();
            return CompletableFuture.completedFuture(List.of());
        }

        @Override
        public CompletableFuture<Void> reconcileTopic(
                String topicName,
                Uuid topicId,
                int partitions,
                Map<String, String> properties,
                long sourceRevision) {
            reconciliation = topicName + ":" + topicId + ":" + partitions;
            invocationClassLoader = Thread.currentThread().getContextClassLoader();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> deleteTopic(String topicName, Uuid topicId) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
            closeClassLoader = Thread.currentThread().getContextClassLoader();
            CLOSED.set(true);
        }
    }

    public static final class RecordingProducerStateLifecycle implements DisklessProducerStateLifecycle {
        static final AtomicBoolean CLOSED = new AtomicBoolean(false);
        static volatile Uuid deletedTopicId;
        static volatile String reconciliation;
        static volatile ClassLoader classLoader;
        static volatile ClassLoader inventoryClassLoader;
        static volatile ClassLoader invocationClassLoader;
        static volatile ClassLoader closeClassLoader;

        public RecordingProducerStateLifecycle(UrsaStorageConfig config) {
            classLoader = Thread.currentThread().getContextClassLoader();
        }

        @Override
        public CompletableFuture<Void> reconcileTopic(
                String topicName,
                Uuid topicId,
                long sourceRevision
        ) {
            reconciliation = topicName + ":" + topicId + ":" + sourceRevision;
            invocationClassLoader = Thread.currentThread().getContextClassLoader();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<List<ManagedProducerStateTopic>> listManagedTopics() {
            inventoryClassLoader = Thread.currentThread().getContextClassLoader();
            if (reconciliation == null) {
                return CompletableFuture.completedFuture(List.of());
            }
            String[] fields = reconciliation.split(":", 3);
            return CompletableFuture.completedFuture(List.of(new ManagedProducerStateTopic(
                    fields[0], Uuid.fromString(fields[1]), Long.parseLong(fields[2]))));
        }

        @Override
        public CompletableFuture<Void> deleteTopicSnapshots(Uuid topicId) {
            deletedTopicId = topicId;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
            closeClassLoader = Thread.currentThread().getContextClassLoader();
            CLOSED.set(true);
        }
    }

    public static final class FailingLifecycle {
        static {
            fail();
        }

        private static void fail() {
            throw new ExceptionInInitializerError("boom");
        }
    }

    public static final class FailingCloseLifecycle implements DisklessTopicLifecycle {
        static final AtomicBoolean FAIL_ON_CLOSE = new AtomicBoolean(true);
        static volatile ClassLoader classLoader;

        public FailingCloseLifecycle(UrsaStorageConfig config) {
            classLoader = Thread.currentThread().getContextClassLoader();
        }

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
        public void close() throws IOException {
            if (FAIL_ON_CLOSE.get()) {
                throw new IOException("close failed");
            }
        }
    }
}
