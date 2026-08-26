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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

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
        RecordingLifecycle.registration = null;
        RecordingLifecycle.classLoader = null;
        RecordingLifecycle.invocationClassLoader = null;
        RecordingLifecycle.closeClassLoader = null;
        RecordingProducerStateStore.CLOSED.set(false);
        RecordingProducerStateStore.deletedTopicId = null;
        RecordingProducerStateStore.classLoader = null;
        FailingCloseLifecycle.classLoader = null;
    }

    @Test
    void testLoadsLifecycleFromIsolatedRuntimeAndReleasesLease() throws Exception {
        UrsaStorageConfig config = ursaConfig(tempDir);
        URL[] urls = DisklessTopicLifecycleLoader.classPathUrls(tempDir.toString());
        ClassLoader parent = DisklessTopicLifecycleLoader.class.getClassLoader();
        Uuid topicId = Uuid.randomUuid();
        try (DisklessTopicLifecycle lifecycle = DisklessTopicLifecycleLoader.load(
                config, RecordingLifecycle.class.getName())) {
            lifecycle.registerTopic("orders", topicId, 3, Map.of("owner", "kafka")).get();
        }

        assertEquals("orders:" + topicId + ":3", RecordingLifecycle.registration);
        assertTrue(RecordingLifecycle.CLOSED.get());
        ClassLoader observedLoader = RecordingLifecycle.classLoader;
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
    void testLoadsProducerStateStoreThroughSeparateSemanticSpi() throws Exception {
        UrsaStorageConfig config = ursaConfig(tempDir);
        Uuid topicId = Uuid.randomUuid();

        try (DisklessProducerStateStore store = DisklessProducerStateStoreLoader.load(
                config, RecordingProducerStateStore.class.getName())) {
            store.deleteTopicSnapshots(topicId).get();
        }

        assertEquals(topicId, RecordingProducerStateStore.deletedTopicId);
        assertTrue(RecordingProducerStateStore.CLOSED.get());
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
    void testLifecycleAndProducerStateStoreShareRuntimeUntilBothClose() throws Exception {
        UrsaStorageConfig config = ursaConfig(tempDir);
        URL[] urls = DisklessTopicLifecycleLoader.classPathUrls(tempDir.toString());
        ClassLoader parent = DisklessTopicLifecycleLoader.class.getClassLoader();

        DisklessTopicLifecycle lifecycle = DisklessTopicLifecycleLoader.load(
                config, RecordingLifecycle.class.getName());
        DisklessProducerStateStore store = DisklessProducerStateStoreLoader.load(
                config, RecordingProducerStateStore.class.getName());
        ClassLoader sharedLoader = RecordingLifecycle.classLoader;
        assertSame(sharedLoader, RecordingProducerStateStore.classLoader);

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
    void testLifecycleCloseFailureStillReleasesLease() throws Exception {
        UrsaStorageConfig config = ursaConfig(tempDir);
        URL[] urls = DisklessTopicLifecycleLoader.classPathUrls(tempDir.toString());
        ClassLoader parent = DisklessTopicLifecycleLoader.class.getClassLoader();
        DisklessTopicLifecycle lifecycle = DisklessTopicLifecycleLoader.load(
                config, FailingCloseLifecycle.class.getName());
        ClassLoader failedLoader = FailingCloseLifecycle.classLoader;

        assertThrows(IOException.class, lifecycle::close);

        DisklessClassLoaderRegistry.Lease nextLease = DisklessClassLoaderRegistry.acquire(urls, parent);
        try {
            assertNotSame(failedLoader, nextLease.classLoader());
        } finally {
            nextLease.close();
        }
    }

    private static UrsaStorageConfig ursaConfig(Path classPath) throws Exception {
        return UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_STORAGE_CLASS_PATH_CONFIG, classPath.toString()));
    }

    public static final class RecordingLifecycle implements DisklessTopicLifecycle {
        static final AtomicBoolean CLOSED = new AtomicBoolean(false);
        static volatile String registration;
        static volatile ClassLoader classLoader;
        static volatile ClassLoader invocationClassLoader;
        static volatile ClassLoader closeClassLoader;

        public RecordingLifecycle(UrsaStorageConfig config) {
            classLoader = Thread.currentThread().getContextClassLoader();
        }

        @Override
        public CompletableFuture<Void> registerTopic(
                String topicName,
                Uuid topicId,
                int partitions,
                Map<String, String> properties) {
            registration = topicName + ":" + topicId + ":" + partitions;
            invocationClassLoader = Thread.currentThread().getContextClassLoader();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> unregisterTopic(String topicName, Uuid topicId) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
            closeClassLoader = Thread.currentThread().getContextClassLoader();
            CLOSED.set(true);
        }
    }

    public static final class RecordingProducerStateStore implements DisklessProducerStateStore {
        static final AtomicBoolean CLOSED = new AtomicBoolean(false);
        static volatile Uuid deletedTopicId;
        static volatile ClassLoader classLoader;

        public RecordingProducerStateStore(UrsaStorageConfig config) {
            classLoader = Thread.currentThread().getContextClassLoader();
        }

        @Override
        public CompletableFuture<Void> deleteTopicSnapshots(Uuid topicId) {
            deletedTopicId = topicId;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
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
        static volatile ClassLoader classLoader;

        public FailingCloseLifecycle(UrsaStorageConfig config) {
            classLoader = Thread.currentThread().getContextClassLoader();
        }

        @Override
        public CompletableFuture<Void> registerTopic(
                String topicName,
                Uuid topicId,
                int partitions,
                Map<String, String> properties) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> unregisterTopic(String topicName, Uuid topicId) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() throws IOException {
            throw new IOException("close failed");
        }
    }
}
