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
import org.apache.kafka.server.util.KafkaPluginClassPaths;
import org.apache.kafka.storage.diskless.handlers.UrsaStorageConfig;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.Objects;

/** Loads producer-state lifecycle operations from the isolated diskless storage runtime. */
public final class DisklessProducerStateLifecycleLoader {

    private static final String DEFAULT_DISKLESS_STORAGE_DIR = "ursa-storage";
    private DisklessProducerStateLifecycleLoader() {
    }

    public static DisklessProducerStateLifecycle load(UrsaStorageConfig config) {
        return DisklessStorageProviderLoader.load(
                config,
                "producer-state lifecycle",
                provider -> provider.createProducerStateLifecycle(config),
                LeasedDisklessProducerStateLifecycle::new);
    }

    /**
     * Returns a non-blocking facade that loads the isolated provider on its first operation.
     * Initialization failures are surfaced through that operation's future and retried by a later
     * operation, allowing the active controller reconciler to supervise provider availability.
     */
    public static DisklessProducerStateLifecycle loadLazily(UrsaStorageConfig config) {
        Objects.requireNonNull(config, "config must not be null");
        return new LazyDisklessProducerStateLifecycle(() -> load(config));
    }

    static DisklessProducerStateLifecycle load(UrsaStorageConfig config, String lifecycleClassName) {
        try {
            ClassLoader parent = DisklessProducerStateLifecycleLoader.class.getClassLoader();
            URL[] urls = classPathUrls(config.getClassPath());
            DisklessClassLoaderRegistry.Lease classLoaderLease = DisklessClassLoaderRegistry.acquire(urls, parent);
            try {
                DisklessProducerStateLifecycle lifecycle = DisklessClassLoaderContext.call(
                        classLoaderLease.classLoader(),
                        () -> {
                            Class<?> lifecycleClass = Class.forName(
                                    lifecycleClassName, true, classLoaderLease.classLoader());
                            Constructor<?> constructor = lifecycleClass.getConstructor(UrsaStorageConfig.class);
                            return (DisklessProducerStateLifecycle) constructor.newInstance(config);
                        });
                return new LeasedDisklessProducerStateLifecycle(lifecycle, classLoaderLease);
            } catch (Throwable t) {
                DisklessClassLoaderRegistry.closeLeaseOnFailure(classLoaderLease, t);
                throw rethrow(t);
            }
        } catch (Exception e) {
            throw new KafkaException("Failed to load diskless producer-state lifecycle", e);
        }
    }

    static URL[] classPathUrls(String configuredClassPath) throws Exception {
        String classPath = KafkaPluginClassPaths.configuredOrDefault(
                configuredClassPath,
                DEFAULT_DISKLESS_STORAGE_DIR,
                DisklessProducerStateLifecycleLoader.class);
        return KafkaPluginClassPaths.toUrls(classPath);
    }

    private static RuntimeException rethrow(Throwable t) throws Exception {
        if (t instanceof Exception exception) {
            throw exception;
        }
        if (t instanceof Error error) {
            throw error;
        }
        return new KafkaException("Failed to load diskless producer-state lifecycle", t);
    }
}
