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

/** Loads the diskless topic lifecycle implementation from the isolated storage runtime. */
public final class DisklessTopicLifecycleLoader {

    private static final String DEFAULT_URSA_STORAGE_DIR = "ursa-storage";
    private static final String URSA_TOPIC_LIFECYCLE_CLASS =
            "org.apache.kafka.storage.diskless.handlers.UrsaDisklessTopicLifecycle";

    private DisklessTopicLifecycleLoader() {
    }

    public static DisklessTopicLifecycle load(UrsaStorageConfig config) {
        return load(config, URSA_TOPIC_LIFECYCLE_CLASS);
    }

    static DisklessTopicLifecycle load(UrsaStorageConfig config, String lifecycleClassName) {
        try {
            ClassLoader parent = DisklessTopicLifecycleLoader.class.getClassLoader();
            URL[] urls = classPathUrls(config.getClassPath());
            DisklessClassLoaderRegistry.Lease classLoaderLease = DisklessClassLoaderRegistry.acquire(urls, parent);
            try {
                DisklessTopicLifecycle lifecycle = DisklessClassLoaderContext.call(
                        classLoaderLease.classLoader(),
                        () -> {
                            Class<?> lifecycleClass = Class.forName(
                                    lifecycleClassName, true, classLoaderLease.classLoader());
                            Constructor<?> constructor = lifecycleClass.getConstructor(UrsaStorageConfig.class);
                            return (DisklessTopicLifecycle) constructor.newInstance(config);
                        });
                return new LeasedDisklessTopicLifecycle(lifecycle, classLoaderLease);
            } catch (Throwable t) {
                DisklessClassLoaderRegistry.closeLeaseOnFailure(classLoaderLease, t);
                throw rethrow(t);
            }
        } catch (Exception e) {
            throw new KafkaException("Failed to load diskless topic lifecycle", e);
        }
    }

    static URL[] classPathUrls(String configuredClassPath) throws Exception {
        String classPath = KafkaPluginClassPaths.configuredOrDefault(
                configuredClassPath,
                DEFAULT_URSA_STORAGE_DIR,
                DisklessTopicLifecycleLoader.class);
        return KafkaPluginClassPaths.toUrls(classPath);
    }

    private static RuntimeException rethrow(Throwable t) throws Exception {
        if (t instanceof Exception exception) {
            throw exception;
        }
        if (t instanceof Error error) {
            throw error;
        }
        return new KafkaException("Failed to load diskless topic lifecycle", t);
    }
}
