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

/** Loads the producer-state store from the isolated diskless storage runtime. */
public final class DisklessProducerStateStoreLoader {

    private static final String DEFAULT_URSA_STORAGE_DIR = "ursa-storage";
    private static final String URSA_PRODUCER_STATE_STORE_CLASS =
            "org.apache.kafka.storage.diskless.OxiaDisklessProducerStateStore";

    private DisklessProducerStateStoreLoader() {
    }

    public static DisklessProducerStateStore load(UrsaStorageConfig config) {
        return load(config, URSA_PRODUCER_STATE_STORE_CLASS);
    }

    static DisklessProducerStateStore load(UrsaStorageConfig config, String storeClassName) {
        try {
            ClassLoader parent = DisklessProducerStateStoreLoader.class.getClassLoader();
            URL[] urls = classPathUrls(config.getClassPath());
            DisklessClassLoaderRegistry.Lease classLoaderLease = DisklessClassLoaderRegistry.acquire(urls, parent);
            try {
                DisklessProducerStateStore store = DisklessClassLoaderContext.call(
                        classLoaderLease.classLoader(),
                        () -> {
                            Class<?> storeClass = Class.forName(
                                    storeClassName, true, classLoaderLease.classLoader());
                            Constructor<?> constructor = storeClass.getConstructor(UrsaStorageConfig.class);
                            return (DisklessProducerStateStore) constructor.newInstance(config);
                        });
                return new LeasedDisklessProducerStateStore(store, classLoaderLease);
            } catch (Throwable t) {
                DisklessClassLoaderRegistry.closeLeaseOnFailure(classLoaderLease, t);
                throw rethrow(t);
            }
        } catch (Exception e) {
            throw new KafkaException("Failed to load diskless producer-state store", e);
        }
    }

    static URL[] classPathUrls(String configuredClassPath) throws Exception {
        String classPath = KafkaPluginClassPaths.configuredOrDefault(
                configuredClassPath,
                DEFAULT_URSA_STORAGE_DIR,
                DisklessProducerStateStoreLoader.class);
        return KafkaPluginClassPaths.toUrls(classPath);
    }

    private static RuntimeException rethrow(Throwable t) throws Exception {
        if (t instanceof Exception exception) {
            throw exception;
        }
        if (t instanceof Error error) {
            throw error;
        }
        return new KafkaException("Failed to load diskless producer-state store", t);
    }
}
