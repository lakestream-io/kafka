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
import org.apache.kafka.storage.diskless.handlers.UrsaStorageConfig;

import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;

final class DisklessStorageProviderLoader {

    private DisklessStorageProviderLoader() {
    }

    static <T> T load(
            UrsaStorageConfig config,
            String componentName,
            ProviderFactory<T> factory,
            LeasedComponentFactory<T> leasedComponentFactory) {
        DisklessClassLoaderRegistry.Lease classLoaderLease = null;
        try {
            ClassLoader parent = DisklessStorageProviderLoader.class.getClassLoader();
            URL[] urls = DisklessStorageEngineLoader.classPathUrls(config.getClassPath());
            classLoaderLease = DisklessClassLoaderRegistry.acquire(urls, parent);
            DisklessClassLoaderRegistry.Lease acquiredLease = classLoaderLease;
            T delegate = DisklessClassLoaderContext.call(acquiredLease.classLoader(), () -> {
                DisklessStorageProvider provider = discoverProvider(acquiredLease.classLoader());
                return Objects.requireNonNull(
                        factory.create(provider),
                        "DisklessStorageProvider returned a null " + componentName);
            });
            return leasedComponentFactory.create(delegate, acquiredLease);
        } catch (Throwable t) {
            if (classLoaderLease != null) {
                DisklessClassLoaderRegistry.closeLeaseOnFailure(classLoaderLease, t);
            }
            if (t instanceof Error error) {
                throw error;
            }
            if (t instanceof KafkaException kafkaException) {
                throw kafkaException;
            }
            throw new KafkaException("Failed to load diskless " + componentName, t);
        }
    }

    private static DisklessStorageProvider discoverProvider(ClassLoader classLoader) {
        List<ServiceLoader.Provider<DisklessStorageProvider>> providers = ServiceLoader
                .load(DisklessStorageProvider.class, classLoader)
                .stream()
                .toList();
        if (providers.isEmpty()) {
            throw new KafkaException(
                    "No DisklessStorageProvider was found on the configured diskless storage classpath");
        }
        if (providers.size() > 1) {
            String providerNames = providers.stream()
                    .map(provider -> provider.type().getName())
                    .sorted()
                    .reduce((left, right) -> left + ", " + right)
                    .orElse("");
            throw new KafkaException(
                    "Multiple DisklessStorageProvider implementations were found: " + providerNames);
        }
        return providers.get(0).get();
    }

    @FunctionalInterface
    interface ProviderFactory<T> {
        T create(DisklessStorageProvider provider) throws Exception;
    }

    @FunctionalInterface
    interface LeasedComponentFactory<T> {
        T create(T delegate, DisklessClassLoaderRegistry.Lease classLoaderLease);
    }
}
