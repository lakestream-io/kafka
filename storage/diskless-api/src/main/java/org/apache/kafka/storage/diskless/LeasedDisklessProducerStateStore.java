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
import org.apache.kafka.common.Uuid;

import java.util.List;
import java.util.concurrent.CompletableFuture;

final class LeasedDisklessProducerStateStore implements DisklessProducerStateStore {
    private final DisklessProducerStateStore delegate;
    private final DisklessClassLoaderRegistry.Lease classLoaderLease;
    private boolean closed;

    LeasedDisklessProducerStateStore(
            DisklessProducerStateStore delegate,
            DisklessClassLoaderRegistry.Lease classLoaderLease) {
        this.delegate = delegate;
        this.classLoaderLease = classLoaderLease;
    }

    @Override
    public CompletableFuture<Void> reconcileTopic(String topicName, Uuid topicId, long sourceRevision) {
        return callWithClassLoader(() -> delegate.reconcileTopic(topicName, topicId, sourceRevision));
    }

    @Override
    public CompletableFuture<List<ManagedProducerStateTopic>> listManagedTopics() {
        return callWithClassLoader(delegate::listManagedTopics);
    }

    @Override
    public CompletableFuture<Void> deleteTopicSnapshots(Uuid topicId) {
        return callWithClassLoader(() -> delegate.deleteTopicSnapshots(topicId));
    }

    @Override
    public synchronized void close() throws Exception {
        if (closed) {
            return;
        }
        DisklessClassLoaderContext.call(classLoaderLease.classLoader(), () -> {
            delegate.close();
            return null;
        });
        classLoaderLease.close();
        closed = true;
    }

    private <T> T callWithClassLoader(DisklessClassLoaderContext.Action<T> action) {
        try {
            return DisklessClassLoaderContext.call(classLoaderLease.classLoader(), action);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Exception e) {
            throw new KafkaException("Failed to invoke diskless producer-state store", e);
        }
    }
}
