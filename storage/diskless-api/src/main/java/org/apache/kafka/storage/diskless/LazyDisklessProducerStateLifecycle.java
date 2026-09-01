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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Lazy controller-side facade for isolated producer-state lifecycle operations. */
final class LazyDisklessProducerStateLifecycle implements DisklessProducerStateLifecycle {

    private final LazyDisklessResource<DisklessProducerStateLifecycle> resource;

    LazyDisklessProducerStateLifecycle(Supplier<DisklessProducerStateLifecycle> loader) {
        this.resource = new LazyDisklessResource<>("diskless producer-state lifecycle", loader);
    }

    @Override
    public CompletableFuture<Void> reconcileTopic(
            String topicName,
            Uuid topicId,
            long sourceRevision) {
        return resource.call(lifecycle -> lifecycle.reconcileTopic(topicName, topicId, sourceRevision));
    }

    @Override
    public CompletableFuture<List<ManagedProducerStateTopic>> listManagedTopics() {
        return resource.call(DisklessProducerStateLifecycle::listManagedTopics);
    }

    @Override
    public CompletableFuture<Void> deleteTopicSnapshots(Uuid topicId) {
        return resource.call(lifecycle -> lifecycle.deleteTopicSnapshots(topicId));
    }

    @Override
    public void close() throws Exception {
        resource.close();
    }
}
