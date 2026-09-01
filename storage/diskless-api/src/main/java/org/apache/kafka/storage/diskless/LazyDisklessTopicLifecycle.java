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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Lazy controller-side facade for the isolated topic lifecycle provider. */
final class LazyDisklessTopicLifecycle implements DisklessTopicLifecycle {

    private final LazyDisklessResource<DisklessTopicLifecycle> resource;

    LazyDisklessTopicLifecycle(Supplier<DisklessTopicLifecycle> loader) {
        this.resource = new LazyDisklessResource<>("diskless topic lifecycle", loader);
    }

    @Override
    public CompletableFuture<List<ManagedTopic>> listManagedTopics() {
        return resource.call(DisklessTopicLifecycle::listManagedTopics);
    }

    @Override
    public CompletableFuture<Void> reconcileTopic(
            String topicName,
            Uuid topicId,
            int partitions,
            Map<String, String> properties,
            long sourceRevision) {
        return resource.call(lifecycle -> lifecycle.reconcileTopic(
                topicName, topicId, partitions, properties, sourceRevision));
    }

    @Override
    public CompletableFuture<Void> deleteTopic(String topicName, Uuid topicId) {
        return resource.call(lifecycle -> lifecycle.deleteTopic(topicName, topicId));
    }

    @Override
    public void close() throws Exception {
        resource.close();
    }
}
