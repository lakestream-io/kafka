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

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Storage-neutral lifecycle operations for Kafka topics whose data is managed by a diskless
 * storage implementation.
 *
 * <p>The implementation owns its catalog schema and metadata-store layout. Kafka supplies only
 * topic identity, partition count, and topic properties.
 */
public interface DisklessTopicLifecycle extends AutoCloseable {

    /** Register a logical diskless topic without creating its physical partition logs. */
    CompletableFuture<Void> registerTopic(
            String topicName,
            Uuid topicId,
            int partitions,
            Map<String, String> properties);

    /**
     * Permanently unregister the logical topic after its Kafka metadata has been deleted.
     *
     * <p>Kafka topic IDs identify immutable topic incarnations. Implementations must durably fence
     * this ID so that an already in-flight registration cannot recreate it after this future
     * completes. A topic recreated with the same name receives a different ID.
     */
    CompletableFuture<Void> unregisterTopic(String topicName, Uuid topicId);
}
