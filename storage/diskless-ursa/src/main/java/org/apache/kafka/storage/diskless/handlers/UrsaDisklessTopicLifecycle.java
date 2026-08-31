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
package org.apache.kafka.storage.diskless.handlers;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.storage.diskless.DisklessTopicLifecycle;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import io.lakestream.api.ExternalStreamRegistry;
import io.lakestream.api.ExternalStreamRegistryLoader;
import io.lakestream.api.StreamIdentifier;

/** Ursa implementation of Kafka's logical diskless topic lifecycle. */
public final class UrsaDisklessTopicLifecycle implements DisklessTopicLifecycle {
    private final ExternalStreamRegistry registry;

    public UrsaDisklessTopicLifecycle(UrsaStorageConfig config) throws Exception {
        Objects.requireNonNull(config, "config must not be null");
        this.registry = ExternalStreamRegistryLoader.open(
                config.getCatalogOxiaServiceUrl(), new Properties());
    }

    UrsaDisklessTopicLifecycle(ExternalStreamRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
    }

    @Override
    public CompletableFuture<Void> registerTopic(
            String topicName,
            Uuid topicId,
            int partitions,
            Map<String, String> properties) {
        Map<String, String> propertySnapshot = KafkaLogNaming.streamProperties(topicName, properties);
        return registry.registerExternalStream(
                streamIdentifier(topicName, topicId), partitions, propertySnapshot);
    }

    @Override
    public CompletableFuture<Void> unregisterTopic(String topicName, Uuid topicId) {
        return registry.permanentlyDeleteExternalStream(streamIdentifier(topicName, topicId));
    }

    @Override
    public void close() throws Exception {
        registry.close();
    }

    static StreamIdentifier streamIdentifier(String topicName, Uuid topicId) {
        Objects.requireNonNull(topicName, "topicName must not be null");
        Objects.requireNonNull(topicId, "topicId must not be null");
        TopicIdPartition topicIdPartition = new TopicIdPartition(
                topicId, new TopicPartition(topicName, 0));
        return StreamIdentifier.of(
                KafkaLogNaming.NAMESPACE, KafkaLogNaming.streamName(topicIdPartition));
    }
}
