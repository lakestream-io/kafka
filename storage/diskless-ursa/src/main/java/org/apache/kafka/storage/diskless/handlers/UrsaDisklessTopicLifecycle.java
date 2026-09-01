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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import io.lakestream.api.Partitioning;
import io.lakestream.api.PartitioningStrategy;
import io.lakestream.api.SchemaConfig;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamCatalogLoader;
import io.lakestream.api.StreamConfig;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.StreamMetadata;
import io.lakestream.api.exception.AlreadyExistsException;
import io.lakestream.api.exception.NoSuchStreamException;

/** Ursa implementation of Kafka's logical diskless topic lifecycle. */
public final class UrsaDisklessTopicLifecycle implements DisklessTopicLifecycle {
    private final StreamCatalog catalog;

    public UrsaDisklessTopicLifecycle(UrsaStorageConfig config) throws Exception {
        Objects.requireNonNull(config, "config must not be null");
        Properties properties = LakestreamStorageHolder.buildStorageProperties(config);
        this.catalog = StreamCatalogLoader.open(config.getCatalogOxiaServiceUrl(), properties);
    }

    UrsaDisklessTopicLifecycle(StreamCatalog catalog) {
        this.catalog = Objects.requireNonNull(catalog, "catalog must not be null");
    }

    @Override
    public CompletableFuture<List<ManagedTopic>> listManagedTopics() {
        return catalog.listStreamEntries(KafkaLogNaming.NAMESPACE)
                .thenApply(entries -> entries.stream()
                        .map(entry -> managedTopic(entry.identifier(), entry.properties()))
                        .filter(Objects::nonNull)
                        .toList());
    }

    @Override
    public CompletableFuture<Void> registerTopic(
            String topicName,
            Uuid topicId,
            int partitions,
            Map<String, String> properties,
            long sourceRevision) {
        if (partitions < 1) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("partitions must be at least 1"));
        }
        StreamIdentifier identifier = streamIdentifier(topicName, topicId);
        Map<String, String> propertySnapshot = KafkaLogNaming.streamProperties(
                topicName, topicId, properties, sourceRevision);
        return loadOrCreate(identifier, partitions, propertySnapshot)
                .thenCompose(metadata -> reconcileStream(
                        identifier, metadata, partitions, propertySnapshot, sourceRevision));
    }

    @Override
    public CompletableFuture<Void> unregisterTopic(String topicName, Uuid topicId) {
        return catalog.dropStream(streamIdentifier(topicName, topicId), true).thenApply(ignored -> null);
    }

    private CompletableFuture<StreamMetadata> loadOrCreate(
            StreamIdentifier identifier,
            int partitions,
            Map<String, String> properties) {
        return catalog.loadStream(identifier)
                .handle((metadata, error) -> {
                    if (error == null) {
                        return CompletableFuture.completedFuture(metadata);
                    }
                    Throwable failure = unwrap(error);
                    if (!(failure instanceof NoSuchStreamException)) {
                        return CompletableFuture.<StreamMetadata>failedFuture(failure);
                    }
                    Partitioning partitioning = new Partitioning(
                            PartitioningStrategy.INDEXED,
                            Map.of("numPartitions", String.valueOf(partitions)));
                    return catalog.createStream(
                                    identifier,
                                    new StreamConfig(),
                                    partitioning,
                                    new SchemaConfig(),
                                    properties)
                            .handle((created, createError) -> {
                                if (createError == null) {
                                    return CompletableFuture.completedFuture(created);
                                }
                                Throwable createFailure = unwrap(createError);
                                if (createFailure instanceof AlreadyExistsException) {
                                    return catalog.loadStream(identifier);
                                }
                                return CompletableFuture.<StreamMetadata>failedFuture(createFailure);
                            })
                            .thenCompose(future -> future);
                })
                .thenCompose(future -> future);
    }

    private CompletableFuture<Void> reconcileStream(
            StreamIdentifier identifier,
            StreamMetadata metadata,
            int partitions,
            Map<String, String> properties,
            long sourceRevision) {
        CompletableFuture<StreamMetadata> partitionFuture = metadata.partitioning().numPartitions() < partitions
                ? catalog.increasePartitions(identifier, partitions)
                : CompletableFuture.completedFuture(metadata);
        return partitionFuture.thenCompose(ignored ->
                catalog.replaceStreamProperties(identifier, properties, sourceRevision))
                .thenApply(ignored -> null);
    }

    private static ManagedTopic managedTopic(
            StreamIdentifier identifier,
            Map<String, String> properties) {
        if (!"true".equals(properties.get(KafkaLogNaming.KAFKA_MANAGED_PROPERTY))) {
            return null;
        }
        String topicName = properties.get(KafkaLogNaming.KAFKA_TOPIC_NAME_PROPERTY);
        String topicIdValue = properties.get(KafkaLogNaming.KAFKA_TOPIC_ID_PROPERTY);
        String sourceRevisionValue = properties.get(KafkaLogNaming.KAFKA_SOURCE_REVISION_PROPERTY);
        if (topicName == null || topicName.isBlank()
                || topicIdValue == null || topicIdValue.isBlank()
                || sourceRevisionValue == null || sourceRevisionValue.isBlank()) {
            return null;
        }
        Uuid topicId;
        long sourceRevision;
        try {
            topicId = Uuid.fromString(topicIdValue);
            sourceRevision = Long.parseLong(sourceRevisionValue);
        } catch (IllegalArgumentException error) {
            return null;
        }
        if (Uuid.ZERO_UUID.equals(topicId)
                || sourceRevision < 0
                || !streamIdentifier(topicName, topicId).equals(identifier)) {
            return null;
        }
        return new ManagedTopic(topicName, topicId, sourceRevision);
    }

    private static Throwable unwrap(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null
                ? error.getCause()
                : error;
    }

    @Override
    public void close() throws Exception {
        catalog.close();
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
