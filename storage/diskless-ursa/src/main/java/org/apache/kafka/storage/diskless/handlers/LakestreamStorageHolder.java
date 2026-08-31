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
import org.apache.kafka.storage.diskless.OxiaServiceUrl;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.lakestream.api.Log;
import io.lakestream.api.Stream;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamCatalogLoader;
import io.lakestream.api.StreamIdentifier;
import io.oxia.client.api.AsyncOxiaClient;

/**
 * Holds a {@link StreamCatalog} instance for Kafka diskless storage.
 */
final class LakestreamStorageHolder implements Closeable {

    private static final long OXIA_CONNECT_TIMEOUT_SECONDS = 10;
    private static final String OXIA_STORAGE_URL_PROP = "oxiaStorageUrl";

    private final StreamCatalog catalog;
    private final AsyncOxiaClient producerStateOxiaClient;
    private final ConcurrentHashMap<StreamIdentifier, TopicOperations> topicOperations = new ConcurrentHashMap<>();
    private final Set<StreamIdentifier> deletedTopicStreams = ConcurrentHashMap.newKeySet();

    LakestreamStorageHolder(
            StreamCatalog catalog,
            AsyncOxiaClient producerStateOxiaClient) {
        this.catalog = catalog;
        this.producerStateOxiaClient = producerStateOxiaClient;
    }

    StreamCatalog catalog() {
        return catalog;
    }

    AsyncOxiaClient oxiaClient() {
        return producerStateOxiaClient;
    }

    CompletableFuture<Log> openPartition(TopicIdPartition tp, Map<String, String> topicConfig) {
        StreamIdentifier identifier = streamIdentifier(tp);
        if (deletedTopicStreams.contains(identifier)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Kafka topic incarnation is already deleted: " + tp));
        }
        Map<String, String> suppliedConfig = KafkaLogNaming.streamProperties(tp.topic(), topicConfig);
        return enqueueTopicOperation(identifier, operations -> operations.enqueueOpen(
                suppliedConfig,
                currentConfig -> {
                    if (deletedTopicStreams.contains(identifier)) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("Kafka topic incarnation is already deleted: " + tp));
                    }
                    return catalog.openExternalPartition(identifier, tp.partition(), currentConfig)
                            // Opening an external partition only updates properties while growing the partition
                            // count. Replace them explicitly so a config event racing this open cannot be
                            // overwritten by the snapshot captured when openLog started.
                            .thenCompose(log -> replaceTopicConfigAndReturnLog(identifier, currentConfig, log));
                }))
                .future();
    }

    CompletableFuture<Void> asyncUpdateTopicConfig(
            TopicIdPartition topicIdPartition,
            Map<String, String> topicConfig
    ) {
        StreamIdentifier identifier = streamIdentifier(topicIdPartition);
        if (deletedTopicStreams.contains(identifier)) {
            return CompletableFuture.completedFuture(null);
        }
        Map<String, String> configSnapshot = KafkaLogNaming.streamProperties(
                topicIdPartition.topic(), topicConfig);
        return enqueueTopicOperation(identifier, operations -> operations.enqueueConfigUpdate(
                configSnapshot,
                () -> deletedTopicStreams.contains(identifier)
                        ? CompletableFuture.completedFuture(null)
                        : replaceTopicConfig(identifier, configSnapshot)))
                .future();
    }

    CompletableFuture<Void> asyncDeleteTopicConfig(TopicIdPartition topicIdPartition) {
        StreamIdentifier identifier = streamIdentifier(topicIdPartition);
        deletedTopicStreams.add(identifier);
        TopicOperation<Void> deletion = enqueueTopicOperation(
                identifier,
                operations -> operations.enqueueDeletion(() -> replaceTopicConfig(identifier, Map.of())));
        deletion.future().whenComplete((ignored, error) -> {
            if (error == null) {
                evictTopicOperations(identifier, deletion);
            }
        });
        return deletion.future();
    }

    private <T> TopicOperation<T> enqueueTopicOperation(
            StreamIdentifier identifier,
            Function<TopicOperations, QueuedOperation<T>> enqueue) {
        while (true) {
            TopicOperations operations = topicOperations.computeIfAbsent(identifier, ignored -> new TopicOperations());
            synchronized (operations) {
                // A successful deletion can evict an idle operations object while another thread still
                // holds a reference to it. Only enqueue while it is still canonical for this exact
                // Kafka topic incarnation; otherwise retry against the replacement.
                if (topicOperations.get(identifier) != operations) {
                    continue;
                }
                QueuedOperation<T> queued = enqueue.apply(operations);
                return new TopicOperation<>(operations, queued.sequence(), queued.future());
            }
        }
    }

    private void evictTopicOperations(StreamIdentifier identifier, TopicOperation<Void> deletion) {
        TopicOperations operations = deletion.operations();
        synchronized (operations) {
            // Keep the state when another operation for this exact UUID-qualified stream was
            // queued behind the deletion. Its queue must remain canonical until that work settles.
            if (operations.isLatest(deletion.sequence())) {
                topicOperations.remove(identifier, operations);
            }
        }
    }

    private CompletableFuture<Void> replaceTopicConfig(
            StreamIdentifier identifier,
            Map<String, String> topicConfig) {
        return catalog.streamExists(identifier).thenCompose(exists -> {
            // Config changes are broadcast to every broker, including brokers that have never opened
            // a partition of this topic. The current metadata supplier will provide the full config
            // when a partition is opened later, so there is nothing to persist locally yet.
            if (!exists) {
                return CompletableFuture.completedFuture(null);
            }
            return catalog.loadStream(identifier).thenCompose(stream -> {
                final Map<String, String> existing;
                try {
                    existing = Map.copyOf(stream.properties());
                } finally {
                    closeStreamQuietly(stream);
                }
                List<String> staleKeys = existing.keySet().stream()
                        .filter(key -> !topicConfig.containsKey(key))
                        .toList();
                CompletableFuture<Void> removeFuture = staleKeys.isEmpty()
                        ? CompletableFuture.completedFuture(null)
                        : catalog.removeStreamProperties(identifier, staleKeys);
                return removeFuture.thenCompose(ignored -> topicConfig.isEmpty()
                        ? CompletableFuture.completedFuture(null)
                        : catalog.setStreamProperties(identifier, topicConfig));
            });
        });
    }

    private CompletableFuture<Log> replaceTopicConfigAndReturnLog(
            StreamIdentifier identifier,
            Map<String, String> topicConfig,
            Log log) {
        if (log == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("StreamCatalog.openExternalPartition returned null log"));
        }
        final CompletableFuture<Void> replaceFuture;
        try {
            replaceFuture = replaceTopicConfig(identifier, topicConfig);
        } catch (Throwable error) {
            closeLogAfterFailure(log, error);
            return CompletableFuture.failedFuture(error);
        }
        if (replaceFuture == null) {
            IllegalStateException error = new IllegalStateException("replaceTopicConfig returned null future");
            closeLogAfterFailure(log, error);
            return CompletableFuture.failedFuture(error);
        }
        return replaceFuture.handle((ignored, error) -> {
            if (error != null) {
                Throwable failure = error instanceof CompletionException
                        && error.getCause() != null ? error.getCause() : error;
                closeLogAfterFailure(log, failure);
                throw new CompletionException(failure);
            }
            return log;
        });
    }

    CompletableFuture<Void> deletePartitionData(TopicIdPartition tp) {
        StreamIdentifier identifier = streamIdentifier(tp);
        // Fence this exact topic incarnation before queuing deletion. A lazy open that was
        // admitted before the metadata delete either completes ahead of this operation or sees
        // the tombstone and fails; it cannot re-register the old stream after cleanup.
        deletedTopicStreams.add(identifier);
        return enqueueTopicOperation(
                identifier,
                operations -> operations.enqueueCleanup(
                        () -> catalog.deleteExternalPartition(identifier, tp.partition())))
                .future();
    }

    static LakestreamStorageHolder create(UrsaStorageConfig config) throws Exception {
        StreamCatalog catalog = null;
        AsyncOxiaClient producerStateOxiaClient = null;
        CompletableFuture<AsyncOxiaClient> producerStateOxiaClientFuture = null;
        try {
            String oxiaUrl = config.getCatalogOxiaServiceUrl();
            Properties properties = buildStorageProperties(config);

            catalog = StreamCatalogLoader.open(oxiaUrl, properties);

            producerStateOxiaClientFuture = new OxiaServiceUrl(config.getUrsaOxiaServiceUrl()).client();
            producerStateOxiaClient = producerStateOxiaClientFuture.get(
                    OXIA_CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            return new LakestreamStorageHolder(catalog, producerStateOxiaClient);
        } catch (Exception e) {
            try {
                if (catalog != null) {
                    catalog.close();
                }
            } catch (Exception ignored) {
            }
            closeOxiaClientAfterFailedCreate(producerStateOxiaClient, producerStateOxiaClientFuture, e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw e;
        }
    }

    static void closeOxiaClientAfterFailedCreate(
            AsyncOxiaClient oxiaClient,
            CompletableFuture<AsyncOxiaClient> oxiaClientFuture,
            Exception createFailure) {
        if (oxiaClient != null) {
            try {
                oxiaClient.close();
            } catch (Exception closeFailure) {
                createFailure.addSuppressed(closeFailure);
            }
        } else if (oxiaClientFuture != null) {
            oxiaClientFuture.thenAccept(client -> {
                try {
                    client.close();
                } catch (Exception ignored) {
                }
            });
        }
    }

    static Properties buildStorageProperties(UrsaStorageConfig config) {
        Properties properties = new Properties();
        String normalizedBackendType = normalizeBackendType(config.getBackendType());
        properties.setProperty("backendStorageType", normalizedBackendType);
        properties.setProperty("storagePath", config.getStoragePath());
        properties.setProperty(OXIA_STORAGE_URL_PROP, config.getUrsaOxiaServiceUrl());
        properties.setProperty("writeBufferFlushIntervalMs", String.valueOf(config.getWriteBufferFlushIntervalMs()));
        properties.setProperty("writeBufferSize", String.valueOf(config.getWriteBufferSize()));
        properties.setProperty("writeBufferFlushSize", String.valueOf(config.getWriteBufferFlushSize()));

        if (isRemoteBackend(normalizedBackendType)) {
            setIfNotEmpty(config.getS3Endpoint(), v -> properties.setProperty("cloudStorageEndpoint", v));
            setIfNotEmpty(config.getS3Bucket(), v -> properties.setProperty("bucket", v));
            setIfNotEmpty(config.getStoragePath(), v -> properties.setProperty("prefix", v));
            setIfNotEmpty(config.getS3Region(), v -> properties.setProperty("region", v));
        }

        if (isS3Backend(normalizedBackendType)) {
            setIfNotEmpty(config.getS3AccessKey(), v -> properties.setProperty("s3AccessKeyId", v));
            setIfNotEmpty(config.getS3SecretKey(), v -> properties.setProperty("s3SecretAccessKey", v));
            setIfNotEmpty(config.getS3SessionToken(), v -> properties.setProperty("s3SessionToken", v));
            if (config.getS3PathStyleAccess() != null) {
                properties.setProperty("s3PathStyleAccess", String.valueOf(config.getS3PathStyleAccess()));
            }
            // Deprecated fields, keep for compatibility with older configs.
            setIfNotEmpty(config.getS3Bucket(), v -> properties.setProperty("s3Bucket", v));
            setIfNotEmpty(config.getStoragePath(), v -> properties.setProperty("s3Prefix", v));
            setIfNotEmpty(config.getS3Region(), v -> properties.setProperty("s3Region", v));
        }

        setIfNotEmpty(config.getCompactionBucket(), v -> properties.setProperty("compactionBucket", v));
        setIfNotEmpty(config.getCompactionPrefix(), v -> properties.setProperty("compactionPrefix", v));
        return properties;
    }

    private static boolean isRemoteBackend(String normalizedBackendType) {
        return !"LOCAL".equals(normalizedBackendType);
    }

    private static boolean isS3Backend(String normalizedBackendType) {
        return "S3".equals(normalizedBackendType);
    }

    private static String normalizeBackendType(String backendType) {
        String normalizedBackendType = backendType.toUpperCase(Locale.ROOT);
        if ("AZURE_BLOB".equals(normalizedBackendType) || "AZUREBLOB".equals(normalizedBackendType)) {
            return "AZUREBLOB";
        }
        return normalizedBackendType;
    }

    private static void setIfNotEmpty(String value, Consumer<String> setter) {
        if (value != null && !value.isEmpty()) {
            setter.accept(value);
        }
    }

    static StreamIdentifier streamIdentifier(TopicIdPartition tp) {
        return StreamIdentifier.of(KafkaLogNaming.NAMESPACE, KafkaLogNaming.streamName(tp));
    }

    private static void closeStreamQuietly(Stream stream) {
        try {
            stream.close();
        } catch (Exception ignored) {
        }
    }

    private static void closeLogAfterFailure(Log log, Throwable failure) {
        try {
            log.close();
        } catch (Throwable closeError) {
            if (failure != closeError) {
                failure.addSuppressed(closeError);
            }
        }
    }

    private static final class TopicOperations {
        private CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);
        private volatile Map<String, String> latestTopicConfig;
        private long sequence;

        synchronized QueuedOperation<Log> enqueueOpen(
                Map<String, String> suppliedConfig,
                Function<Map<String, String>, CompletableFuture<Log>> operation) {
            if (latestTopicConfig == null) {
                latestTopicConfig = suppliedConfig;
            }
            return enqueue(() -> operation.apply(
                    latestTopicConfig != null ? latestTopicConfig : suppliedConfig));
        }

        synchronized QueuedOperation<Void> enqueueConfigUpdate(
                Map<String, String> topicConfig,
                Supplier<CompletableFuture<Void>> operation) {
            latestTopicConfig = topicConfig;
            return enqueue(operation);
        }

        synchronized QueuedOperation<Void> enqueueDeletion(Supplier<CompletableFuture<Void>> operation) {
            latestTopicConfig = null;
            return enqueue(operation);
        }

        synchronized QueuedOperation<Void> enqueueCleanup(Supplier<CompletableFuture<Void>> operation) {
            return enqueue(operation);
        }

        private <T> QueuedOperation<T> enqueue(Supplier<CompletableFuture<T>> operation) {
            long operationSequence = ++sequence;
            CompletableFuture<T> next = tail.handle((ignored, previousError) -> null)
                    .thenCompose(ignored -> operation.get());
            tail = next.thenApply(ignored -> null);
            return new QueuedOperation<>(operationSequence, next);
        }

        synchronized boolean isLatest(long operationSequence) {
            return sequence == operationSequence;
        }
    }

    private record QueuedOperation<T>(long sequence, CompletableFuture<T> future) {
    }

    private record TopicOperation<T>(
            TopicOperations operations,
            long sequence,
            CompletableFuture<T> future) {
    }

    @Override
    public void close() throws IOException {
        topicOperations.clear();
        deletedTopicStreams.clear();
        List<Exception> failures = new ArrayList<>();
        try {
            if (catalog != null) {
                catalog.close();
            }
        } catch (Exception e) {
            failures.add(e);
        }
        try {
            if (producerStateOxiaClient != null) {
                producerStateOxiaClient.close();
            }
        } catch (Exception e) {
            failures.add(e);
        }
        if (!failures.isEmpty()) {
            IOException failure = new IOException(failures.get(0));
            failures.stream().skip(1).forEach(failure::addSuppressed);
            throw failure;
        }
    }
}
