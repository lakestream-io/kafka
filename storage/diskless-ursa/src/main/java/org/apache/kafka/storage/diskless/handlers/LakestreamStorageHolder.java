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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.options.DeleteOption;
import io.streamnative.lakestream.api.CatalogType;
import io.streamnative.lakestream.api.Log;
import io.streamnative.lakestream.api.LogId;
import io.streamnative.lakestream.api.Stream;
import io.streamnative.lakestream.api.StreamIdentifier;
import io.streamnative.ursa.lakestream.impl.IndexedStreamCatalog;
import io.streamnative.ursa.lakestream.impl.StreamCatalogService;

import static io.streamnative.ursa.storage.impl.StorageFormat.STREAM_ID_GENERATOR_PATH;

/**
 * Holds an {@link IndexedStreamCatalog} instance for Kafka diskless storage.
 *
 * <p>Uses {@link StreamCatalogService} to bootstrap the full lakestream stack:
 * UrsaStorage, StorageApi, LogStorage, LogStateManager, EntryIndexCache, and OxiaClient.
 */
final class LakestreamStorageHolder implements Closeable {

    private static final long OXIA_CONNECT_TIMEOUT_SECONDS = 10;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final String EXTERNAL_READER_FACTORY_CLASS_PROP = "externalReaderFactoryClass";
    public static final String LEGACY_SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP =
            "ursa.externalReaderFactoryClass";
    static final String KAFKA_LAKEHOUSE_READER_FACTORY_CLASS =
            "io.streamnative.ursa.kafka.reader.KafkaLakehouseReaderFactory";
    static final String LEGACY_LAKEHOUSE_READER_FACTORY_CLASS =
            "io.streamnative.ursa.lakehouse.reader.LakehouseReaderFactory";
    private static final String OXIA_STORAGE_URL_PROP = "oxiaStorageUrl";
    private static final String SERVICE_NAME = "kafka-diskless-storage";

    private final IndexedStreamCatalog catalog;
    private final AsyncOxiaClient producerStateOxiaClient;
    private final OpenTelemetrySdk openTelemetrySdk;
    private final ConcurrentHashMap<String, TopicOperations> topicOperations = new ConcurrentHashMap<>();

    LakestreamStorageHolder(
            IndexedStreamCatalog catalog,
            AsyncOxiaClient producerStateOxiaClient,
            OpenTelemetrySdk openTelemetrySdk) {
        this.catalog = catalog;
        this.producerStateOxiaClient = producerStateOxiaClient;
        this.openTelemetrySdk = openTelemetrySdk;
    }

    IndexedStreamCatalog catalog() {
        return catalog;
    }

    AsyncOxiaClient oxiaClient() {
        return producerStateOxiaClient;
    }

    CompletableFuture<Void> registerPartition(TopicIdPartition tp, long streamId, Map<String, String> topicConfig) {
        String topic = tp.topic();
        Map<String, String> suppliedConfig = topicConfig != null ? Map.copyOf(topicConfig) : Map.of();
        return enqueueTopicOperation(topic, operations -> operations.enqueueRegistration(
                suppliedConfig,
                currentConfig -> catalog.registerExternalPartition(
                                streamIdentifier(topic),
                                tp.partition(),
                                streamId,
                                currentConfig)
                        // registerExternalPartition only updates properties while growing the partition count.
                        // Replace them explicitly so a config event racing this open cannot be overwritten by
                        // the snapshot captured when openLog started.
                        .thenCompose(ignored -> replaceTopicConfig(topic, currentConfig))))
                .future();
    }

    CompletableFuture<Void> asyncUpdateTopicConfig(String topic, Map<String, String> topicConfig) {
        Map<String, String> configSnapshot = topicConfig != null ? Map.copyOf(topicConfig) : Map.of();
        return enqueueTopicOperation(topic, operations -> operations.enqueueConfigUpdate(
                configSnapshot,
                () -> replaceTopicConfig(topic, configSnapshot)))
                .future();
    }

    CompletableFuture<Void> asyncDeleteTopicConfig(String topic) {
        TopicOperation deletion = enqueueTopicOperation(
                topic,
                operations -> operations.enqueueDeletion(() -> replaceTopicConfig(topic, Map.of())));
        deletion.future().whenComplete((ignored, error) -> {
            if (error == null) {
                evictTopicOperations(topic, deletion);
            }
        });
        return deletion.future();
    }

    private TopicOperation enqueueTopicOperation(
            String topic,
            Function<TopicOperations, QueuedOperation> enqueue) {
        while (true) {
            TopicOperations operations = topicOperations.computeIfAbsent(topic, ignored -> new TopicOperations());
            synchronized (operations) {
                // A successful deletion can evict an idle operations object while another thread still
                // holds a reference to it. Only enqueue while it is still the canonical object for this
                // topic; otherwise retry against the replacement.
                if (topicOperations.get(topic) != operations) {
                    continue;
                }
                QueuedOperation queued = enqueue.apply(operations);
                return new TopicOperation(operations, queued.sequence(), queued.future());
            }
        }
    }

    private void evictTopicOperations(String topic, TopicOperation deletion) {
        TopicOperations operations = deletion.operations();
        synchronized (operations) {
            // Keep the state when an update or same-name recreation was queued behind the deletion.
            // Its queue is needed to prevent that recreation from racing the old metadata drop.
            if (operations.isLatest(deletion.sequence())) {
                topicOperations.remove(topic, operations);
            }
        }
    }

    private CompletableFuture<Void> replaceTopicConfig(String topic, Map<String, String> topicConfig) {
        StreamIdentifier identifier = streamIdentifier(topic);
        return catalog.streamExists(identifier).thenCompose(exists -> {
            // Config changes are broadcast to every broker, including brokers that have never opened
            // a partition of this topic. The current metadata supplier will provide the full config
            // when a partition is opened later, so there is nothing to persist locally yet.
            if (!exists) {
                return CompletableFuture.completedFuture(null);
            }
            return catalog.loadStream(identifier).thenCompose(stream -> {
                Map<String, String> existing = Map.copyOf(stream.properties());
                closeStreamQuietly(stream);
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

    CompletableFuture<Void> deletePartitionData(TopicIdPartition tp) {
        String logName = KafkaLogNaming.logName(tp);
        String metadataPath = KafkaLogNaming.logMetadataPath(tp);
        return catalog.getOxiaClient().get(metadataPath).thenCompose(metadata -> {
            CompletableFuture<Void> deleteLogFuture;
            if (metadata == null || metadata.value().length == 0) {
                deleteLogFuture = CompletableFuture.completedFuture(null);
            } else {
                try {
                    JsonNode root = OBJECT_MAPPER.readTree(metadata.value());
                    JsonNode streamIdNode = root.get("streamId");
                    if (streamIdNode == null || streamIdNode.asLong(-1L) < 0) {
                        deleteLogFuture = CompletableFuture.completedFuture(null);
                    } else {
                        Log log = catalog.createLog(logName, LogId.of(streamIdNode.asLong()));
                        deleteLogFuture = deleteLogAndClose(log);
                    }
                } catch (IOException e) {
                    deleteLogFuture = CompletableFuture.failedFuture(e);
                }
            }

            return deleteLogFuture
                    .thenCompose(ignored -> catalog.getOxiaClient().delete(metadataPath))
                    // Delete catalog metadata before the keyed stream-ID mapping. If cleanup is
                    // interrupted between the two, recreation reuses the same now-empty stream ID
                    // instead of registering a new ID while stale catalog metadata points at the old one.
                    .thenCompose(ignored -> producerStateOxiaClient.delete(
                            STREAM_ID_GENERATOR_PATH + "/" + logName,
                            Set.of(DeleteOption.PartitionKey(STREAM_ID_GENERATOR_PATH))))
                    .thenApply(ignored -> null);
        });
    }

    static LakestreamStorageHolder create(UrsaStorageConfig config) throws Exception {
        IndexedStreamCatalog catalog = null;
        AsyncOxiaClient producerStateOxiaClient = null;
        CompletableFuture<AsyncOxiaClient> producerStateOxiaClientFuture = null;
        OpenTelemetrySdk openTelemetrySdk = null;
        try {
            openTelemetrySdk = createOpenTelemetrySdk();
            String oxiaUrl = config.getCatalogOxiaServiceUrl();
            Properties properties = buildStorageProperties(config);
            properties.setProperty("storageTier", "default");

            StreamCatalogService scs = new StreamCatalogService();
            catalog = scs.open(oxiaUrl, CatalogType.KAFKA, properties, openTelemetrySdk);

            producerStateOxiaClientFuture = new OxiaServiceUrl(config.getUrsaOxiaServiceUrl()).client();
            producerStateOxiaClient = producerStateOxiaClientFuture.get(
                    OXIA_CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            return new LakestreamStorageHolder(catalog, producerStateOxiaClient, openTelemetrySdk);
        } catch (Exception e) {
            try {
                if (catalog != null) {
                    catalog.close();
                }
            } catch (Exception ignored) {
            }
            closeOxiaClientAfterFailedCreate(producerStateOxiaClient, producerStateOxiaClientFuture, e);
            if (openTelemetrySdk != null) {
                openTelemetrySdk.close();
            }
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

    static OpenTelemetrySdk createOpenTelemetrySdk() {
        return AutoConfiguredOpenTelemetrySdk.builder()
                .addPropertiesSupplier(LakestreamStorageHolder::buildOpenTelemetryProperties)
                .build()
                .getOpenTelemetrySdk();
    }

    static Map<String, String> buildOpenTelemetryProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("otel.service.name", SERVICE_NAME);
        props.put("otel.metrics.exporter", "none");
        props.put("otel.traces.exporter", "none");
        props.put("otel.logs.exporter", "none");
        for (String name : System.getProperties().stringPropertyNames()) {
            if (name.startsWith("otel.")) {
                props.put(name, System.getProperty(name));
            }
        }
        return props;
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

        String externalReaderFactoryClass = normalizeExternalReaderFactoryClass(firstNonBlank(
                config.getConfiguredExternalReaderFactoryClass(),
                System.getProperty(LEGACY_SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP),
                UrsaStorageConfig.NOOP_EXTERNAL_READER_FACTORY_CLASS));
        properties.setProperty(EXTERNAL_READER_FACTORY_CLASS_PROP, externalReaderFactoryClass);
        if (!UrsaStorageConfig.NOOP_EXTERNAL_READER_FACTORY_CLASS.equals(externalReaderFactoryClass)) {
            if (isRemoteBackend(normalizedBackendType)) {
                properties.setProperty(
                        "compactionBackendStorageType", compactionBackendType(normalizedBackendType));
                properties.setProperty("compactionBucket", config.getCompactionBucket());
                properties.setProperty("compactionPrefix", config.getCompactionPrefix());
                properties.setProperty("compactionBucketRegion", config.getS3Region());
                setIfNotEmpty(config.getS3Endpoint(), v -> properties.setProperty("cloudStorageEndpoint", v));
            }
        }
        return properties;
    }

    private static boolean isRemoteBackend(String normalizedBackendType) {
        return !"LOCAL".equals(normalizedBackendType);
    }

    private static boolean isS3Backend(String normalizedBackendType) {
        return "S3".equals(normalizedBackendType);
    }

    private static String compactionBackendType(String normalizedBackendType) {
        // Ursa uses the Azure Blob SDK for WAL objects, while Hadoop 3.5 only supports
        // the ABFS connector for compacted Parquet files. Both can address an HNS-enabled
        // Azure storage account, but they intentionally use different backend identifiers.
        return "AZUREBLOB".equals(normalizedBackendType) ? "AZUREDFS" : normalizedBackendType;
    }

    private static String normalizeBackendType(String backendType) {
        String normalizedBackendType = backendType.toUpperCase(Locale.ROOT);
        if ("AZURE_BLOB".equals(normalizedBackendType) || "AZUREBLOB".equals(normalizedBackendType)) {
            return "AZUREBLOB";
        }
        return normalizedBackendType;
    }

    private static void setIfNotEmpty(String value, java.util.function.Consumer<String> setter) {
        if (value != null && !value.isEmpty()) {
            setter.accept(value);
        }
    }

    private static String firstNonBlank(String first, String second, String defaultValue) {
        if (first != null && !first.isBlank()) {
            return first;
        }
        if (second != null && !second.isBlank()) {
            return second;
        }
        return defaultValue;
    }

    private static String normalizeExternalReaderFactoryClass(String className) {
        return LEGACY_LAKEHOUSE_READER_FACTORY_CLASS.equals(className)
                ? KAFKA_LAKEHOUSE_READER_FACTORY_CLASS
                : className;
    }

    static StreamIdentifier streamIdentifier(String topic) {
        return StreamIdentifier.of(
                KafkaLogNaming.TENANT + "/" + KafkaLogNaming.NAMESPACE,
                KafkaLogNaming.DOMAIN + "/" + topic);
    }

    private static void closeStreamQuietly(Stream stream) {
        try {
            stream.close();
        } catch (Exception ignored) {
        }
    }

    private static void closeLogQuietly(Log log) {
        try {
            log.close();
        } catch (Exception ignored) {
        }
    }

    private static CompletableFuture<Void> deleteLogAndClose(Log log) {
        try {
            CompletableFuture<Void> deleteFuture = log.delete();
            if (deleteFuture == null) {
                throw new IllegalStateException("Log.delete returned null future");
            }
            return deleteFuture.whenComplete((ignored, error) -> closeLogQuietly(log));
        } catch (Throwable deleteError) {
            closeLogQuietly(log);
            return CompletableFuture.failedFuture(deleteError);
        }
    }

    private static final class TopicOperations {
        private CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);
        private volatile Map<String, String> latestTopicConfig;
        private long sequence;

        synchronized QueuedOperation enqueueRegistration(
                Map<String, String> suppliedConfig,
                Function<Map<String, String>, CompletableFuture<Void>> operation) {
            if (latestTopicConfig == null) {
                latestTopicConfig = suppliedConfig;
            }
            return enqueue(() -> operation.apply(
                    latestTopicConfig != null ? latestTopicConfig : suppliedConfig));
        }

        synchronized QueuedOperation enqueueConfigUpdate(
                Map<String, String> topicConfig,
                Supplier<CompletableFuture<Void>> operation) {
            latestTopicConfig = topicConfig;
            return enqueue(operation);
        }

        synchronized QueuedOperation enqueueDeletion(Supplier<CompletableFuture<Void>> operation) {
            latestTopicConfig = null;
            return enqueue(operation);
        }

        private QueuedOperation enqueue(Supplier<CompletableFuture<Void>> operation) {
            long operationSequence = ++sequence;
            CompletableFuture<Void> next = tail.handle((ignored, previousError) -> null)
                    .thenCompose(ignored -> operation.get());
            tail = next;
            return new QueuedOperation(operationSequence, next);
        }

        synchronized boolean isLatest(long operationSequence) {
            return sequence == operationSequence;
        }
    }

    private record QueuedOperation(long sequence, CompletableFuture<Void> future) {
    }

    private record TopicOperation(
            TopicOperations operations,
            long sequence,
            CompletableFuture<Void> future) {
    }

    @Override
    public void close() throws IOException {
        topicOperations.clear();
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
        try {
            if (openTelemetrySdk != null) {
                openTelemetrySdk.close();
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
