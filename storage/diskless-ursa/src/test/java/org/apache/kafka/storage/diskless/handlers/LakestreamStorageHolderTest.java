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
import org.apache.kafka.server.config.ServerLogConfigs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.streamnative.lakestream.api.Log;
import io.streamnative.lakestream.api.LogId;
import io.streamnative.lakestream.api.Stream;
import io.streamnative.lakestream.api.StreamIdentifier;
import io.streamnative.ursa.lakestream.impl.IndexedStreamCatalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LakestreamStorageHolderTest {

    private static final String SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP = "ursa.externalReaderFactoryClass";
    private static final String EXTERNAL_READER_FACTORY_CLASS_PROP = "externalReaderFactoryClass";
    private static final String NOOP_EXTERNAL_READER_FACTORY_CLASS =
            "io.streamnative.ursa.lakestream.reader.NoopCompactedObjectReaderFactory";
    private static final String SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP = "kopSchemaRegistryUrl";
    private static final String SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP =
            "kopSchemaRegistryHttpHeaderAuthorization";
    private static final String SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP =
            "kopSchemaRegistryHttpHeaderAuthorizationFile";

    @BeforeEach
    void setUp() {
        clearSystemProperties();
    }

    @AfterEach
    void tearDown() {
        clearSystemProperties();
    }

    private static void clearSystemProperties() {
        System.clearProperty(SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP);
        System.clearProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP);
        System.clearProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP);
        System.clearProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP);
        System.clearProperty("otel.service.name");
        System.clearProperty("otel.metrics.exporter");
        System.clearProperty("otel.traces.exporter");
        System.clearProperty("otel.logs.exporter");
        System.clearProperty("otel.exporter.otlp.endpoint");
    }

    @Test
    void testDoesNotInjectSchemaRegistryConfigWhenExternalReaderFactoryIsNoop() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of());

        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP, "http://example:8001");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP, "Bearer token");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP, "/mnt/secrets/token");

        Properties properties = LakestreamStorageHolder.buildStorageProperties(config);

        assertEquals(NOOP_EXTERNAL_READER_FACTORY_CLASS, properties.getProperty(EXTERNAL_READER_FACTORY_CLASS_PROP));
        assertFalse(properties.containsKey(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP));
        assertFalse(properties.containsKey(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP));
        assertFalse(properties.containsKey(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP));
    }

    @Test
    void testInjectsSchemaRegistryAuthorizationWhenExternalReaderEnabled() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of());

        System.setProperty(SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP,
                "io.streamnative.ursa.lakehouse.reader.LakehouseReaderFactory");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP, "http://example:8001");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP, "Bearer token");

        Properties properties = LakestreamStorageHolder.buildStorageProperties(config);

        assertEquals(
                "io.streamnative.ursa.lakehouse.reader.LakehouseReaderFactory",
                properties.getProperty(EXTERNAL_READER_FACTORY_CLASS_PROP));
        assertEquals("http://example:8001", properties.getProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP));
        assertEquals("Bearer token", properties.getProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP));
        assertFalse(properties.containsKey(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP));
    }

    @Test
    void testAuthorizationFileTakesPrecedenceOverAuthorizationValue() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of());

        System.setProperty(SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP,
                "io.streamnative.ursa.lakehouse.reader.LakehouseReaderFactory");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP, "http://example:8001");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP, "Bearer token");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP, "/mnt/secrets/token");

        Properties properties = LakestreamStorageHolder.buildStorageProperties(config);

        assertEquals("http://example:8001", properties.getProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP));
        assertEquals(
                "/mnt/secrets/token",
                properties.getProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP));
        assertFalse(properties.containsKey(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP));
    }

    @Test
    void testBlankAuthorizationIsIgnored() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of());

        System.setProperty(SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP,
                "io.streamnative.ursa.lakehouse.reader.LakehouseReaderFactory");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP, "http://example:8001");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP, "   ");

        Properties properties = LakestreamStorageHolder.buildStorageProperties(config);

        assertEquals("http://example:8001", properties.getProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP));
        assertFalse(properties.containsKey(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP));
        assertFalse(properties.containsKey(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP));
    }

    @Test
    void testBrokerConfigOverridesLegacySystemPropertyFallback() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_STORAGE_EXTERNAL_READER_FACTORY_CLASS_CONFIG,
                        "io.streamnative.ursa.lakehouse.reader.ConfigReaderFactory",
                ServerLogConfigs.URSA_STORAGE_KOP_SCHEMA_REGISTRY_URL_CONFIG, "http://config:8001",
                ServerLogConfigs.URSA_STORAGE_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_CONFIG,
                        "Bearer config-token"
        ));
        System.setProperty(SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP,
                "io.streamnative.ursa.lakehouse.reader.LegacyReaderFactory");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP, "http://legacy:8001");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP, "Bearer legacy-token");

        Properties properties = LakestreamStorageHolder.buildStorageProperties(config);

        assertEquals(
                "io.streamnative.ursa.lakehouse.reader.ConfigReaderFactory",
                properties.getProperty(EXTERNAL_READER_FACTORY_CLASS_PROP));
        assertEquals("http://config:8001", properties.getProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP));
        assertEquals(
                "Bearer config-token",
                properties.getProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP));
    }

    @Test
    void testGcsBackendUsesGenericObjectStorageProperties() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_STORAGE_BACKEND_TYPE_CONFIG, "GCS",
                ServerLogConfigs.URSA_STORAGE_PATH_CONFIG, "gcs-prefix",
                ServerLogConfigs.URSA_STORAGE_S3_ENDPOINT_CONFIG, "http://fake-gcs:4443",
                ServerLogConfigs.URSA_STORAGE_S3_BUCKET_CONFIG, "gcs-bucket",
                ServerLogConfigs.URSA_STORAGE_S3_REGION_CONFIG, "us-central1"
        ));

        Properties properties = LakestreamStorageHolder.buildStorageProperties(config);

        assertEquals("GCS", properties.getProperty("backendStorageType"));
        assertEquals("gcs-bucket", properties.getProperty("bucket"));
        assertEquals("gcs-prefix", properties.getProperty("prefix"));
        assertEquals("us-central1", properties.getProperty("region"));
        assertEquals("http://fake-gcs:4443", properties.getProperty("cloudStorageEndpoint"));
        assertFalse(properties.containsKey("s3AccessKeyId"));
        assertFalse(properties.containsKey("s3SecretAccessKey"));
    }

    @Test
    void testAzureBackendAliasesNormalizeToUrsaValue() throws Exception {
        verifyAzureBackendAlias("AZURE_BLOB");
        verifyAzureBackendAlias("AZUREBLOB");
    }

    @Test
    void testS3BackendStillIncludesS3SpecificCredentials() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_STORAGE_BACKEND_TYPE_CONFIG, "S3",
                ServerLogConfigs.URSA_STORAGE_PATH_CONFIG, "s3-prefix",
                ServerLogConfigs.URSA_STORAGE_S3_ENDPOINT_CONFIG, "http://localstack:4566",
                ServerLogConfigs.URSA_STORAGE_S3_BUCKET_CONFIG, "s3-bucket",
                ServerLogConfigs.URSA_STORAGE_S3_REGION_CONFIG, "us-east-1",
                ServerLogConfigs.URSA_STORAGE_S3_ACCESS_KEY_CONFIG, "access",
                ServerLogConfigs.URSA_STORAGE_S3_SECRET_KEY_CONFIG, "secret"
        ));

        Properties properties = LakestreamStorageHolder.buildStorageProperties(config);

        assertEquals("S3", properties.getProperty("backendStorageType"));
        assertEquals("access", properties.getProperty("s3AccessKeyId"));
        assertEquals("secret", properties.getProperty("s3SecretAccessKey"));
        assertEquals("s3-bucket", properties.getProperty("s3Bucket"));
        assertEquals("s3-prefix", properties.getProperty("s3Prefix"));
        assertEquals("us-east-1", properties.getProperty("s3Region"));
    }

    @Test
    void testCloseOwnsSeparateProducerStateOxiaClient() throws Exception {
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        AsyncOxiaClient producerStateOxiaClient = mock(AsyncOxiaClient.class);
        OpenTelemetrySdk openTelemetrySdk = mock(OpenTelemetrySdk.class);

        LakestreamStorageHolder holder = new LakestreamStorageHolder(
                catalog,
                producerStateOxiaClient,
                openTelemetrySdk);

        assertSame(catalog, holder.catalog());
        assertSame(producerStateOxiaClient, holder.oxiaClient());

        holder.close();

        verify(catalog).close();
        verify(producerStateOxiaClient).close();
        verify(openTelemetrySdk).close();
    }

    @Test
    void testTopicConfigUpdateReplacesStaleStreamProperties() throws Exception {
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        Stream stream = mock(Stream.class);
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier("test-topic");
        Map<String, String> updatedConfig = Map.of("keep", "new", "added", "value");

        when(catalog.streamExists(identifier)).thenReturn(CompletableFuture.completedFuture(true));
        when(catalog.loadStream(identifier)).thenReturn(CompletableFuture.completedFuture(stream));
        when(stream.properties()).thenReturn(Map.of("keep", "old", "stale", "remove"));
        when(catalog.removeStreamProperties(identifier, List.of("stale")))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(catalog.setStreamProperties(identifier, updatedConfig))
                .thenReturn(CompletableFuture.completedFuture(null));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null, null);
        holder.asyncUpdateTopicConfig("test-topic", updatedConfig).get();

        verify(stream).close();
        verify(catalog).removeStreamProperties(identifier, List.of("stale"));
        verify(catalog).setStreamProperties(identifier, updatedConfig);
    }

    @Test
    void testTopicConfigDeleteClearsPropertiesWithoutDroppingStreamMetadata() throws Exception {
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        Stream stream = mock(Stream.class);
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier("test-topic");

        when(catalog.streamExists(identifier)).thenReturn(CompletableFuture.completedFuture(true));
        when(catalog.loadStream(identifier)).thenReturn(CompletableFuture.completedFuture(stream));
        when(stream.properties()).thenReturn(Map.of("stale", "remove"));
        when(catalog.removeStreamProperties(identifier, List.of("stale")))
                .thenReturn(CompletableFuture.completedFuture(null));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null, null);
        holder.asyncDeleteTopicConfig("test-topic").get();

        verify(stream).close();
        verify(catalog).removeStreamProperties(identifier, List.of("stale"));
        verify(catalog, never()).setStreamProperties(identifier, Map.of());
        verify(catalog, never()).dropStream(identifier, false);
    }

    @Test
    void testTopicConfigDeleteToleratesAlreadyMissingStream() throws Exception {
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier("missing-topic");

        when(catalog.streamExists(identifier)).thenReturn(CompletableFuture.completedFuture(false));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null, null);
        holder.asyncDeleteTopicConfig("missing-topic").get();

        verify(catalog, never()).loadStream(identifier);
        verify(catalog, never()).dropStream(identifier, false);
    }

    @Test
    void testTopicConfigDeletePropagatesOtherCatalogFailures() {
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier("failed-delete-topic");

        when(catalog.streamExists(identifier)).thenReturn(
                CompletableFuture.failedFuture(new RuntimeException("catalog unavailable")));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null, null);

        assertThrows(ExecutionException.class, () -> holder.asyncDeleteTopicConfig("failed-delete-topic").get());
    }

    @Test
    void testDeleteThenSameNameRegistrationUsesOnlyRecreatedTopicConfig() throws Exception {
        String topic = "recreated-topic";
        Map<String, String> staleConfig = Map.of("stale", "old");
        Map<String, String> recreatedConfig = Map.of("retention.ms", "2000");
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(topic, 0));
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(topic);
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        Stream deletedStream = mock(Stream.class);
        Stream recreatedStream = mock(Stream.class);

        when(catalog.streamExists(identifier)).thenReturn(
                CompletableFuture.completedFuture(false),
                CompletableFuture.completedFuture(true),
                CompletableFuture.completedFuture(true));
        when(catalog.registerExternalPartition(identifier, 0, 29L, recreatedConfig))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(catalog.loadStream(identifier)).thenReturn(
                CompletableFuture.completedFuture(deletedStream),
                CompletableFuture.completedFuture(recreatedStream));
        when(deletedStream.properties()).thenReturn(staleConfig);
        when(recreatedStream.properties()).thenReturn(Map.of());
        when(catalog.removeStreamProperties(identifier, List.of("stale")))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(catalog.setStreamProperties(identifier, recreatedConfig))
                .thenReturn(CompletableFuture.completedFuture(null));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null, null);
        holder.asyncUpdateTopicConfig(topic, staleConfig).get();
        holder.asyncDeleteTopicConfig(topic).get();
        holder.registerPartition(tp, 29L, recreatedConfig).get();

        InOrder inOrder = org.mockito.Mockito.inOrder(catalog);
        inOrder.verify(catalog).removeStreamProperties(identifier, List.of("stale"));
        inOrder.verify(catalog).registerExternalPartition(identifier, 0, 29L, recreatedConfig);
        inOrder.verify(catalog).setStreamProperties(identifier, recreatedConfig);
        verify(catalog, never()).registerExternalPartition(identifier, 0, 29L, staleConfig);
        verify(catalog, never()).dropStream(identifier, false);
    }

    @Test
    void testDeleteKeepsQueueWhenConfigUpdateRacesSameNameRecreation() throws Exception {
        String topic = "racing-recreation-topic";
        Map<String, String> recreatedConfig = Map.of("retention.ms", "2000");
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(topic, 0));
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(topic);
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        Stream stream = mock(Stream.class);
        CompletableFuture<Boolean> deleteStreamExistsFuture = new CompletableFuture<>();
        CompletableFuture<Boolean> updateStreamExistsFuture = new CompletableFuture<>();

        when(catalog.streamExists(identifier)).thenReturn(
                deleteStreamExistsFuture,
                updateStreamExistsFuture,
                CompletableFuture.completedFuture(true));
        when(catalog.registerExternalPartition(identifier, 0, 31L, recreatedConfig))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(catalog.loadStream(identifier)).thenReturn(CompletableFuture.completedFuture(stream));
        when(stream.properties()).thenReturn(Map.of());
        when(catalog.setStreamProperties(identifier, recreatedConfig))
                .thenReturn(CompletableFuture.completedFuture(null));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null, null);
        CompletableFuture<Void> deleteFuture = holder.asyncDeleteTopicConfig(topic);
        CompletableFuture<Void> updateFuture = holder.asyncUpdateTopicConfig(topic, recreatedConfig);

        assertFalse(deleteFuture.isDone());
        assertFalse(updateFuture.isDone());
        deleteStreamExistsFuture.complete(false);
        deleteFuture.get();
        assertFalse(updateFuture.isDone());

        CompletableFuture<Void> registrationFuture = holder.registerPartition(tp, 31L, Map.of("stale", "snapshot"));
        assertFalse(registrationFuture.isDone());
        verify(catalog, never()).registerExternalPartition(identifier, 0, 31L, recreatedConfig);

        updateStreamExistsFuture.complete(false);
        updateFuture.get();
        registrationFuture.get();

        InOrder inOrder = org.mockito.Mockito.inOrder(catalog);
        inOrder.verify(catalog).registerExternalPartition(identifier, 0, 31L, recreatedConfig);
        inOrder.verify(catalog).setStreamProperties(identifier, recreatedConfig);
        verify(catalog, never()).dropStream(identifier, false);
    }

    @Test
    void testTopicConfigUpdateIsDeferredUntilAStreamExists() throws Exception {
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier("not-opened-topic");
        when(catalog.streamExists(identifier)).thenReturn(CompletableFuture.completedFuture(false));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null, null);
        holder.asyncUpdateTopicConfig("not-opened-topic", Map.of("retention.ms", "1000")).get();

        verify(catalog, never()).loadStream(identifier);
        verify(catalog, never()).setStreamProperties(identifier, Map.of("retention.ms", "1000"));
    }

    @Test
    void testTopicConfigUpdateRacingPartitionRegistrationWins() throws Exception {
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        Stream stream = mock(Stream.class);
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("race-topic", 0));
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp.topic());
        Map<String, String> initialConfig = Map.of("retention.ms", "1000");
        Map<String, String> latestConfig = Map.of("retention.ms", "2000");
        CompletableFuture<Void> registration = new CompletableFuture<>();

        when(catalog.registerExternalPartition(identifier, 0, 17L, initialConfig)).thenReturn(registration);
        when(catalog.streamExists(identifier)).thenReturn(CompletableFuture.completedFuture(true));
        when(catalog.loadStream(identifier)).thenReturn(CompletableFuture.completedFuture(stream));
        when(stream.properties()).thenReturn(Map.of());
        when(catalog.setStreamProperties(identifier, initialConfig))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(catalog.setStreamProperties(identifier, latestConfig))
                .thenReturn(CompletableFuture.completedFuture(null));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null, null);
        CompletableFuture<Void> openFuture = holder.registerPartition(tp, 17L, initialConfig);
        CompletableFuture<Void> updateFuture = holder.asyncUpdateTopicConfig(tp.topic(), latestConfig);

        assertFalse(openFuture.isDone());
        assertFalse(updateFuture.isDone());
        registration.complete(null);
        openFuture.get();
        updateFuture.get();

        InOrder inOrder = org.mockito.Mockito.inOrder(catalog);
        inOrder.verify(catalog).registerExternalPartition(identifier, 0, 17L, initialConfig);
        inOrder.verify(catalog).setStreamProperties(identifier, initialConfig);
        inOrder.verify(catalog).setStreamProperties(identifier, latestConfig);
    }

    @Test
    void testPartitionDeletionUsesSeparateProducerStateOxiaClient() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("delete-topic", 0));
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        AsyncOxiaClient catalogOxiaClient = mock(AsyncOxiaClient.class);
        AsyncOxiaClient producerStateOxiaClient = mock(AsyncOxiaClient.class);
        String metadataPath = KafkaManagedLedgerNaming.managedLedgerMetadataPath(tp);

        when(catalog.getOxiaClient()).thenReturn(catalogOxiaClient);
        when(catalogOxiaClient.get(metadataPath)).thenReturn(CompletableFuture.completedFuture(null));
        when(catalogOxiaClient.delete(metadataPath)).thenReturn(CompletableFuture.completedFuture(true));
        when(producerStateOxiaClient.delete(anyString(), anySet()))
                .thenReturn(CompletableFuture.completedFuture(true));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, producerStateOxiaClient, null);
        holder.deletePartitionData(tp).get();

        verify(catalogOxiaClient).get(metadataPath);
        InOrder deletionOrder = org.mockito.Mockito.inOrder(catalogOxiaClient, producerStateOxiaClient);
        deletionOrder.verify(catalogOxiaClient).delete(metadataPath);
        deletionOrder.verify(producerStateOxiaClient).delete(anyString(), anySet());
    }

    @Test
    void testPartitionDeletionDeletesResolvedLogAndExactKeyedStreamMapping() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("delete-topic", 3));
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        AsyncOxiaClient catalogOxiaClient = mock(AsyncOxiaClient.class);
        AsyncOxiaClient producerStateOxiaClient = mock(AsyncOxiaClient.class);
        GetResult metadata = mock(GetResult.class);
        Log log = mock(Log.class);
        String metadataPath = KafkaManagedLedgerNaming.managedLedgerMetadataPath(tp);
        String managedLedgerName = KafkaManagedLedgerNaming.managedLedgerName(tp);

        when(catalog.getOxiaClient()).thenReturn(catalogOxiaClient);
        when(catalogOxiaClient.get(metadataPath)).thenReturn(CompletableFuture.completedFuture(metadata));
        when(metadata.value()).thenReturn("{\"streamId\":42}".getBytes(StandardCharsets.UTF_8));
        when(catalog.createLog(LogId.of(42L))).thenReturn(log);
        when(log.delete()).thenReturn(CompletableFuture.completedFuture(null));
        when(producerStateOxiaClient.delete(anyString(), anySet()))
                .thenReturn(CompletableFuture.completedFuture(true));
        when(catalogOxiaClient.delete(metadataPath)).thenReturn(CompletableFuture.completedFuture(true));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, producerStateOxiaClient, null);
        holder.deletePartitionData(tp).get();

        InOrder deletionOrder = org.mockito.Mockito.inOrder(log, catalogOxiaClient, producerStateOxiaClient);
        deletionOrder.verify(log).delete();
        deletionOrder.verify(log).close();
        deletionOrder.verify(catalogOxiaClient).delete(metadataPath);
        deletionOrder.verify(producerStateOxiaClient).delete(
                eq("/stream-id-generator/" + managedLedgerName),
                anySet());
    }

    @Test
    void testPartitionDeletionKeepsKeyedStreamMappingWhenMetadataDeleteFails() {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("delete-topic", 0));
        IndexedStreamCatalog catalog = mock(IndexedStreamCatalog.class);
        AsyncOxiaClient catalogOxiaClient = mock(AsyncOxiaClient.class);
        AsyncOxiaClient producerStateOxiaClient = mock(AsyncOxiaClient.class);
        String metadataPath = KafkaManagedLedgerNaming.managedLedgerMetadataPath(tp);

        when(catalog.getOxiaClient()).thenReturn(catalogOxiaClient);
        when(catalogOxiaClient.get(metadataPath)).thenReturn(CompletableFuture.completedFuture(null));
        when(catalogOxiaClient.delete(metadataPath))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("delete failed")));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, producerStateOxiaClient, null);

        assertThrows(ExecutionException.class, () -> holder.deletePartitionData(tp).get());
        verify(producerStateOxiaClient, never()).delete(anyString(), anySet());
    }

    @Test
    void testLateProducerStateOxiaClientIsClosedAfterCreateFailure() throws Exception {
        CompletableFuture<AsyncOxiaClient> clientFuture = new CompletableFuture<>();
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);

        LakestreamStorageHolder.closeOxiaClientAfterFailedCreate(
                null, clientFuture, new Exception("creation failed"));
        clientFuture.complete(client);

        verify(client).close();
    }

    @Test
    void testOpenTelemetryPropertiesHonorDefaultsAndOverrides() {
        Map<String, String> defaults = LakestreamStorageHolder.buildOpenTelemetryProperties();
        assertEquals("kafka-diskless-storage", defaults.get("otel.service.name"));
        assertEquals("none", defaults.get("otel.metrics.exporter"));
        assertEquals("none", defaults.get("otel.traces.exporter"));
        assertEquals("none", defaults.get("otel.logs.exporter"));

        System.setProperty("otel.service.name", "custom-service");
        System.setProperty("otel.metrics.exporter", "otlp");
        System.setProperty("otel.exporter.otlp.endpoint", "http://collector:4317");

        Map<String, String> overridden = LakestreamStorageHolder.buildOpenTelemetryProperties();
        assertEquals("custom-service", overridden.get("otel.service.name"));
        assertEquals("otlp", overridden.get("otel.metrics.exporter"));
        assertEquals("http://collector:4317", overridden.get("otel.exporter.otlp.endpoint"));
        assertEquals("none", overridden.get("otel.traces.exporter"));
    }

    private static void verifyAzureBackendAlias(String backendType) throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_STORAGE_BACKEND_TYPE_CONFIG, backendType,
                ServerLogConfigs.URSA_STORAGE_PATH_CONFIG, "azure-prefix",
                ServerLogConfigs.URSA_STORAGE_S3_ENDPOINT_CONFIG, "https://account.blob.core.windows.net",
                ServerLogConfigs.URSA_STORAGE_S3_BUCKET_CONFIG, "account@container",
                ServerLogConfigs.URSA_STORAGE_S3_REGION_CONFIG, "unused-region"
        ));

        System.setProperty(SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP,
                "io.streamnative.ursa.lakehouse.reader.LakehouseReaderFactory");

        Properties properties = LakestreamStorageHolder.buildStorageProperties(config);

        assertEquals("AZUREBLOB", properties.getProperty("backendStorageType"));
        assertEquals("account@container", properties.getProperty("bucket"));
        assertEquals("azure-prefix", properties.getProperty("prefix"));
        assertEquals("unused-region", properties.getProperty("region"));
        assertEquals("https://account.blob.core.windows.net", properties.getProperty("cloudStorageEndpoint"));
        assertEquals("AZUREBLOB", properties.getProperty("compactionBackendStorageType"));
        assertEquals("account@container", properties.getProperty("compactionBucket"));
        assertEquals("/tmp/compaction-data", properties.getProperty("compactionPrefix"));
        assertEquals("unused-region", properties.getProperty("compactionBucketRegion"));
        assertFalse(properties.containsKey("s3AccessKeyId"));
        assertFalse(properties.containsKey("s3SecretAccessKey"));
    }
}
