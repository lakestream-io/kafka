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

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.lakestream.api.Log;
import io.lakestream.api.Stream;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamIdentifier;
import io.oxia.client.api.AsyncOxiaClient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class LakestreamStorageHolderTest {

    @Test
    void testUsesNeutralStoragePropertiesWithoutReaderImplementationDetails() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_OXIA_SERVICE_URL_CONFIG, "oxia://storage:6648/kafka",
                ServerLogConfigs.URSA_STORAGE_BACKEND_TYPE_CONFIG, "GCS",
                ServerLogConfigs.URSA_STORAGE_PATH_CONFIG, "gcs-prefix",
                ServerLogConfigs.URSA_STORAGE_S3_ENDPOINT_CONFIG, "http://fake-gcs:4443",
                ServerLogConfigs.URSA_STORAGE_S3_BUCKET_CONFIG, "gcs-bucket",
                ServerLogConfigs.URSA_STORAGE_S3_REGION_CONFIG, "us-central1",
                ServerLogConfigs.URSA_STORAGE_COMPACTION_BUCKET_CONFIG, "compacted-bucket",
                ServerLogConfigs.URSA_STORAGE_COMPACTION_PREFIX_CONFIG, "compacted-prefix"
        ));

        Properties properties = LakestreamStorageHolder.buildStorageProperties(config);

        assertEquals("oxia://storage:6648/kafka", properties.getProperty("oxiaStorageUrl"));
        assertEquals("GCS", properties.getProperty("backendStorageType"));
        assertEquals("gcs-bucket", properties.getProperty("bucket"));
        assertEquals("gcs-prefix", properties.getProperty("prefix"));
        assertEquals("us-central1", properties.getProperty("region"));
        assertEquals("compacted-bucket", properties.getProperty("compactionBucket"));
        assertEquals("compacted-prefix", properties.getProperty("compactionPrefix"));
        assertFalse(properties.containsKey("externalReaderFactoryClass"));
        assertFalse(properties.containsKey("compactedObjectReaderFactoryClass"));
        assertFalse(properties.containsKey("compactionBackendStorageType"));
        assertFalse(properties.containsKey("compactionBucketRegion"));
        assertFalse(properties.containsKey("storageTier"));
    }

    @Test
    void testAzureBackendAliasesNormalizeWithoutDerivingReaderBackend() throws Exception {
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
                ServerLogConfigs.URSA_STORAGE_S3_SECRET_KEY_CONFIG, "secret",
                ServerLogConfigs.URSA_STORAGE_S3_SESSION_TOKEN_CONFIG, "session",
                ServerLogConfigs.URSA_STORAGE_S3_PATH_STYLE_ACCESS_CONFIG, "false"
        ));

        Properties properties = LakestreamStorageHolder.buildStorageProperties(config);

        assertEquals("S3", properties.getProperty("backendStorageType"));
        assertEquals("access", properties.getProperty("s3AccessKeyId"));
        assertEquals("secret", properties.getProperty("s3SecretAccessKey"));
        assertEquals("session", properties.getProperty("s3SessionToken"));
        assertEquals("false", properties.getProperty("s3PathStyleAccess"));
        assertEquals("s3-bucket", properties.getProperty("s3Bucket"));
        assertEquals("s3-prefix", properties.getProperty("s3Prefix"));
        assertEquals("us-east-1", properties.getProperty("s3Region"));
    }

    @Test
    void testCloseOwnsCatalogAndSeparateProducerStateOxiaClient() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        AsyncOxiaClient producerStateOxiaClient = mock(AsyncOxiaClient.class);
        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, producerStateOxiaClient);

        assertSame(catalog, holder.catalog());
        assertSame(producerStateOxiaClient, holder.oxiaClient());

        holder.close();

        verify(catalog).close();
        verify(producerStateOxiaClient).close();
    }

    @Test
    void testTopicConfigUpdateReplacesStaleStreamProperties() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log log = mock(Log.class);
        Stream stream = mock(Stream.class);
        TopicIdPartition tp = topicIdPartition("test-topic");
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);
        Map<String, String> updatedConfig = Map.of("keep", "new", "added", "value");

        when(catalog.openExternalPartition(identifier, 0, streamProperties(tp, Map.of())))
                .thenReturn(CompletableFuture.completedFuture(log));
        when(catalog.streamExists(identifier))
                .thenReturn(CompletableFuture.completedFuture(true))
                .thenReturn(CompletableFuture.completedFuture(true));
        when(catalog.loadStream(identifier)).thenReturn(CompletableFuture.completedFuture(stream));
        when(stream.properties())
                .thenReturn(Map.of())
                .thenReturn(Map.of("keep", "old", "stale", "remove"));
        when(catalog.removeStreamProperties(identifier, List.of("stale")))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(catalog.setStreamProperties(identifier, streamProperties(tp, Map.of())))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(catalog.setStreamProperties(identifier, streamProperties(tp, updatedConfig)))
                .thenReturn(CompletableFuture.completedFuture(null));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null);
        assertSame(log, holder.openPartition(tp, Map.of()).get());
        holder.asyncUpdateTopicConfig(tp, updatedConfig).get();

        verify(catalog).removeStreamProperties(identifier, List.of("stale"));
        verify(catalog).setStreamProperties(identifier, streamProperties(tp, updatedConfig));
    }

    @Test
    void testTopicConfigDeleteToleratesAlreadyMissingStream() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        TopicIdPartition tp = topicIdPartition("missing-topic");
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);
        when(catalog.streamExists(identifier)).thenReturn(CompletableFuture.completedFuture(false));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null);
        holder.asyncDeleteTopicConfig(tp).get();

        verify(catalog, never()).loadStream(identifier);
        verify(catalog, never()).dropStream(identifier, false);
    }

    @Test
    void testTopicConfigDeleteClearsPropertiesWithoutDroppingStreamMetadata() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log log = mock(Log.class);
        Stream stream = mock(Stream.class);
        TopicIdPartition tp = topicIdPartition("test-topic");
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);

        when(catalog.openExternalPartition(identifier, 0, streamProperties(tp, Map.of())))
                .thenReturn(CompletableFuture.completedFuture(log));
        when(catalog.streamExists(identifier))
                .thenReturn(CompletableFuture.completedFuture(false))
                .thenReturn(CompletableFuture.completedFuture(true));
        when(catalog.loadStream(identifier)).thenReturn(CompletableFuture.completedFuture(stream));
        when(stream.properties()).thenReturn(Map.of("stale", "remove"));
        when(catalog.removeStreamProperties(identifier, List.of("stale")))
                .thenReturn(CompletableFuture.completedFuture(null));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null);
        holder.openPartition(tp, Map.of()).get();
        holder.asyncDeleteTopicConfig(tp).get();

        verify(stream).close();
        verify(catalog).removeStreamProperties(identifier, List.of("stale"));
        verify(catalog, never()).setStreamProperties(identifier, Map.of());
        verify(catalog, never()).dropStream(identifier, false);
    }

    @Test
    void testTopicConfigDeletePropagatesOtherCatalogFailures() {
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log log = mock(Log.class);
        TopicIdPartition tp = topicIdPartition("failed-delete-topic");
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);

        when(catalog.openExternalPartition(identifier, 0, streamProperties(tp, Map.of())))
                .thenReturn(CompletableFuture.completedFuture(log));
        when(catalog.streamExists(identifier))
                .thenReturn(CompletableFuture.completedFuture(false))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("catalog unavailable")));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null);
        holder.openPartition(tp, Map.of()).join();

        assertThrows(ExecutionException.class, () -> holder.asyncDeleteTopicConfig(tp).get());
    }

    @Test
    void testDeleteThenSameNameOpenUsesOnlyRecreatedTopicConfig() throws Exception {
        String topic = "recreated-topic";
        Map<String, String> staleConfig = Map.of("stale", "old");
        Map<String, String> recreatedConfig = Map.of("retention.ms", "2000");
        TopicPartition partition = new TopicPartition(topic, 0);
        TopicIdPartition deletedTp = new TopicIdPartition(Uuid.randomUuid(), partition);
        TopicIdPartition recreatedTp = new TopicIdPartition(Uuid.randomUuid(), partition);
        StreamIdentifier deletedIdentifier = LakestreamStorageHolder.streamIdentifier(deletedTp);
        StreamIdentifier recreatedIdentifier = LakestreamStorageHolder.streamIdentifier(recreatedTp);
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log deletedLog = mock(Log.class);
        Log recreatedLog = mock(Log.class);
        Stream deletedStream = mock(Stream.class);
        Stream recreatedStream = mock(Stream.class);

        when(catalog.openExternalPartition(deletedIdentifier, 0, streamProperties(deletedTp, staleConfig)))
                .thenReturn(CompletableFuture.completedFuture(deletedLog));
        when(catalog.streamExists(deletedIdentifier))
                .thenReturn(CompletableFuture.completedFuture(false))
                .thenReturn(CompletableFuture.completedFuture(true));
        when(catalog.openExternalPartition(recreatedIdentifier, 0, streamProperties(recreatedTp, recreatedConfig)))
                .thenReturn(CompletableFuture.completedFuture(recreatedLog));
        when(catalog.streamExists(recreatedIdentifier)).thenReturn(CompletableFuture.completedFuture(true));
        when(catalog.loadStream(deletedIdentifier)).thenReturn(CompletableFuture.completedFuture(deletedStream));
        when(catalog.loadStream(recreatedIdentifier)).thenReturn(CompletableFuture.completedFuture(recreatedStream));
        when(deletedStream.properties()).thenReturn(staleConfig);
        when(recreatedStream.properties()).thenReturn(Map.of());
        when(catalog.removeStreamProperties(deletedIdentifier, List.of("stale")))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(catalog.setStreamProperties(recreatedIdentifier, streamProperties(recreatedTp, recreatedConfig)))
                .thenReturn(CompletableFuture.completedFuture(null));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null);
        holder.openPartition(deletedTp, staleConfig).get();
        holder.asyncDeleteTopicConfig(deletedTp).get();
        holder.openPartition(recreatedTp, recreatedConfig).get();

        InOrder order = inOrder(catalog);
        order.verify(catalog).removeStreamProperties(deletedIdentifier, List.of("stale"));
        order.verify(catalog).openExternalPartition(
                recreatedIdentifier, 0, streamProperties(recreatedTp, recreatedConfig));
        order.verify(catalog).setStreamProperties(
                recreatedIdentifier, streamProperties(recreatedTp, recreatedConfig));
        verify(catalog, never()).openExternalPartition(recreatedIdentifier, 0, staleConfig);
        verify(catalog, never()).dropStream(deletedIdentifier, false);
    }

    @Test
    void testLateOldIncarnationDeleteDoesNotClearRecreatedTopicConfig() throws Exception {
        String topic = "late-delete-recreated-topic";
        TopicPartition partition = new TopicPartition(topic, 0);
        TopicIdPartition deletedTp = new TopicIdPartition(Uuid.randomUuid(), partition);
        TopicIdPartition recreatedTp = new TopicIdPartition(Uuid.randomUuid(), partition);
        StreamIdentifier deletedIdentifier = LakestreamStorageHolder.streamIdentifier(deletedTp);
        StreamIdentifier recreatedIdentifier = LakestreamStorageHolder.streamIdentifier(recreatedTp);
        Map<String, String> deletedConfig = Map.of("stale", "old");
        Map<String, String> recreatedConfig = Map.of("retention.ms", "2000");
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log deletedLog = mock(Log.class);
        Log recreatedLog = mock(Log.class);
        Stream deletedStream = mock(Stream.class);

        when(catalog.openExternalPartition(deletedIdentifier, 0, streamProperties(deletedTp, deletedConfig)))
                .thenReturn(CompletableFuture.completedFuture(deletedLog));
        when(catalog.openExternalPartition(recreatedIdentifier, 0, streamProperties(recreatedTp, recreatedConfig)))
                .thenReturn(CompletableFuture.completedFuture(recreatedLog));
        when(catalog.streamExists(deletedIdentifier))
                .thenReturn(CompletableFuture.completedFuture(false))
                .thenReturn(CompletableFuture.completedFuture(true));
        when(catalog.streamExists(recreatedIdentifier)).thenReturn(CompletableFuture.completedFuture(false));
        when(catalog.loadStream(deletedIdentifier)).thenReturn(CompletableFuture.completedFuture(deletedStream));
        when(deletedStream.properties()).thenReturn(deletedConfig);
        when(catalog.removeStreamProperties(deletedIdentifier, List.of("stale")))
                .thenReturn(CompletableFuture.completedFuture(null));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null);
        holder.openPartition(deletedTp, deletedConfig).get();
        holder.openPartition(recreatedTp, recreatedConfig).get();
        holder.asyncDeleteTopicConfig(deletedTp).get();

        verify(catalog).removeStreamProperties(deletedIdentifier, List.of("stale"));
        verify(catalog, never()).loadStream(recreatedIdentifier);
        verify(catalog).openExternalPartition(
                recreatedIdentifier, 0, streamProperties(recreatedTp, recreatedConfig));
    }

    @Test
    void testDeleteKeepsQueueWhenConfigUpdateRacesSameNameRecreation() throws Exception {
        String topic = "racing-recreation-topic";
        Map<String, String> recreatedConfig = Map.of("retention.ms", "2000");
        TopicPartition partition = new TopicPartition(topic, 0);
        TopicIdPartition deletedTp = new TopicIdPartition(Uuid.randomUuid(), partition);
        TopicIdPartition recreatedTp = new TopicIdPartition(Uuid.randomUuid(), partition);
        StreamIdentifier deletedIdentifier = LakestreamStorageHolder.streamIdentifier(deletedTp);
        StreamIdentifier recreatedIdentifier = LakestreamStorageHolder.streamIdentifier(recreatedTp);
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log deletedLog = mock(Log.class);
        Log recreatedLog = mock(Log.class);
        Stream stream = mock(Stream.class);
        CompletableFuture<Boolean> deleteStreamExistsFuture = new CompletableFuture<>();

        when(catalog.openExternalPartition(deletedIdentifier, 0, streamProperties(deletedTp, Map.of())))
                .thenReturn(CompletableFuture.completedFuture(deletedLog));
        when(catalog.streamExists(deletedIdentifier))
                .thenReturn(CompletableFuture.completedFuture(false))
                .thenReturn(deleteStreamExistsFuture);
        when(catalog.openExternalPartition(recreatedIdentifier, 0, streamProperties(recreatedTp, recreatedConfig)))
                .thenReturn(CompletableFuture.completedFuture(recreatedLog));
        when(catalog.streamExists(recreatedIdentifier))
                .thenReturn(CompletableFuture.completedFuture(false))
                .thenReturn(CompletableFuture.completedFuture(true));
        when(catalog.loadStream(recreatedIdentifier)).thenReturn(CompletableFuture.completedFuture(stream));
        when(stream.properties()).thenReturn(Map.of());
        when(catalog.setStreamProperties(recreatedIdentifier, streamProperties(recreatedTp, recreatedConfig)))
                .thenReturn(CompletableFuture.completedFuture(null));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null);
        holder.openPartition(deletedTp, Map.of()).get();
        CompletableFuture<Void> deleteFuture = holder.asyncDeleteTopicConfig(deletedTp);
        CompletableFuture<Void> updateFuture = holder.asyncUpdateTopicConfig(recreatedTp, recreatedConfig);

        assertFalse(deleteFuture.isDone());
        assertTrue(updateFuture.isDone());
        deleteStreamExistsFuture.complete(false);
        deleteFuture.get();
        updateFuture.get();

        assertSame(recreatedLog, holder.openPartition(
                recreatedTp, Map.of("stale", "snapshot")).get());

        InOrder order = inOrder(catalog);
        order.verify(catalog).openExternalPartition(
                recreatedIdentifier, 0, streamProperties(recreatedTp, recreatedConfig));
        order.verify(catalog).setStreamProperties(
                recreatedIdentifier, streamProperties(recreatedTp, recreatedConfig));
        verify(catalog, never()).dropStream(deletedIdentifier, false);
    }

    @Test
    void testTopicConfigUpdateIsDeferredUntilAStreamExists() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        TopicIdPartition tp = topicIdPartition("not-opened-topic");
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);
        when(catalog.streamExists(identifier)).thenReturn(CompletableFuture.completedFuture(false));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null);
        holder.asyncUpdateTopicConfig(tp, Map.of("retention.ms", "1000")).get();

        verify(catalog, never()).loadStream(identifier);
        verify(catalog, never()).setStreamProperties(identifier, Map.of("retention.ms", "1000"));
    }

    @Test
    void testTopicConfigUpdateRacingPartitionOpenWins() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log log = mock(Log.class);
        Stream stream = mock(Stream.class);
        TopicIdPartition tp = topicIdPartition("race-topic");
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);
        Map<String, String> initialConfig = Map.of("retention.ms", "1000");
        Map<String, String> latestConfig = Map.of("retention.ms", "2000");
        CompletableFuture<Log> opening = new CompletableFuture<>();

        when(catalog.openExternalPartition(identifier, 0, streamProperties(tp, initialConfig))).thenReturn(opening);
        when(catalog.streamExists(identifier)).thenReturn(CompletableFuture.completedFuture(true));
        when(catalog.loadStream(identifier)).thenReturn(CompletableFuture.completedFuture(stream));
        when(stream.properties()).thenReturn(Map.of());
        when(catalog.setStreamProperties(identifier, streamProperties(tp, initialConfig)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(catalog.setStreamProperties(identifier, streamProperties(tp, latestConfig)))
                .thenReturn(CompletableFuture.completedFuture(null));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null);
        CompletableFuture<Log> openFuture = holder.openPartition(tp, initialConfig);
        CompletableFuture<Void> updateFuture = holder.asyncUpdateTopicConfig(tp, latestConfig);

        assertFalse(openFuture.isDone());
        assertFalse(updateFuture.isDone());
        opening.complete(log);
        assertSame(log, openFuture.get());
        updateFuture.get();

        InOrder order = inOrder(catalog);
        order.verify(catalog).openExternalPartition(identifier, 0, streamProperties(tp, initialConfig));
        order.verify(catalog).setStreamProperties(identifier, streamProperties(tp, initialConfig));
        order.verify(catalog).setStreamProperties(identifier, streamProperties(tp, latestConfig));
    }

    @Test
    void testOpenClosesLogAndPreservesCloseFailureWhenConfigReplacementFails() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log log = mock(Log.class);
        TopicIdPartition tp = topicIdPartition("failed-config-topic");
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);
        RuntimeException configError = new RuntimeException("catalog unavailable");
        RuntimeException closeError = new RuntimeException("close failed");

        when(catalog.openExternalPartition(identifier, 0, streamProperties(tp, Map.of())))
                .thenReturn(CompletableFuture.completedFuture(log));
        when(catalog.streamExists(identifier)).thenReturn(CompletableFuture.failedFuture(configError));
        doThrow(closeError).when(log).close();

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null);
        ExecutionException failure = assertThrows(
                ExecutionException.class,
                () -> holder.openPartition(tp, Map.of()).get());

        assertSame(configError, failure.getCause());
        assertEquals(List.of(closeError), List.of(configError.getSuppressed()));
        verify(log).close();
    }

    @Test
    void testPartitionDeletionWaitsForOpenAndFencesLateOpen() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log log = mock(Log.class);
        TopicIdPartition tp = topicIdPartition("delete-during-open-topic");
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);
        CompletableFuture<Log> opening = new CompletableFuture<>();

        when(catalog.openExternalPartition(identifier, 0, streamProperties(tp, Map.of()))).thenReturn(opening);
        when(catalog.streamExists(identifier)).thenReturn(CompletableFuture.completedFuture(false));
        when(catalog.deleteExternalPartition(identifier, 0))
                .thenReturn(CompletableFuture.completedFuture(null));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null);
        CompletableFuture<Log> openFuture = holder.openPartition(tp, Map.of());
        CompletableFuture<Void> deleteFuture = holder.deletePartitionData(tp);

        assertFalse(openFuture.isDone());
        assertFalse(deleteFuture.isDone());
        verify(catalog, never()).deleteExternalPartition(identifier, 0);

        opening.complete(log);
        assertSame(log, openFuture.get());
        deleteFuture.get();

        CompletableFuture<Log> lateOpen = holder.openPartition(tp, Map.of());
        assertThrows(ExecutionException.class, lateOpen::get);
        InOrder order = inOrder(catalog);
        order.verify(catalog).openExternalPartition(identifier, 0, streamProperties(tp, Map.of()));
        order.verify(catalog).deleteExternalPartition(identifier, 0);
    }

    @Test
    void testPartitionDeletionDelegatesWithoutUsingProducerStateOxiaClient() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        AsyncOxiaClient producerStateOxiaClient = mock(AsyncOxiaClient.class);
        TopicIdPartition tp = topicIdPartition("delete-topic");
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);
        when(catalog.deleteExternalPartition(identifier, 0))
                .thenReturn(CompletableFuture.completedFuture(null));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, producerStateOxiaClient);
        holder.deletePartitionData(tp).get();

        verify(catalog).deleteExternalPartition(identifier, 0);
        verifyNoInteractions(producerStateOxiaClient);
    }

    @Test
    void testFailedDeletionCannotAttachSameNameRecreationToOldStream() throws Exception {
        TopicPartition partition = new TopicPartition("failed-deletion-recreation", 0);
        TopicIdPartition deletedTp = new TopicIdPartition(Uuid.randomUuid(), partition);
        TopicIdPartition recreatedTp = new TopicIdPartition(Uuid.randomUuid(), partition);
        StreamIdentifier deletedIdentifier = LakestreamStorageHolder.streamIdentifier(deletedTp);
        StreamIdentifier recreatedIdentifier = LakestreamStorageHolder.streamIdentifier(recreatedTp);
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log deletedLog = mock(Log.class);
        Log recreatedLog = mock(Log.class);
        RuntimeException deleteError = new RuntimeException("delete failed");

        when(catalog.openExternalPartition(deletedIdentifier, 0, streamProperties(deletedTp, Map.of())))
                .thenReturn(CompletableFuture.completedFuture(deletedLog));
        when(catalog.deleteExternalPartition(deletedIdentifier, 0))
                .thenReturn(CompletableFuture.failedFuture(deleteError));
        when(catalog.openExternalPartition(recreatedIdentifier, 0, streamProperties(recreatedTp, Map.of())))
                .thenReturn(CompletableFuture.completedFuture(recreatedLog));
        when(catalog.streamExists(deletedIdentifier)).thenReturn(CompletableFuture.completedFuture(false));
        when(catalog.streamExists(recreatedIdentifier)).thenReturn(CompletableFuture.completedFuture(false));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null);
        assertSame(deletedLog, holder.openPartition(deletedTp, Map.of()).get());
        ExecutionException failure = assertThrows(
                ExecutionException.class,
                () -> holder.deletePartitionData(deletedTp).get());
        assertSame(deleteError, failure.getCause());

        assertSame(recreatedLog, holder.openPartition(recreatedTp, Map.of()).get());
        assertNotEquals(deletedIdentifier, recreatedIdentifier);
        verify(catalog).openExternalPartition(deletedIdentifier, 0, streamProperties(deletedTp, Map.of()));
        verify(catalog).openExternalPartition(recreatedIdentifier, 0, streamProperties(recreatedTp, Map.of()));
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

    private static void verifyAzureBackendAlias(String backendType) throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_STORAGE_BACKEND_TYPE_CONFIG, backendType,
                ServerLogConfigs.URSA_STORAGE_PATH_CONFIG, "azure-prefix",
                ServerLogConfigs.URSA_STORAGE_S3_ENDPOINT_CONFIG, "https://account.blob.core.windows.net",
                ServerLogConfigs.URSA_STORAGE_S3_BUCKET_CONFIG, "account@container",
                ServerLogConfigs.URSA_STORAGE_S3_REGION_CONFIG, "unused-region"
        ));

        Properties properties = LakestreamStorageHolder.buildStorageProperties(config);

        assertEquals("AZUREBLOB", properties.getProperty("backendStorageType"));
        assertEquals("account@container", properties.getProperty("bucket"));
        assertEquals("azure-prefix", properties.getProperty("prefix"));
        assertEquals("unused-region", properties.getProperty("region"));
        assertFalse(properties.containsKey("compactionBackendStorageType"));
        assertFalse(properties.containsKey("compactionBucketRegion"));
    }

    private static TopicIdPartition topicIdPartition(String topic) {
        return new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(topic, 0));
    }

    private static Map<String, String> streamProperties(
            TopicIdPartition topicIdPartition,
            Map<String, String> topicConfig) {
        return KafkaLogNaming.streamProperties(topicIdPartition.topic(), topicConfig);
    }
}
