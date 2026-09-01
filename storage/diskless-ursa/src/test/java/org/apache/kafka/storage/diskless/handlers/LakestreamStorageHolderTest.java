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
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import io.lakestream.api.Log;
import io.lakestream.api.LogId;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.StreamLayout;
import io.lakestream.api.StreamMetadata;
import io.lakestream.api.exception.NoSuchStreamException;
import io.oxia.client.api.AsyncOxiaClient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
    void testOpensPartitionFromCommittedStreamLayout() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        TopicIdPartition tp = new TopicIdPartition(
                Uuid.randomUuid(), new TopicPartition("layout-topic", 1));
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);
        LogId firstLogId = LogId.of(41);
        LogId secondLogId = LogId.of(42);
        Log log = mock(Log.class);
        stubLayout(catalog, identifier, List.of(firstLogId, secondLogId));
        when(catalog.openLog(identifier, secondLogId))
                .thenReturn(CompletableFuture.completedFuture(log));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null);

        Log openedLog = holder.openPartition(tp).get();
        assertNotSame(log, openedLog);
        verify(catalog).loadStream(identifier);
        verify(catalog).openLog(identifier, secondLogId);
        openedLog.close();
        verify(log).close();
    }

    @Test
    void testMissingPartitionInLayoutFailsWithoutOpeningLog() {
        StreamCatalog catalog = mock(StreamCatalog.class);
        TopicIdPartition tp = new TopicIdPartition(
                Uuid.randomUuid(), new TopicPartition("short-layout", 1));
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);
        stubLayout(catalog, identifier, List.of(LogId.of(41)));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null);

        ExecutionException failure = assertThrows(
                ExecutionException.class,
                () -> holder.openPartition(tp).get());
        assertEquals(IllegalStateException.class, failure.getCause().getClass());
        verify(catalog, never()).openLog(identifier, LogId.of(41));
    }

    @Test
    void testBrokerDeletionFencesAccessWhileAnOpenedHandleDrains() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        TopicIdPartition tp = topicIdPartition("deleted-topic");
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);
        LogId logId = LogId.of(54);
        Log log = mock(Log.class);
        stubLayoutThenDeleted(catalog, identifier, List.of(logId));
        when(catalog.openLog(identifier, logId)).thenReturn(CompletableFuture.completedFuture(log));
        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null);

        Log openedLog = holder.openPartition(tp).get();
        holder.markTopicDeleted(tp);

        assertEquals(1, holder.deletionFenceCount());
        assertThrows(ExecutionException.class, () -> holder.openPartition(tp).get());
        verify(catalog, times(1)).loadStream(identifier);
        openedLog.close();
        assertEquals(0, holder.deletionFenceCount());
    }

    @Test
    void testDeleteWhileOpeningClosesLateLog() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        TopicIdPartition tp = topicIdPartition("racing-delete");
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);
        LogId logId = LogId.of(55);
        Log log = mock(Log.class);
        CompletableFuture<Log> opening = new CompletableFuture<>();
        stubLayoutThenDeleted(catalog, identifier, List.of(logId));
        when(catalog.openLog(identifier, logId)).thenReturn(opening);
        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null);

        CompletableFuture<Log> result = holder.openPartition(tp);
        holder.markTopicDeleted(tp);
        opening.complete(log);

        assertThrows(ExecutionException.class, result::get);
        verify(log).close();
        assertEquals(0, holder.deletionFenceCount());
    }

    @Test
    void testDeletionFenceWaitsForSuccessfulLogClose() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        TopicIdPartition tp = topicIdPartition("close-retry");
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);
        LogId logId = LogId.of(56);
        Log log = mock(Log.class);
        stubLayoutThenDeleted(catalog, identifier, List.of(logId));
        when(catalog.openLog(identifier, logId)).thenReturn(CompletableFuture.completedFuture(log));
        doThrow(new IOException("close failed")).doNothing().when(log).close();
        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null);

        Log openedLog = holder.openPartition(tp).get();
        holder.markTopicDeleted(tp);

        assertThrows(IOException.class, openedLog::close);
        assertEquals(1, holder.deletionFenceCount());

        openedLog.close();
        assertEquals(0, holder.deletionFenceCount());
        verify(log, times(2)).close();
    }

    @Test
    void testDeletedTopicChurnDoesNotRetainDrainedFences() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null);

        for (int index = 0; index < 250; index++) {
            TopicIdPartition tp = topicIdPartition("churn-topic-" + index);
            StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);
            LogId logId = LogId.of(1_000L + index);
            Log log = mock(Log.class);
            stubLayoutThenDeleted(catalog, identifier, List.of(logId));
            when(catalog.openLog(identifier, logId))
                    .thenReturn(CompletableFuture.completedFuture(log));

            Log openedLog = holder.openPartition(tp).get();
            holder.markTopicDeleted(tp);
            openedLog.close();
        }

        assertEquals(0, holder.deletionFenceCount());
    }

    @Test
    void testDeletionWithoutLocalStateFencesLateAdmissionUntilCatalogDropIsVisible() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        TopicIdPartition tp = topicIdPartition("late-admission");
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);
        CompletableFuture<StreamMetadata> deletionCheck = new CompletableFuture<>();
        when(catalog.loadStream(identifier)).thenReturn(deletionCheck);
        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null, 1, 5_000);

        holder.markTopicDeleted(tp);

        assertEquals(1, holder.deletionFenceCount());
        ExecutionException failure = assertThrows(ExecutionException.class, () -> holder.openPartition(tp).get());
        assertEquals(IllegalStateException.class, failure.getCause().getClass());
        verify(catalog, times(1)).loadStream(identifier);

        deletionCheck.completeExceptionally(new NoSuchStreamException(identifier));
        assertEquals(0, holder.deletionFenceCount());
        holder.close();
    }

    @Test
    void testTransientDeletionCheckFailureIsRetriedBeforeFenceRelease() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        TopicIdPartition tp = topicIdPartition("deletion-check-retry");
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);
        CompletableFuture<StreamMetadata> retryCheck = new CompletableFuture<>();
        when(catalog.loadStream(identifier))
                .thenReturn(CompletableFuture.failedFuture(new IOException("catalog unavailable")))
                .thenReturn(retryCheck);
        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null, 1, 5_000);

        holder.markTopicDeleted(tp);

        verify(catalog, timeout(5_000).times(2)).loadStream(identifier);
        assertEquals(1, holder.deletionFenceCount());
        retryCheck.completeExceptionally(new NoSuchStreamException(identifier));
        assertEquals(0, holder.deletionFenceCount());
        holder.close();
    }

    @Test
    void testCloseCancelsInFlightDeletionCheckBeforeCatalogClose() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        TopicIdPartition tp = topicIdPartition("close-deletion-check");
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);
        CompletableFuture<StreamMetadata> deletionCheck = new CompletableFuture<>();
        when(catalog.loadStream(identifier)).thenReturn(deletionCheck);
        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null, 1, 5_000);

        holder.markTopicDeleted(tp);
        holder.close();

        assertTrue(deletionCheck.isCancelled());
        verify(catalog).close();
    }

    @Test
    void testCloseWaitsForInFlightOpenAndClosesLateLogBeforeCatalogClose() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        AsyncOxiaClient producerStateOxiaClient = mock(AsyncOxiaClient.class);
        TopicIdPartition tp = topicIdPartition("close-in-flight-open");
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);
        LogId logId = LogId.of(73);
        Log log = mock(Log.class);
        CompletableFuture<Log> opening = new CompletableFuture<>();
        stubLayout(catalog, identifier, List.of(logId));
        when(catalog.openLog(identifier, logId)).thenReturn(opening);
        LakestreamStorageHolder holder = new LakestreamStorageHolder(
                catalog, producerStateOxiaClient, 1, 5_000);

        CompletableFuture<Log> openResult = holder.openPartition(tp);
        CompletableFuture<Void> closeResult = CompletableFuture.runAsync(() -> {
            try {
                holder.close();
            } catch (IOException error) {
                throw new CompletionException(error);
            }
        });
        TestUtils.waitForCondition(holder::isClosing, 5_000, "Expected holder close to start");

        assertFalse(closeResult.isDone());
        verify(catalog, never()).close();
        opening.complete(log);

        assertThrows(ExecutionException.class, openResult::get);
        closeResult.get();
        InOrder closeOrder = inOrder(log, catalog, producerStateOxiaClient);
        closeOrder.verify(log).close();
        closeOrder.verify(catalog).close();
        closeOrder.verify(producerStateOxiaClient).close();
    }

    @Test
    void testCloseTimeoutRetainsFailedHandleAndCanBeRetried() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        AsyncOxiaClient producerStateOxiaClient = mock(AsyncOxiaClient.class);
        TopicIdPartition tp = topicIdPartition("retry-close-after-timeout");
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);
        LogId logId = LogId.of(74);
        Log log = mock(Log.class);
        AtomicBoolean allowClose = new AtomicBoolean();
        stubLayout(catalog, identifier, List.of(logId));
        when(catalog.openLog(identifier, logId)).thenReturn(CompletableFuture.completedFuture(log));
        doAnswer(ignored -> {
            if (!allowClose.get()) {
                throw new IOException("lease release unavailable");
            }
            return null;
        }).when(log).close();
        LakestreamStorageHolder holder = new LakestreamStorageHolder(
                catalog, producerStateOxiaClient, 1, 25);
        holder.openPartition(tp).get();

        IOException failure = assertThrows(IOException.class, holder::close);

        assertTrue(failure.getMessage().contains("draining Lakestream lifecycle operations"));
        assertEquals(1, holder.deletionFenceCount());
        verify(catalog, never()).close();
        verify(producerStateOxiaClient, never()).close();

        allowClose.set(true);
        holder.close();

        verify(log, atLeast(2)).close();
        verify(catalog).close();
        verify(producerStateOxiaClient).close();
        assertEquals(0, holder.deletionFenceCount());
    }

    @Test
    void testOpenCompletingAfterCloseTimeoutIsRejectedClosedAndRetryable() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        TopicIdPartition tp = topicIdPartition("late-open-after-close-timeout");
        StreamIdentifier identifier = LakestreamStorageHolder.streamIdentifier(tp);
        LogId logId = LogId.of(75);
        Log log = mock(Log.class);
        CompletableFuture<Log> opening = new CompletableFuture<>();
        stubLayout(catalog, identifier, List.of(logId));
        when(catalog.openLog(identifier, logId)).thenReturn(opening);
        doThrow(new IOException("late close failed")).doNothing().when(log).close();
        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null, 1, 25);

        CompletableFuture<Log> openResult = holder.openPartition(tp);
        assertThrows(IOException.class, holder::close);
        verify(catalog, never()).close();

        opening.complete(log);

        assertThrows(ExecutionException.class, openResult::get);
        verify(log, timeout(5_000).times(2)).close();
        assertEquals(1, holder.deletionFenceCount());
        holder.close();
        verify(catalog).close();
    }

    @Test
    void testSameNameRecreationUsesIndependentStreamLayout() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        TopicPartition partition = new TopicPartition("recreated-topic", 0);
        TopicIdPartition deletedTp = new TopicIdPartition(Uuid.randomUuid(), partition);
        TopicIdPartition recreatedTp = new TopicIdPartition(Uuid.randomUuid(), partition);
        StreamIdentifier deletedIdentifier = LakestreamStorageHolder.streamIdentifier(deletedTp);
        StreamIdentifier recreatedIdentifier = LakestreamStorageHolder.streamIdentifier(recreatedTp);
        LogId recreatedLogId = LogId.of(72);
        Log recreatedLog = mock(Log.class);
        when(catalog.loadStream(deletedIdentifier)).thenReturn(
                CompletableFuture.failedFuture(new NoSuchStreamException(deletedIdentifier)));
        stubLayout(catalog, recreatedIdentifier, List.of(recreatedLogId));
        when(catalog.openLog(recreatedIdentifier, recreatedLogId))
                .thenReturn(CompletableFuture.completedFuture(recreatedLog));
        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null);

        holder.markTopicDeleted(deletedTp);

        Log openedLog = holder.openPartition(recreatedTp).get();
        assertNotSame(recreatedLog, openedLog);
        assertNotEquals(deletedIdentifier, recreatedIdentifier);
        openedLog.close();
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

    private static void stubLayout(
            StreamCatalog catalog,
            StreamIdentifier identifier,
            List<LogId> logIds) {
        StreamMetadata metadata = mock(StreamMetadata.class);
        StreamLayout layout = mock(StreamLayout.class);
        when(catalog.loadStream(identifier)).thenReturn(CompletableFuture.completedFuture(metadata));
        when(metadata.layout()).thenReturn(layout);
        when(layout.logIds()).thenReturn(CompletableFuture.completedFuture(logIds));
    }

    private static void stubLayoutThenDeleted(
            StreamCatalog catalog,
            StreamIdentifier identifier,
            List<LogId> logIds) {
        StreamMetadata metadata = mock(StreamMetadata.class);
        StreamLayout layout = mock(StreamLayout.class);
        when(catalog.loadStream(identifier))
                .thenReturn(CompletableFuture.completedFuture(metadata))
                .thenReturn(CompletableFuture.failedFuture(new NoSuchStreamException(identifier)));
        when(metadata.layout()).thenReturn(layout);
        when(layout.logIds()).thenReturn(CompletableFuture.completedFuture(logIds));
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
}
