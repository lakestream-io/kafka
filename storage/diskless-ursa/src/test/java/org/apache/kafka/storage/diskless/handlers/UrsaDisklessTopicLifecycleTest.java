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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.storage.diskless.DisklessTopicLifecycle;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import io.lakestream.api.LifecycleState;
import io.lakestream.api.Partitioning;
import io.lakestream.api.PartitioningStrategy;
import io.lakestream.api.SchemaConfig;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamCatalogEntry;
import io.lakestream.api.StreamConfig;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.StreamMetadata;
import io.lakestream.api.exception.AlreadyExistsException;
import io.lakestream.api.exception.NoSuchStreamException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UrsaDisklessTopicLifecycleTest {

    @Test
    void testListsOnlyStreamsWithVerifiedKafkaOwnershipMetadata() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        Uuid topicId = Uuid.fromString("65WMNfybQpCDVulYOxMCTw");
        StreamIdentifier owned = UrsaDisklessTopicLifecycle.streamIdentifier("orders", topicId);
        StreamIdentifier missingManagedMarker = StreamIdentifier.of("default", "missing-managed-marker");
        StreamIdentifier missingTopicId = StreamIdentifier.of("default", "missing-topic-id");
        StreamIdentifier malformedTopicId = StreamIdentifier.of("default", "malformed-topic-id");
        StreamIdentifier mismatchedIdentifier = StreamIdentifier.of("default", "unrelated-stream");
        Uuid missingRevisionTopicId = Uuid.randomUuid();
        StreamIdentifier missingRevision = UrsaDisklessTopicLifecycle.streamIdentifier(
                "missing-revision", missingRevisionTopicId);
        Uuid malformedRevisionTopicId = Uuid.randomUuid();
        StreamIdentifier malformedRevision = UrsaDisklessTopicLifecycle.streamIdentifier(
                "malformed-revision", malformedRevisionTopicId);
        Uuid negativeRevisionTopicId = Uuid.randomUuid();
        StreamIdentifier negativeRevision = UrsaDisklessTopicLifecycle.streamIdentifier(
                "negative-revision", negativeRevisionTopicId);
        when(catalog.listStreamEntries(KafkaLogNaming.NAMESPACE)).thenReturn(
                CompletableFuture.completedFuture(List.of(
                        entry(
                                owned,
                                Map.of(
                                        KafkaLogNaming.KAFKA_MANAGED_PROPERTY, "true",
                                        KafkaLogNaming.KAFKA_TOPIC_NAME_PROPERTY, "orders",
                                        KafkaLogNaming.KAFKA_TOPIC_ID_PROPERTY, topicId.toString(),
                                        KafkaLogNaming.KAFKA_SOURCE_REVISION_PROPERTY, "42"),
                                LifecycleState.CREATING),
                        entry(
                                missingManagedMarker,
                                Map.of(
                                        KafkaLogNaming.KAFKA_TOPIC_NAME_PROPERTY, "orders",
                                        KafkaLogNaming.KAFKA_TOPIC_ID_PROPERTY, topicId.toString(),
                                        KafkaLogNaming.KAFKA_SOURCE_REVISION_PROPERTY, "42"),
                                LifecycleState.ACTIVE),
                        entry(
                                missingTopicId,
                                Map.of(
                                        KafkaLogNaming.KAFKA_MANAGED_PROPERTY, "true",
                                        KafkaLogNaming.KAFKA_TOPIC_NAME_PROPERTY, "orders",
                                        KafkaLogNaming.KAFKA_SOURCE_REVISION_PROPERTY, "42"),
                                LifecycleState.DELETING),
                        entry(
                                malformedTopicId,
                                Map.of(
                                        KafkaLogNaming.KAFKA_MANAGED_PROPERTY, "true",
                                        KafkaLogNaming.KAFKA_TOPIC_NAME_PROPERTY, "orders",
                                        KafkaLogNaming.KAFKA_TOPIC_ID_PROPERTY, "not-a-topic-id",
                                        KafkaLogNaming.KAFKA_SOURCE_REVISION_PROPERTY, "42"),
                                LifecycleState.ACTIVE),
                        entry(
                                mismatchedIdentifier,
                                Map.of(
                                        KafkaLogNaming.KAFKA_MANAGED_PROPERTY, "true",
                                        KafkaLogNaming.KAFKA_TOPIC_NAME_PROPERTY, "orders",
                                        KafkaLogNaming.KAFKA_TOPIC_ID_PROPERTY, topicId.toString(),
                                        KafkaLogNaming.KAFKA_SOURCE_REVISION_PROPERTY, "42"),
                                LifecycleState.ACTIVE),
                        entry(
                                missingRevision,
                                Map.of(
                                        KafkaLogNaming.KAFKA_MANAGED_PROPERTY, "true",
                                        KafkaLogNaming.KAFKA_TOPIC_NAME_PROPERTY, "missing-revision",
                                        KafkaLogNaming.KAFKA_TOPIC_ID_PROPERTY, missingRevisionTopicId.toString()),
                                LifecycleState.ACTIVE),
                        entry(
                                malformedRevision,
                                Map.of(
                                        KafkaLogNaming.KAFKA_MANAGED_PROPERTY, "true",
                                        KafkaLogNaming.KAFKA_TOPIC_NAME_PROPERTY, "malformed-revision",
                                        KafkaLogNaming.KAFKA_TOPIC_ID_PROPERTY, malformedRevisionTopicId.toString(),
                                        KafkaLogNaming.KAFKA_SOURCE_REVISION_PROPERTY, "not-a-revision"),
                                LifecycleState.ACTIVE),
                        entry(
                                negativeRevision,
                                Map.of(
                                        KafkaLogNaming.KAFKA_MANAGED_PROPERTY, "true",
                                        KafkaLogNaming.KAFKA_TOPIC_NAME_PROPERTY, "negative-revision",
                                        KafkaLogNaming.KAFKA_TOPIC_ID_PROPERTY, negativeRevisionTopicId.toString(),
                                        KafkaLogNaming.KAFKA_SOURCE_REVISION_PROPERTY, "-1"),
                                LifecycleState.ACTIVE))));

        try (UrsaDisklessTopicLifecycle lifecycle = new UrsaDisklessTopicLifecycle(catalog)) {
            assertEquals(
                    List.of(new DisklessTopicLifecycle.ManagedTopic("orders", topicId, 42L)),
                    lifecycle.listManagedTopics().get());
        }
    }

    @Test
    void testListManagedTopicsIncludesKafkaOwnedDeletingEntry() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        Uuid topicId = Uuid.randomUuid();
        StreamIdentifier identifier = UrsaDisklessTopicLifecycle.streamIdentifier(
                "deleting-topic", topicId);
        when(catalog.listStreamEntries(KafkaLogNaming.NAMESPACE))
                .thenReturn(CompletableFuture.completedFuture(List.of(entry(
                        identifier,
                        Map.of(
                                KafkaLogNaming.KAFKA_MANAGED_PROPERTY, "true",
                                KafkaLogNaming.KAFKA_TOPIC_NAME_PROPERTY, "deleting-topic",
                                KafkaLogNaming.KAFKA_TOPIC_ID_PROPERTY, topicId.toString(),
                                KafkaLogNaming.KAFKA_SOURCE_REVISION_PROPERTY, "43"),
                        LifecycleState.DELETING))));

        try (UrsaDisklessTopicLifecycle lifecycle = new UrsaDisklessTopicLifecycle(catalog)) {
            assertEquals(
                    List.of(new DisklessTopicLifecycle.ManagedTopic("deleting-topic", topicId, 43L)),
                    lifecycle.listManagedTopics().get());
        }
    }

    @Test
    void testCreatesMissingStreamAndRecordsKafkaRevision() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        Uuid topicId = Uuid.fromString("65WMNfybQpCDVulYOxMCTw");
        StreamIdentifier identifier = UrsaDisklessTopicLifecycle.streamIdentifier("orders", topicId);
        Map<String, String> properties = Map.of("retention.ms", "60000");
        Map<String, String> streamProperties = KafkaLogNaming.streamProperties(
                "orders", topicId, properties, 17L);
        StreamMetadata created = metadataWithPartitions(3);
        when(catalog.loadStream(identifier))
                .thenReturn(CompletableFuture.failedFuture(new NoSuchStreamException(identifier)));
        when(catalog.createStream(
                eq(identifier),
                any(StreamConfig.class),
                eq(indexedPartitions(3)),
                any(SchemaConfig.class),
                eq(streamProperties)))
                .thenReturn(CompletableFuture.completedFuture(created));
        when(catalog.replaceStreamProperties(identifier, streamProperties, 17))
                .thenReturn(CompletableFuture.completedFuture(created));

        try (UrsaDisklessTopicLifecycle lifecycle = new UrsaDisklessTopicLifecycle(catalog)) {
            lifecycle.registerTopic("orders", topicId, 3, properties, 17).get();
        }

        verify(catalog).createStream(
                eq(identifier),
                any(StreamConfig.class),
                eq(indexedPartitions(3)),
                any(SchemaConfig.class),
                eq(streamProperties));
        verify(catalog, never()).increasePartitions(identifier, 3);
        verify(catalog).replaceStreamProperties(identifier, streamProperties, 17);
        verify(catalog).close();
    }

    @Test
    void testExistingStreamIsGrownBeforePropertiesAreReplaced() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        Uuid topicId = Uuid.randomUuid();
        StreamIdentifier identifier = UrsaDisklessTopicLifecycle.streamIdentifier("orders", topicId);
        Map<String, String> streamProperties = KafkaLogNaming.streamProperties(
                "orders", topicId, Map.of(), 18L);
        StreamMetadata existing = metadataWithPartitions(1);
        StreamMetadata expanded = metadataWithPartitions(3);
        when(catalog.loadStream(identifier)).thenReturn(CompletableFuture.completedFuture(existing));
        when(catalog.increasePartitions(identifier, 3))
                .thenReturn(CompletableFuture.completedFuture(expanded));
        when(catalog.replaceStreamProperties(identifier, streamProperties, 18))
                .thenReturn(CompletableFuture.completedFuture(expanded));

        try (UrsaDisklessTopicLifecycle lifecycle = new UrsaDisklessTopicLifecycle(catalog)) {
            lifecycle.registerTopic("orders", topicId, 3, Map.of(), 18).get();
        }

        verify(catalog).increasePartitions(identifier, 3);
        verify(catalog).replaceStreamProperties(identifier, streamProperties, 18);
        verify(catalog, never()).createStream(
                any(), any(), any(), any(), any());
    }

    @Test
    void testConcurrentCreateLoadsWinnerAndReconcilesIt() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        Uuid topicId = Uuid.randomUuid();
        StreamIdentifier identifier = UrsaDisklessTopicLifecycle.streamIdentifier("orders", topicId);
        Map<String, String> streamProperties = KafkaLogNaming.streamProperties(
                "orders", topicId, Map.of(), 19L);
        StreamMetadata winner = metadataWithPartitions(3);
        when(catalog.loadStream(identifier))
                .thenReturn(CompletableFuture.failedFuture(new NoSuchStreamException(identifier)))
                .thenReturn(CompletableFuture.completedFuture(winner));
        when(catalog.createStream(
                eq(identifier), any(), eq(indexedPartitions(3)), any(), eq(streamProperties)))
                .thenReturn(CompletableFuture.failedFuture(new AlreadyExistsException("racing create")));
        when(catalog.replaceStreamProperties(identifier, streamProperties, 19))
                .thenReturn(CompletableFuture.completedFuture(winner));

        try (UrsaDisklessTopicLifecycle lifecycle = new UrsaDisklessTopicLifecycle(catalog)) {
            lifecycle.registerTopic("orders", topicId, 3, Map.of(), 19).get();
        }

        verify(catalog).replaceStreamProperties(identifier, streamProperties, 19);
    }

    @Test
    void testUnregisterPermanentlyDropsStream() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        Uuid topicId = Uuid.randomUuid();
        StreamIdentifier identifier = UrsaDisklessTopicLifecycle.streamIdentifier("orders", topicId);
        when(catalog.dropStream(identifier, true))
                .thenReturn(CompletableFuture.completedFuture(true));

        try (UrsaDisklessTopicLifecycle lifecycle = new UrsaDisklessTopicLifecycle(catalog)) {
            lifecycle.unregisterTopic("orders", topicId).get();
        }

        verify(catalog).dropStream(identifier, true);
    }

    @Test
    void testSameNameTopicIncarnationsUseDifferentStreamIdentifiers() {
        StreamIdentifier first = UrsaDisklessTopicLifecycle.streamIdentifier(
                "orders", Uuid.fromString("65WMNfybQpCDVulYOxMCTw"));
        StreamIdentifier second = UrsaDisklessTopicLifecycle.streamIdentifier(
                "orders", Uuid.fromString("VkZ5AkuESPGkMc2OxpKUjw"));

        assertNotEquals(first, second);
    }

    private static StreamMetadata metadataWithPartitions(int partitions) {
        StreamMetadata metadata = mock(StreamMetadata.class);
        when(metadata.partitioning()).thenReturn(indexedPartitions(partitions));
        return metadata;
    }

    private static StreamCatalogEntry entry(
            StreamIdentifier identifier,
            Map<String, String> properties,
            LifecycleState state) {
        return new StreamCatalogEntry(identifier, state, properties, 1L);
    }

    private static Partitioning indexedPartitions(int partitions) {
        return new Partitioning(
                PartitioningStrategy.INDEXED,
                Map.of("numPartitions", String.valueOf(partitions)));
    }
}
