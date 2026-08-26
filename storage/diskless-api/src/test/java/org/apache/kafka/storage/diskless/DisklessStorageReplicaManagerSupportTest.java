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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.replica.ClientMetadata.DefaultClientMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DisklessStorageReplicaManagerSupportTest {

    @Test
    void testUpdateTopicConfigForwardsStringMapToEngine() throws Exception {
        DisklessStorageEngine engine = mock(DisklessStorageEngine.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        TopicIdPartition topicIdentity = topicIdPartition("diskless-topic", 0);
        when(metadataView.getTopicId(topicIdentity.topic())).thenReturn(topicIdentity.topicId());
        Properties config = new Properties();
        config.setProperty("retention.ms", "1000");
        config.setProperty("retention.bytes", "2048");

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, 1, selector, engine)) {
            support.updateTopicConfig(topicIdentity.topic(), config);

            verify(engine).updateTopicConfig(topicIdentity, Map.of(
                    "retention.ms", "1000",
                    "retention.bytes", "2048"));
        }
    }

    @Test
    void testDeleteTopicConfigForwardsTopicToEngine() throws Exception {
        DisklessStorageEngine engine = mock(DisklessStorageEngine.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        TopicIdPartition topicIdentity = topicIdPartition("diskless-topic", 0);

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, 1, selector, engine)) {
            support.deleteTopicConfig(topicIdentity);

            verify(engine).deleteTopicConfig(topicIdentity);
        }
    }

    @Test
    void testHandleAppendRedirectsNonOwnerWithoutCallingWriter() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-append-redirect-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        stubNoZoneSelection(selector, tp, localBrokerId + 1);

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            Map<TopicIdPartition, org.apache.kafka.common.requests.ProduceResponse.PartitionResponse> responses =
                    support.handleAppend(Map.of(tp, MemoryRecords.EMPTY), null).join();

            assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, responses.get(tp).error);
            verify(writer, never()).write(any(), anyString());
            assertTrue(support.snapshotTrackedPartitions().isEmpty());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testHandleFetchRedirectsNonOwnerWithoutCallingReader() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-fetch-redirect-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        stubNoZoneSelection(selector, tp, localBrokerId + 1);

        FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(
                tp.topicId(),
                0L,
                0L,
                1024,
                Optional.empty()
        );

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            Map<TopicIdPartition, FetchPartitionData> responses =
                    support.handleFetch(mock(FetchParams.class), Map.of(tp, partitionData), null).join();

            FetchPartitionData response = responses.get(tp);
            assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, response.error);
            assertEquals(UnifiedLog.UNKNOWN_OFFSET, response.highWatermark);
            verify(reader, never()).fetch(any(), any());
            assertTrue(support.snapshotTrackedPartitions().isEmpty());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testHandleListOffsetsRedirectsNonOwnerWithoutCallingReader() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-list-offsets-redirect-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        stubNoZoneSelection(selector, tp, localBrokerId + 1);

        ListOffsetsPartitionRequest request = new ListOffsetsPartitionRequest(tp, ListOffsetsRequest.LATEST_TIMESTAMP, Optional.empty());

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            Map<TopicIdPartition, ListOffsetsPartitionResponse> responses = support.handleListOffsets(Map.of(tp, request), null).join();

            assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, responses.get(tp).error());
            verify(reader, never()).listOffsets(any());
            assertTrue(support.snapshotTrackedPartitions().isEmpty());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testStaleTopicIncarnationIsRejectedBeforeAllEngineRoutes() {
        int localBrokerId = 1;
        TopicIdPartition staleTp = topicIdPartition("recreated-diskless-topic", 0);
        Uuid currentTopicId = Uuid.randomUuid();
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        when(metadataView.getTopicId(staleTp.topic())).thenReturn(currentTopicId);
        when(metadataView.isDisklessStorageTopic(staleTp.topic())).thenReturn(true);
        stubNoZoneSelection(selector, staleTp, localBrokerId);

        FetchRequest.PartitionData fetchRequest = new FetchRequest.PartitionData(
                staleTp.topicId(), 0L, 0L, 1024, Optional.empty());
        ListOffsetsPartitionRequest offsetsRequest = new ListOffsetsPartitionRequest(
                staleTp, ListOffsetsRequest.LATEST_TIMESTAMP, Optional.empty());

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(
                             metadataView,
                             localBrokerId,
                             selector,
                             new TestDisklessStorageEngine(writer, reader, ursaState))) {
            Map<TopicIdPartition, ProduceResponse.PartitionResponse> appendResponses =
                    support.handleAppend(Map.of(staleTp, MemoryRecords.EMPTY), null).join();
            Map<TopicIdPartition, FetchPartitionData> fetchResponses = support.handleFetch(
                    mock(FetchParams.class), Map.of(staleTp, fetchRequest), null).join();
            Map<TopicIdPartition, ListOffsetsPartitionResponse> offsetsResponses =
                    support.handleListOffsets(Map.of(staleTp, offsetsRequest), null).join();

            assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, appendResponses.get(staleTp).error);
            assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, fetchResponses.get(staleTp).error);
            assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, offsetsResponses.get(staleTp).error());
            verify(selector, never()).selectBrokerForZone(any(), anyInt(), anyString());
            verify(writer, never()).write(any(), anyString());
            verify(reader, never()).fetch(any(), any());
            verify(reader, never()).listOffsets(any());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testHandleAppendUsesClientZoneAwareOwnership() {
        int localBrokerId = 0;
        TopicIdPartition tp = topicIdPartition("diskless-zone-aware-append-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(true);
        DisklessBrokerSelector selector = new DisklessBrokerSelector(
                ignored -> List.of(
                        new Node(0, "host0", 9092, "zone-a"),
                        new Node(1, "host1", 9092, "zone-b")
                ),
                new ListenerName("INTERNAL")
        );
        when(writer.write(any(), eq("zone-a"))).thenReturn(
                CompletableFuture.completedFuture(
                        Map.of(tp, new ProduceResponse.PartitionResponse(Errors.NONE))));

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            Map<TopicIdPartition, ProduceResponse.PartitionResponse> responses =
                    support.handleAppend(Map.of(tp, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("test".getBytes()))),
                            "producer,zone_id=zone-a").join();

            assertEquals(Errors.NONE, responses.get(tp).error);
            verify(writer).write(any(), eq("zone-a"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testHandleFetchUsesClientMetadataZoneAwareOwnership() {
        int localBrokerId = 0;
        TopicIdPartition tp = topicIdPartition("diskless-zone-aware-fetch-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(true);
        DisklessBrokerSelector selector = new DisklessBrokerSelector(
                ignored -> List.of(
                        new Node(0, "host0", 9092, "zone-a"),
                        new Node(1, "host1", 9092, "zone-b")
                ),
                new ListenerName("INTERNAL")
        );

        FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(
                tp.topicId(),
                0L,
                0L,
                1024,
                Optional.empty()
        );
        FetchPartitionData fetchResponse = new FetchPartitionData(
                Errors.NONE,
                10L,
                0L,
                MemoryRecords.EMPTY,
                Optional.empty(),
                java.util.OptionalLong.empty(),
                Optional.empty(),
                java.util.OptionalInt.empty(),
                false
        );
        FetchParams params = new FetchParams(
                -1,
                -1,
                500,
                1,
                1024,
                FetchIsolation.HIGH_WATERMARK,
                Optional.of(new DefaultClientMetadata(
                        "",
                        "consumer,zone_id=zone-a",
                        InetAddress.getLoopbackAddress(),
                        KafkaPrincipal.ANONYMOUS,
                        "PLAINTEXT"
                ))
        );
        when(reader.fetch(eq(params), any())).thenReturn(CompletableFuture.completedFuture(Map.of(tp, fetchResponse)));

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            Map<TopicIdPartition, FetchPartitionData> responses =
                    support.handleFetch(params, Map.of(tp, partitionData), "consumer,zone_id=zone-a").join();

            assertEquals(Errors.NONE, responses.get(tp).error);
            verify(reader).fetch(eq(params), any());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testHandleLegacyZeroTopicIdFetchUsesCurrentIncarnationAndRemapsResponse() {
        int localBrokerId = 1;
        String topic = "legacy-fetch-topic";
        TopicIdPartition canonicalTp = topicIdPartition(topic, 0);
        TopicIdPartition requestTp = new TopicIdPartition(
                Uuid.ZERO_UUID,
                canonicalTp.topicPartition());
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        stubNoZoneSelection(selector, canonicalTp, localBrokerId);
        when(metadataView.getTopicId(topic)).thenReturn(canonicalTp.topicId());
        when(metadataView.isDisklessStorageTopic(topic)).thenReturn(true);

        FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(
                Uuid.ZERO_UUID,
                0L,
                0L,
                1024,
                Optional.empty());
        FetchPartitionData fetchResponse = new FetchPartitionData(
                Errors.NONE,
                10L,
                0L,
                MemoryRecords.EMPTY,
                Optional.empty(),
                java.util.OptionalLong.empty(),
                Optional.empty(),
                java.util.OptionalInt.empty(),
                false);
        FetchParams params = mock(FetchParams.class);
        when(reader.fetch(eq(params), any())).thenAnswer(invocation -> {
            Map<TopicIdPartition, FetchRequest.PartitionData> engineFetches = invocation.getArgument(1);
            assertEquals(Set.of(canonicalTp), engineFetches.keySet());
            assertFalse(engineFetches.containsKey(requestTp));
            return CompletableFuture.completedFuture(Map.of(canonicalTp, fetchResponse));
        });

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(
                             metadataView,
                             localBrokerId,
                             selector,
                             new TestDisklessStorageEngine(writer, reader, ursaState))) {
            Map<TopicIdPartition, FetchPartitionData> responses = support.handleFetch(
                    params,
                    Map.of(requestTp, partitionData),
                    null).join();

            assertEquals(Set.of(requestTp), responses.keySet());
            assertEquals(Errors.NONE, responses.get(requestTp).error);
            verify(selector).selectBrokerForZone(
                    canonicalTp.topicId(),
                    canonicalTp.partition(),
                    DisklessClientZone.NO_ZONE);
            verify(reader).fetch(eq(params), any());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testHandleLegacyZeroTopicIdFetchDoesNotOpenUnknownIncarnation() {
        int localBrokerId = 1;
        TopicIdPartition requestTp = new TopicIdPartition(
                Uuid.ZERO_UUID,
                new TopicPartition("unknown-legacy-fetch-topic", 0));
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        when(selector.effectiveZone(any())).thenReturn(DisklessClientZone.NO_ZONE);
        when(metadataView.getTopicId(requestTp.topic())).thenReturn(Uuid.ZERO_UUID);
        FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(
                Uuid.ZERO_UUID,
                0L,
                0L,
                1024,
                Optional.empty());

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(
                             metadataView,
                             localBrokerId,
                             selector,
                             new TestDisklessStorageEngine(writer, reader, ursaState))) {
            Map<TopicIdPartition, FetchPartitionData> responses = support.handleFetch(
                    mock(FetchParams.class),
                    Map.of(requestTp, partitionData),
                    null).join();

            assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, responses.get(requestTp).error);
            verify(selector, never()).selectBrokerForZone(any(), anyInt(), anyString());
            verify(reader, never()).fetch(any(), any());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testReconcileCleansTrackedPartitionWhenTopicDeleted() {
        int localBrokerId = 1;
        String topic = "diskless-topic";
        TopicIdPartition tp = topicIdPartition(topic, 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        configureWriterState(writer, ursaState, true);
        AtomicReference<Uuid> currentTopicId = new AtomicReference<>(tp.topicId());
        AtomicBoolean diskless = new AtomicBoolean(true);
        when(metadataView.getTopicId(topic)).thenAnswer(ignored -> currentTopicId.get());
        when(metadataView.isDisklessStorageTopic(topic)).thenAnswer(ignored -> diskless.get());
        stubNoZoneSelection(selector, tp, localBrokerId);

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            support.handleAppend(Map.of(tp, MemoryRecords.EMPTY), null).join();

            assertEquals(Set.of(tp), support.snapshotTrackedPartitions());
            assertTrue(support.hasTrackedPartitionsForTopic(topic));
            assertEquals(Set.of(topic), support.trackedTopicNames());

            currentTopicId.set(Uuid.ZERO_UUID);
            diskless.set(false);
            AtomicInteger callbackCount = new AtomicInteger(0);
            support.reconcileTrackedPartitions(Set.of(tp), ignored -> callbackCount.incrementAndGet());

            verify(ursaState).cleanupPartition(tp, true);
            verify(ursaState).deletePartitionData(tp);
            assertEquals(1, callbackCount.get());
            assertTrue(support.snapshotTrackedPartitions().isEmpty());
            assertFalse(support.hasTrackedPartitionsForTopic(topic));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testReconcileDeletesPartitionDataOnlyOnOwnerBroker() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-delete-owned-by-other-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        configureWriterState(writer, ursaState, true);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(true);
        when(selector.effectiveZone(any())).thenReturn(DisklessClientZone.NO_ZONE);
        when(selector.selectBrokerForZone(tp.topicId(), tp.partition(), DisklessClientZone.NO_ZONE)).thenReturn(
                java.util.OptionalInt.of(localBrokerId),
                java.util.OptionalInt.of(localBrokerId + 1)
        );

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            support.handleAppend(Map.of(tp, MemoryRecords.EMPTY), null).join();

            support.reconcileTrackedPartitions(Set.of(tp), ignored -> { });

            verify(ursaState).cleanupPartition(tp, true);
            verify(ursaState, never()).deletePartitionData(tp);
            assertTrue(support.snapshotTrackedPartitions().isEmpty());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testReconcileCleansTrackedPartitionWhenBrokerOwnsNoActiveZone() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-owner-change-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        configureWriterState(writer, ursaState, true);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(true);
        stubNoZoneSelection(selector, tp, localBrokerId);

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            support.handleAppend(Map.of(tp, MemoryRecords.EMPTY), null).join();
            long initialGeneration = support.currentDisklessOwnershipGeneration(tp);
            assertTrue(initialGeneration > 0);
            clearInvocations(selector);
            when(selector.activeZones()).thenReturn(Set.of(DisklessClientZone.NO_ZONE));
            when(selector.selectBrokerForZone(tp.topicId(), tp.partition(), DisklessClientZone.NO_ZONE)).thenReturn(
                    java.util.OptionalInt.of(localBrokerId + 1));

            AtomicInteger ownershipLossCount = new AtomicInteger();
            AtomicLong lostGeneration = new AtomicLong();
            support.reconcileTrackedPartitions(Set.of(), ignored -> { }, (lostPartition, generation) -> {
                assertEquals(tp, lostPartition);
                verify(ursaState, never()).cleanupPartition(tp, false);
                ownershipLossCount.incrementAndGet();
                lostGeneration.set(generation);
            });

            verify(ursaState).cleanupPartition(tp, false);
            verify(ursaState, never()).deletePartitionData(tp);
            assertEquals(1, ownershipLossCount.get());
            assertTrue(lostGeneration.get() > initialGeneration);
            assertFalse(support.isCurrentDisklessOwnershipGeneration(tp, initialGeneration));
            assertEquals(0, support.ownershipGenerationCount());

            when(selector.selectBrokerForZone(tp.topicId(), tp.partition(), DisklessClientZone.NO_ZONE))
                    .thenReturn(java.util.OptionalInt.of(localBrokerId));
            long reacquiredGeneration = support.currentDisklessOwnershipGeneration(tp);
            assertTrue(reacquiredGeneration > lostGeneration.get());
            assertTrue(support.isCurrentDisklessOwnershipGeneration(tp, reacquiredGeneration));
            assertTrue(support.snapshotTrackedPartitions().isEmpty());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testReconcilePublicationOwnershipWakesUntrackedTailAndFencesTransfer() throws Exception {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-publication-owner-topic", 0);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        AtomicInteger selectedBroker = new AtomicInteger(localBrokerId);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(true);
        when(selector.activeZones()).thenReturn(Set.of(DisklessClientZone.NO_ZONE));
        when(selector.selectBrokerForZone(tp.topicId(), tp.partition(), DisklessClientZone.NO_ZONE))
                .thenAnswer(ignored -> java.util.OptionalInt.of(selectedBroker.get()));

        try (DisklessStorageReplicaManagerSupport support =
                 new DisklessStorageReplicaManagerSupport(
                     metadataView, localBrokerId, selector, mock(DisklessStorageEngine.class))) {
            AtomicInteger acquisitionCount = new AtomicInteger();
            AtomicInteger lossCount = new AtomicInteger();
            AtomicLong acquiredGeneration = new AtomicLong();
            AtomicLong lostGeneration = new AtomicLong();

            support.reconcilePublicationOwnership(
                    Set.of(tp),
                    (partition, generation) -> {
                        assertEquals(tp, partition);
                        lossCount.incrementAndGet();
                        lostGeneration.set(generation);
                    },
                    (partition, generation) -> {
                        assertEquals(tp, partition);
                        acquisitionCount.incrementAndGet();
                        acquiredGeneration.set(generation);
                    });

            assertEquals(1, acquisitionCount.get());
            assertEquals(0, lossCount.get());
            assertTrue(acquiredGeneration.get() > 0);
            assertTrue(support.isCurrentDisklessOwnershipGeneration(tp, acquiredGeneration.get()));
            assertTrue(support.snapshotTrackedPartitions().isEmpty());

            support.reconcilePublicationOwnership(
                    Set.of(tp),
                    (partition, generation) -> lossCount.incrementAndGet(),
                    (partition, generation) -> acquisitionCount.incrementAndGet());
            assertEquals(1, acquisitionCount.get());
            assertEquals(0, lossCount.get());

            selectedBroker.set(localBrokerId + 1);
            support.reconcilePublicationOwnership(
                    Set.of(tp),
                    (partition, generation) -> {
                        lossCount.incrementAndGet();
                        lostGeneration.set(generation);
                    },
                    (partition, generation) -> acquisitionCount.incrementAndGet());

            assertEquals(1, acquisitionCount.get());
            assertEquals(1, lossCount.get());
            assertTrue(lostGeneration.get() > acquiredGeneration.get());
            assertFalse(support.isCurrentDisklessOwnershipGeneration(tp, acquiredGeneration.get()));
            assertEquals(0, support.ownershipGenerationCount());

            selectedBroker.set(localBrokerId);
            support.reconcilePublicationOwnership(
                    Set.of(tp),
                    (partition, generation) -> lossCount.incrementAndGet(),
                    (partition, generation) -> {
                        acquisitionCount.incrementAndGet();
                        acquiredGeneration.set(generation);
                    });

            assertEquals(2, acquisitionCount.get());
            assertEquals(1, lossCount.get());
            assertTrue(acquiredGeneration.get() > lostGeneration.get());
        }
    }

    @Test
    void testGenerationAwareLogMetadataRejectsLossBeforeLazyOpen() throws Exception {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-metadata-loss-before-open", 0);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        DisklessStorageEngine engine = mock(DisklessStorageEngine.class);
        AtomicInteger selectedBroker = new AtomicInteger(localBrokerId);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(true);
        when(selector.activeZones()).thenReturn(Set.of(DisklessClientZone.NO_ZONE));
        when(selector.selectBrokerForZone(tp.topicId(), tp.partition(), DisklessClientZone.NO_ZONE))
                .thenAnswer(ignored -> java.util.OptionalInt.of(selectedBroker.get()));

        try (DisklessStorageReplicaManagerSupport support =
                 new DisklessStorageReplicaManagerSupport(
                     metadataView, localBrokerId, selector, engine)) {
            long generation = support.currentDisklessOwnershipGeneration(tp);
            selectedBroker.set(localBrokerId + 1);
            support.reconcilePublicationOwnership(Set.of(tp), (ignored, lost) -> { }, (ignored, acquired) -> { });

            assertTrue(support.logMetadata(tp, generation).join().isEmpty());
            verify(engine, never()).logMetadata(tp);
        }
    }

    @Test
    void testGenerationAwareLogMetadataRegistersLazyOpenBeforeOwnershipLoss() throws Exception {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-metadata-open-before-loss", 0);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        DisklessStorageEngine engine = mock(DisklessStorageEngine.class);
        AtomicInteger selectedBroker = new AtomicInteger(localBrokerId);
        CountDownLatch openStarted = new CountDownLatch(1);
        CountDownLatch releaseOpen = new CountDownLatch(1);
        CountDownLatch lossStarted = new CountDownLatch(1);
        AtomicBoolean trackedLog = new AtomicBoolean();
        AtomicInteger lossCount = new AtomicInteger();
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(true);
        when(selector.activeZones()).thenReturn(Set.of(DisklessClientZone.NO_ZONE));
        when(selector.selectBrokerForZone(tp.topicId(), tp.partition(), DisklessClientZone.NO_ZONE))
                .thenAnswer(ignored -> java.util.OptionalInt.of(selectedBroker.get()));
        when(engine.logMetadata(tp)).thenAnswer(ignored -> {
            trackedLog.set(true);
            openStarted.countDown();
            assertTrue(releaseOpen.await(10, TimeUnit.SECONDS));
            return CompletableFuture.completedFuture(Optional.of(new DisklessLogMetadata(73L, 10L)));
        });
        when(engine.snapshotTrackedPartitions()).thenAnswer(
                ignored -> trackedLog.get() ? Set.of(tp) : Set.of());
        when(engine.cleanupPartition(tp, false)).thenAnswer(ignored -> {
            assertTrue(trackedLog.get());
            trackedLog.set(false);
            return true;
        });

        try (DisklessStorageReplicaManagerSupport support =
                 new DisklessStorageReplicaManagerSupport(
                     metadataView, localBrokerId, selector, engine)) {
            long generation = support.currentDisklessOwnershipGeneration(tp);
            CompletableFuture<CompletableFuture<Optional<DisklessLogMetadata>>> metadataCall =
                    CompletableFuture.supplyAsync(() -> support.logMetadata(tp, generation));
            assertTrue(openStarted.await(10, TimeUnit.SECONDS));

            selectedBroker.set(localBrokerId + 1);
            CompletableFuture<Void> ownershipLoss = CompletableFuture.runAsync(() -> {
                lossStarted.countDown();
                Set<TopicIdPartition> fenced = support.reconcilePublicationOwnership(
                        Set.of(tp),
                        (ignored, lost) -> lossCount.incrementAndGet(),
                        (ignored, acquired) -> { });
                support.reconcileTrackedPartitions(
                        Set.of(),
                        ignored -> { },
                        (ignored, lost) -> lossCount.incrementAndGet(),
                        fenced);
            });
            assertTrue(lossStarted.await(10, TimeUnit.SECONDS));
            try {
                assertThrows(TimeoutException.class, () -> ownershipLoss.get(200, TimeUnit.MILLISECONDS));
            } finally {
                releaseOpen.countDown();
            }

            assertEquals(73L, metadataCall.get(10, TimeUnit.SECONDS).join().orElseThrow().streamId());
            ownershipLoss.get(10, TimeUnit.SECONDS);
            assertFalse(support.isCurrentDisklessOwnershipGeneration(tp, generation));
            assertFalse(trackedLog.get());
            assertTrue(support.snapshotTrackedPartitions().isEmpty());
            assertEquals(0, support.ownershipGenerationCount());
            assertEquals(1, lossCount.get());
            verify(engine).cleanupPartition(tp, false);
        } finally {
            releaseOpen.countDown();
        }
    }

    @Test
    void testReconcilePublicationOwnershipFencesOldUuidBeforeAcquiringReplacement() throws Exception {
        int localBrokerId = 1;
        String topic = "diskless-publication-recreation-topic";
        TopicIdPartition oldPartition = new TopicIdPartition(
                Uuid.randomUuid(), new TopicPartition(topic, 0));
        TopicIdPartition newPartition = new TopicIdPartition(
                Uuid.randomUuid(), new TopicPartition(topic, 0));
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        AtomicReference<Uuid> currentTopicId = new AtomicReference<>(oldPartition.topicId());
        when(metadataView.getTopicId(topic)).thenAnswer(ignored -> currentTopicId.get());
        when(metadataView.isDisklessStorageTopic(topic)).thenReturn(true);
        when(selector.activeZones()).thenReturn(Set.of(DisklessClientZone.NO_ZONE));
        when(selector.selectBrokerForZone(any(), eq(0), eq(DisklessClientZone.NO_ZONE)))
                .thenReturn(java.util.OptionalInt.of(localBrokerId));

        try (DisklessStorageReplicaManagerSupport support =
                 new DisklessStorageReplicaManagerSupport(
                     metadataView, localBrokerId, selector, mock(DisklessStorageEngine.class))) {
            AtomicLong initialGeneration = new AtomicLong();
            support.reconcilePublicationOwnership(
                    Set.of(oldPartition),
                    (partition, generation) -> { },
                    (partition, generation) -> initialGeneration.set(generation));

            currentTopicId.set(newPartition.topicId());
            List<String> events = new ArrayList<>();
            AtomicLong lostGeneration = new AtomicLong();
            AtomicLong replacementGeneration = new AtomicLong();
            support.reconcilePublicationOwnership(
                    Set.of(newPartition),
                    (partition, generation) -> {
                        events.add("lost:" + partition.topicId());
                        lostGeneration.set(generation);
                    },
                    (partition, generation) -> {
                        events.add("acquired:" + partition.topicId());
                        replacementGeneration.set(generation);
                    });

            assertEquals(List.of(
                    "lost:" + oldPartition.topicId(),
                    "acquired:" + newPartition.topicId()), events);
            assertTrue(lostGeneration.get() > initialGeneration.get());
            assertTrue(replacementGeneration.get() > lostGeneration.get());
        }
    }

    @Test
    void testReconcilePublicationOwnershipRemovesRetainedClassicFenceAfterDeletion() throws Exception {
        int localBrokerId = 1;
        String topic = "diskless-classic-deletion-topic";
        TopicIdPartition oldPartition = new TopicIdPartition(
                Uuid.randomUuid(), new TopicPartition(topic, 0));
        TopicIdPartition newPartition = new TopicIdPartition(
                Uuid.randomUuid(), new TopicPartition(topic, 0));
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        AtomicReference<Uuid> currentTopicId = new AtomicReference<>(oldPartition.topicId());
        AtomicBoolean diskless = new AtomicBoolean(true);
        when(metadataView.getTopicId(topic)).thenAnswer(ignored -> currentTopicId.get());
        when(metadataView.isDisklessStorageTopic(topic)).thenAnswer(ignored -> diskless.get());
        when(selector.activeZones()).thenReturn(Set.of(DisklessClientZone.NO_ZONE));
        when(selector.selectBrokerForZone(any(), eq(0), eq(DisklessClientZone.NO_ZONE)))
                .thenReturn(java.util.OptionalInt.of(localBrokerId));

        try (DisklessStorageReplicaManagerSupport support =
                 new DisklessStorageReplicaManagerSupport(
                     metadataView, localBrokerId, selector, mock(DisklessStorageEngine.class))) {
            AtomicInteger lossCount = new AtomicInteger();
            AtomicInteger acquisitionCount = new AtomicInteger();
            support.reconcilePublicationOwnership(
                    Set.of(oldPartition),
                    (partition, generation) -> lossCount.incrementAndGet(),
                    (partition, generation) -> acquisitionCount.incrementAndGet());

            diskless.set(false);
            support.reconcilePublicationOwnership(
                    Set.of(),
                    (partition, generation) -> lossCount.incrementAndGet(),
                    (partition, generation) -> acquisitionCount.incrementAndGet());
            assertEquals(1, lossCount.get());
            assertEquals(1, support.ownershipGenerationCount());

            currentTopicId.set(Uuid.ZERO_UUID);
            support.reconcilePublicationOwnership(
                    Set.of(),
                    (partition, generation) -> lossCount.incrementAndGet(),
                    (partition, generation) -> acquisitionCount.incrementAndGet());
            assertEquals(1, lossCount.get());
            assertEquals(0, support.ownershipGenerationCount());

            currentTopicId.set(newPartition.topicId());
            diskless.set(true);
            support.reconcilePublicationOwnership(
                    Set.of(newPartition),
                    (partition, generation) -> lossCount.incrementAndGet(),
                    (partition, generation) -> acquisitionCount.incrementAndGet());
            assertEquals(1, lossCount.get());
            assertEquals(2, acquisitionCount.get());
            assertEquals(1, support.ownershipGenerationCount());
        }
    }

    @Test
    void testClassicPublisherGenerationFencesDisklessOwnershipAndReacquisition() throws Exception {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("storage-mode-generation-topic", 0);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(true, false, false, true, true);
        stubNoZoneSelection(selector, tp, localBrokerId);

        try (DisklessStorageReplicaManagerSupport support =
                 new DisklessStorageReplicaManagerSupport(
                     metadataView, localBrokerId, selector, mock(DisklessStorageEngine.class))) {
            long disklessGeneration = support.currentDisklessOwnershipGeneration(tp);
            long classicGeneration = support.currentClassicPublisherGeneration(tp);

            assertTrue(classicGeneration > disklessGeneration);
            assertEquals(classicGeneration, support.currentClassicPublisherGeneration(tp));

            long reacquiredGeneration = support.currentDisklessOwnershipGeneration(tp);
            assertTrue(reacquiredGeneration > classicGeneration);
            assertTrue(support.isCurrentDisklessOwnershipGeneration(tp, reacquiredGeneration));
        }
    }

    @Test
    void testPureClassicPublisherGenerationDoesNotCreateDisklessOwnershipState() throws Exception {
        TopicIdPartition tp = topicIdPartition("pure-classic-generation-topic", 0);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(false);

        try (DisklessStorageReplicaManagerSupport support =
                 new DisklessStorageReplicaManagerSupport(
                     metadataView,
                     1,
                     mock(DisklessBrokerSelector.class),
                     mock(DisklessStorageEngine.class))) {
            long firstGeneration = support.currentClassicPublisherGeneration(tp);
            long secondGeneration = support.currentClassicPublisherGeneration(tp);
            assertTrue(firstGeneration > 0);
            assertTrue(secondGeneration > firstGeneration);
            assertEquals(0, support.ownershipGenerationCount());
        }
    }

    @Test
    void testReconcileCleansTrackedPartitionWhenTopicIdChanges() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-topicid-change-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        configureWriterState(writer, ursaState, true);
        AtomicReference<Uuid> currentTopicId = new AtomicReference<>(tp.topicId());
        when(metadataView.getTopicId(tp.topic())).thenAnswer(ignored -> currentTopicId.get());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(true);
        stubNoZoneSelection(selector, tp, localBrokerId);

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            support.handleAppend(Map.of(tp, MemoryRecords.EMPTY), null).join();
            currentTopicId.set(Uuid.randomUuid());
            clearInvocations(selector);

            support.reconcileTrackedPartitions(Set.of(), ignored -> { });

            verify(ursaState).cleanupPartition(tp, false);
            verify(ursaState, never()).deletePartitionData(tp);
            assertTrue(support.snapshotTrackedPartitions().isEmpty());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testReconcileKeepsTrackedPartitionWhenBrokerIsStillOwner() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-owned-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        configureWriterState(writer, ursaState, true);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(true);
        stubNoZoneSelection(selector, tp, localBrokerId);
        when(selector.activeZones()).thenReturn(Set.of(DisklessClientZone.NO_ZONE, "zone-b"));
        when(selector.selectBrokerForZone(tp.topicId(), tp.partition(), DisklessClientZone.NO_ZONE))
                .thenReturn(java.util.OptionalInt.of(localBrokerId));
        when(selector.selectBrokerForZone(tp.topicId(), tp.partition(), "zone-b"))
                .thenReturn(java.util.OptionalInt.of(localBrokerId + 1));

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            support.handleAppend(Map.of(tp, MemoryRecords.EMPTY), null).join();
            clearInvocations(selector);

            support.reconcileTrackedPartitions(Set.of(), ignored -> { });

            verify(ursaState, never()).cleanupPartition(any(), eq(false));
            verify(ursaState, never()).deletePartitionData(any());
            assertEquals(Set.of(tp), support.snapshotTrackedPartitions());
            assertTrue(support.hasTrackedPartitionsForTopic(tp.topic()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testReconcileCleansNonOwnedProducerStatesOnRetainedPartition() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-no-owner-check-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        configureWriterState(writer, ursaState, true);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(true);
        stubNoZoneSelection(selector, tp, localBrokerId);
        when(selector.activeZones()).thenReturn(Set.of(DisklessClientZone.NO_ZONE, "zone-b"));
        when(selector.selectBrokerForZone(tp.topicId(), tp.partition(), DisklessClientZone.NO_ZONE))
                .thenReturn(java.util.OptionalInt.of(localBrokerId));
        when(selector.selectBrokerForZone(tp.topicId(), tp.partition(), "zone-b"))
                .thenReturn(java.util.OptionalInt.of(localBrokerId + 1));

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            support.handleAppend(Map.of(tp, MemoryRecords.EMPTY), null).join();
            clearInvocations(selector);

            support.reconcileTrackedPartitions(Set.of(), ignored -> { });

            verify(ursaState, never()).cleanupPartition(any(), eq(false));
            verify(ursaState).cleanupNonOwnedProducerStates(tp, Set.of(DisklessClientZone.NO_ZONE), false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testReconcileRetainsPartitionStateWhenBrokerOnlyOwnsZoneScopedState() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-zone-only-owned-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        configureWriterState(writer, ursaState, true);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(true);
        stubNoZoneSelection(selector, tp, localBrokerId);

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            support.handleAppend(Map.of(tp, MemoryRecords.EMPTY), null).join();
            clearInvocations(selector);
            when(selector.activeZones()).thenReturn(Set.of(DisklessClientZone.NO_ZONE, "zone-a"));
            when(selector.selectBrokerForZone(tp.topicId(), tp.partition(), DisklessClientZone.NO_ZONE))
                    .thenReturn(java.util.OptionalInt.of(localBrokerId + 1));
            when(selector.selectBrokerForZone(tp.topicId(), tp.partition(), "zone-a"))
                    .thenReturn(java.util.OptionalInt.of(localBrokerId));

            support.reconcileTrackedPartitions(Set.of(), ignored -> { });

            verify(ursaState, never()).cleanupPartition(tp, false);
            verify(ursaState).cleanupNonOwnedProducerStates(tp, Set.of("zone-a"), false);
            assertEquals(Set.of(tp), support.snapshotTrackedPartitions());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testReconcileRetainsTrackedPartitionWhenCleanupFails() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-retry-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        configureWriterState(writer, ursaState, false);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        AtomicBoolean diskless = new AtomicBoolean(true);
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenAnswer(ignored -> diskless.get());
        stubNoZoneSelection(selector, tp, localBrokerId);
        doThrow(new RuntimeException("boom")).when(ursaState).cleanupPartition(tp, false);

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            support.handleAppend(Map.of(tp, MemoryRecords.EMPTY), null).join();
            diskless.set(false);

            AtomicInteger callbackCount = new AtomicInteger(0);
            AtomicInteger ownershipLossCount = new AtomicInteger();
            support.reconcileTrackedPartitions(
                    Set.of(),
                    ignored -> callbackCount.incrementAndGet(),
                    ignored -> ownershipLossCount.incrementAndGet());

            verify(ursaState, times(1)).cleanupPartition(tp, false);
            verify(ursaState, never()).deletePartitionData(tp);
            assertEquals(0, callbackCount.get());
            assertEquals(1, ownershipLossCount.get());
            assertEquals(Set.of(tp), support.snapshotTrackedPartitions());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testReconcileCleansUntrackedUrsaStateDiscoveredViaStateSnapshot() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-untracked-state-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(false);
        when(ursaState.snapshotTrackedPartitions()).thenReturn(Set.of(tp));

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            support.reconcileTrackedPartitions(Set.of(), ignored -> { });

            verify(ursaState).cleanupPartition(tp, false);
            verify(ursaState, never()).deletePartitionData(tp);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testReconcileDeletesUntrackedDeletedPartitionDataWhenCurrentBrokerIsOwner() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-deleted-untracked-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        stubNoZoneSelection(selector, tp, localBrokerId);

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            support.reconcileTrackedPartitions(Set.of(tp), ignored -> { });

            verify(ursaState, never()).cleanupPartition(any(), eq(false));
            verify(ursaState).deletePartitionData(tp);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private TopicIdPartition topicIdPartition(String topic, int partition) {
        return new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(topic, partition));
    }

    private void stubNoZoneSelection(DisklessBrokerSelector selector, TopicIdPartition tp, int brokerId) {
        when(selector.effectiveZone(any())).thenReturn(DisklessClientZone.NO_ZONE);
        when(selector.selectBrokerForZone(tp.topicId(), tp.partition(), DisklessClientZone.NO_ZONE))
                .thenReturn(java.util.OptionalInt.of(brokerId));
    }

    private void configureWriterState(Writer writer, DisklessStorageStateOperations ursaState, boolean removeOnCleanup) {
        Set<TopicIdPartition> trackedState = new LinkedHashSet<>();
        when(ursaState.snapshotTrackedPartitions()).thenAnswer(invocation -> Set.copyOf(trackedState));
        when(writer.write(any(), anyString())).thenAnswer(invocation -> {
            Map<TopicIdPartition, MemoryRecords> entries = invocation.getArgument(0);
            trackedState.addAll(entries.keySet());
            return CompletableFuture.completedFuture(Map.of());
        });
        if (removeOnCleanup) {
            org.mockito.Mockito.doAnswer(invocation -> {
                TopicIdPartition tp = invocation.getArgument(0);
                trackedState.remove(tp);
                return true;
            }).when(ursaState).cleanupPartition(any(), anyBoolean());
        }
    }
}
