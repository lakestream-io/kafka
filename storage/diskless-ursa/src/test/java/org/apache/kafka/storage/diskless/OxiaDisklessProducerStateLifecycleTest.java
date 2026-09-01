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
import org.apache.kafka.storage.diskless.DisklessProducerStateLifecycle.ManagedProducerStateTopic;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateSnapshotKeys;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OxiaDisklessProducerStateLifecycleTest {

    @Test
    void testReconcilePersistsInventoryAndRejectsRevisionRegression() throws Exception {
        Uuid topicId = Uuid.fromString("65WMNfybQpCDVulYOxMCTw");
        InMemoryOxia oxia = new InMemoryOxia();

        try (OxiaDisklessProducerStateLifecycle store = new OxiaDisklessProducerStateLifecycle(oxia.client())) {
            store.reconcileTopic("orders", topicId, 17).get();
            store.reconcileTopic("orders", topicId, 9).get();

            assertEquals(
                    List.of(new ManagedProducerStateTopic("orders", topicId, 17)),
                    store.listManagedTopics().get());

            store.reconcileTopic("orders", topicId, 23).get();
            assertEquals(
                    List.of(new ManagedProducerStateTopic("orders", topicId, 23)),
                    store.listManagedTopics().get());
        }
    }

    @Test
    void testDeleteJournalsThenFencesBeforeCleaningSnapshotsAndManifest() throws Exception {
        Uuid topicId = Uuid.fromString("65WMNfybQpCDVulYOxMCTw");
        Uuid otherTopicId = Uuid.fromString("VkZ5AkuESPGkMc2OxpKUjw");
        String topicPrefix = ProducerStateSnapshotKeys.topicSnapshotPrefix(topicId.toString());
        String deletedTopicMarkerKey = ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicId.toString());
        String cleanupJournalKey = ProducerStateSnapshotKeys.cleanupJournalKey(topicId.toString());
        String unzonedKey = ProducerStateSnapshotKeys.snapshotKey(topicId.toString(), 0);
        String zonedKey = ProducerStateSnapshotKeys.snapshotKey(topicId.toString(), 1, "rack/region/zone");
        String otherTopicKey = ProducerStateSnapshotKeys.snapshotKey(
                otherTopicId.toString(), 0, "rack/region/zone");
        String malformedTopicScopedKey = topicPrefix + "metadata";
        InMemoryOxia oxia = new InMemoryOxia();
        oxia.storeValue(unzonedKey, new byte[]{1});
        oxia.storeValue(zonedKey, new byte[]{2});
        oxia.storeValue(otherTopicKey, new byte[]{3});
        oxia.storeValue(malformedTopicScopedKey, new byte[]{4});
        oxia.indexKeys(List.of(unzonedKey, otherTopicKey, malformedTopicScopedKey, zonedKey));

        try (OxiaDisklessProducerStateLifecycle store = new OxiaDisklessProducerStateLifecycle(oxia.client())) {
            store.reconcileTopic("orders", topicId, 17).get();
            oxia.clearEvents();

            store.deleteTopicSnapshots(topicId).get();

            assertTrue(oxia.contains(deletedTopicMarkerKey), "the permanent deletion fence must remain");
            assertFalse(oxia.contains(unzonedKey));
            assertFalse(oxia.contains(zonedKey));
            assertTrue(oxia.contains(otherTopicKey));
            assertEquals(List.of(), store.listManagedTopics().get(),
                    "the permanent fence must not remain in the active cleanup inventory");
            int markerPut = firstEventWithPrefix(
                    oxia.events(), "conditional-put:" + deletedTopicMarkerKey);
            int journalPut = firstEventWithPrefix(
                    oxia.events(), "conditional-put:" + cleanupJournalKey);
            int snapshotRangeDelete = oxia.events().indexOf(
                    "delete-range:" + topicPrefix + ":" + topicPrefix + '\uffff');
            int manifestDelete = firstEventWithPrefix(oxia.events(), "conditional-delete:");
            assertTrue(journalPut >= 0 && journalPut < markerPut);
            assertTrue(markerPut < snapshotRangeDelete);
            assertTrue(snapshotRangeDelete < manifestDelete);

            ExecutionException deletedFailure = assertThrows(
                    ExecutionException.class,
                    () -> store.reconcileTopic("orders", topicId, 99).get());
            assertInstanceOf(IllegalStateException.class, deletedFailure.getCause());
            assertEquals(List.of(), store.listManagedTopics().get());
        }
    }

    @Test
    void testReconcileStartedBeforeDeletionCannotResurrectManifest() throws Exception {
        Uuid topicId = Uuid.fromString("65WMNfybQpCDVulYOxMCTw");
        String deletedTopicMarkerKey = ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicId.toString());
        InMemoryOxia oxia = new InMemoryOxia();
        CompletableFuture<GetResult> delayedManifestRead = oxia.deferNextGet(
                key -> !key.equals(deletedTopicMarkerKey));

        try (OxiaDisklessProducerStateLifecycle store = new OxiaDisklessProducerStateLifecycle(oxia.client())) {
            CompletableFuture<Void> reconcile = store.reconcileTopic("orders", topicId, 17);
            assertFalse(reconcile.isDone());

            store.deleteTopicSnapshots(topicId).get();
            delayedManifestRead.complete(null);

            ExecutionException deletedFailure = assertThrows(ExecutionException.class, reconcile::get);
            assertInstanceOf(IllegalStateException.class, deletedFailure.getCause());
            assertTrue(oxia.contains(deletedTopicMarkerKey));
            assertEquals(List.of(), store.listManagedTopics().get());
        }
    }

    @Test
    void testDeletionWaitsForActiveWriterClaimBeforeCleaningSnapshots() throws Exception {
        Uuid topicId = Uuid.fromString("65WMNfybQpCDVulYOxMCTw");
        String markerKey = ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicId.toString());
        String snapshotKey = ProducerStateSnapshotKeys.snapshotKey(topicId.toString(), 0);
        String writerClaimKey = ProducerStateSnapshotKeys.writerClaimKey(topicId.toString(), "manager-1");
        InMemoryOxia oxia = new InMemoryOxia();
        ManualExecutor claimRetryExecutor = new ManualExecutor();
        oxia.storeValue(snapshotKey, new byte[]{9});
        oxia.storeValue(writerClaimKey, new byte[0]);
        oxia.indexKeys(List.of(snapshotKey));

        try (OxiaDisklessProducerStateLifecycle store = new OxiaDisklessProducerStateLifecycle(
                oxia.client(), TimeUnit.SECONDS.toMillis(10), claimRetryExecutor)) {
            store.reconcileTopic("orders", topicId, 17).get();

            CompletableFuture<Void> deletion = store.deleteTopicSnapshots(topicId);
            assertFalse(deletion.isDone());
            assertTrue(oxia.contains(markerKey), "the durable fence must be installed before waiting");
            assertTrue(oxia.contains(snapshotKey));
            assertEquals(1, claimRetryExecutor.pendingTaskCount());

            oxia.removeValue(writerClaimKey);
            claimRetryExecutor.runNext();
            deletion.get();

            assertFalse(oxia.contains(snapshotKey));
            assertTrue(oxia.contains(markerKey));
            assertEquals(List.of(), store.listManagedTopics().get());
        }
    }

    @Test
    void testInterruptedCleanupRemainsDiscoverableAndRetryRemovesOnlyActiveJournal() throws Exception {
        Uuid topicId = Uuid.fromString("65WMNfybQpCDVulYOxMCTw");
        String markerKey = ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicId.toString());
        String cleanupJournalKey = ProducerStateSnapshotKeys.cleanupJournalKey(topicId.toString());
        InMemoryOxia oxia = new InMemoryOxia();
        CompletableFuture<List<String>> failedClaimList = oxia.deferNextList();

        try (OxiaDisklessProducerStateLifecycle store = new OxiaDisklessProducerStateLifecycle(oxia.client())) {
            store.reconcileTopic("orders", topicId, 17).get();

            CompletableFuture<Void> firstAttempt = store.deleteTopicSnapshots(topicId);
            RuntimeException listFailure = new RuntimeException("claim inventory unavailable");
            failedClaimList.completeExceptionally(listFailure);
            ExecutionException failure = assertThrows(ExecutionException.class, firstAttempt::get);
            assertEquals(listFailure, failure.getCause());
            assertTrue(oxia.contains(markerKey));
            assertTrue(oxia.contains(cleanupJournalKey));
            assertEquals(
                    List.of(new ManagedProducerStateTopic("orders", topicId, 17)),
                    store.listManagedTopics().get());

            store.deleteTopicSnapshots(topicId).get();
            assertTrue(oxia.contains(markerKey), "the permanent deletion fence must remain");
            assertFalse(oxia.contains(cleanupJournalKey), "successful cleanup must retire its active journal");
            assertEquals(List.of(), store.listManagedTopics().get());
        }
    }

    @Test
    void testJournalFirstCrashWithoutManifestRemainsDiscoverable() throws Exception {
        Uuid topicId = Uuid.fromString("65WMNfybQpCDVulYOxMCTw");
        String markerKey = ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicId.toString());
        String cleanupJournalKey = ProducerStateSnapshotKeys.cleanupJournalKey(topicId.toString());
        InMemoryOxia oxia = new InMemoryOxia();
        CompletableFuture<PutResult> pendingFence = oxia.deferNextConditionalPut(markerKey);

        try (OxiaDisklessProducerStateLifecycle store = new OxiaDisklessProducerStateLifecycle(oxia.client())) {
            CompletableFuture<Void> deletion = store.deleteTopicSnapshots(topicId);

            assertFalse(deletion.isDone());
            assertTrue(oxia.contains(cleanupJournalKey));
            assertFalse(oxia.contains(markerKey));
            assertEquals(List.of(deletedTopicJournal(topicId)), store.listManagedTopics().get());

            RuntimeException crash = new RuntimeException("controller stopped before fencing");
            pendingFence.completeExceptionally(crash);
            ExecutionException failure = assertThrows(ExecutionException.class, deletion::get);
            assertEquals(crash, failure.getCause());

            store.deleteTopicSnapshots(topicId).get();
            assertTrue(oxia.contains(markerKey));
            assertFalse(oxia.contains(cleanupJournalKey));
            assertEquals(List.of(), store.listManagedTopics().get());
        }
    }

    @Test
    void testActiveWriterWaitIsBoundedAndRetryable() throws Exception {
        Uuid topicId = Uuid.fromString("65WMNfybQpCDVulYOxMCTw");
        String markerKey = ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicId.toString());
        String cleanupJournalKey = ProducerStateSnapshotKeys.cleanupJournalKey(topicId.toString());
        String writerClaimKey = ProducerStateSnapshotKeys.writerClaimKey(topicId.toString(), "manager-1");
        InMemoryOxia oxia = new InMemoryOxia();
        ManualExecutor claimRetryExecutor = new ManualExecutor();
        oxia.storeValue(writerClaimKey, new byte[0]);

        try (OxiaDisklessProducerStateLifecycle store = new OxiaDisklessProducerStateLifecycle(
                oxia.client(), TimeUnit.SECONDS.toMillis(10), claimRetryExecutor, 2)) {
            store.reconcileTopic("orders", topicId, 17).get();

            CompletableFuture<Void> deletion = store.deleteTopicSnapshots(topicId);
            assertEquals(1, claimRetryExecutor.pendingTaskCount());
            claimRetryExecutor.runNext();

            ExecutionException failure = assertThrows(ExecutionException.class, deletion::get);
            assertInstanceOf(TimeoutException.class, failure.getCause());
            assertTrue(oxia.contains(markerKey));
            assertTrue(oxia.contains(cleanupJournalKey));
            assertEquals(
                    List.of(new ManagedProducerStateTopic("orders", topicId, 17)),
                    store.listManagedTopics().get());

            oxia.removeValue(writerClaimKey);
            store.deleteTopicSnapshots(topicId).get();
            assertEquals(List.of(), store.listManagedTopics().get());
        }
    }

    @Test
    void testCancelledWriterWaitDoesNotStartAnotherPoll() throws Exception {
        Uuid topicId = Uuid.fromString("65WMNfybQpCDVulYOxMCTw");
        String writerClaimKey = ProducerStateSnapshotKeys.writerClaimKey(topicId.toString(), "manager-1");
        String writerClaimPrefix = ProducerStateSnapshotKeys.writerClaimPrefix(topicId.toString());
        InMemoryOxia oxia = new InMemoryOxia();
        ManualExecutor claimRetryExecutor = new ManualExecutor();
        oxia.storeValue(writerClaimKey, new byte[0]);

        try (OxiaDisklessProducerStateLifecycle store = new OxiaDisklessProducerStateLifecycle(
                oxia.client(), TimeUnit.SECONDS.toMillis(10), claimRetryExecutor)) {
            CompletableFuture<Void> deletion = store.deleteTopicSnapshots(topicId);
            assertEquals(1, countEventsWithPrefix(oxia.events(), "list:" + writerClaimPrefix));
            assertTrue(deletion.cancel(true));

            claimRetryExecutor.runNext();
            assertEquals(1, countEventsWithPrefix(oxia.events(), "list:" + writerClaimPrefix),
                    "cancellation must stop the delayed writer-claim polling chain");
        }
    }

    @Test
    void testCorruptManifestDoesNotPoisonInventoryAndExactDeleteQuarantinesIt() throws Exception {
        Uuid validTopicId = Uuid.fromString("65WMNfybQpCDVulYOxMCTw");
        Uuid corruptTopicId = Uuid.fromString("VkZ5AkuESPGkMc2OxpKUjw");
        String corruptManifestKey = "producer-state-managed-topic/" + corruptTopicId;
        InMemoryOxia oxia = new InMemoryOxia();
        oxia.storeValue(corruptManifestKey, new byte[]{1, 2, 3});

        try (OxiaDisklessProducerStateLifecycle store = new OxiaDisklessProducerStateLifecycle(oxia.client())) {
            store.reconcileTopic("orders", validTopicId, 17).get();

            assertEquals(
                    List.of(new ManagedProducerStateTopic("orders", validTopicId, 17)),
                    store.listManagedTopics().get());

            store.deleteTopicSnapshots(corruptTopicId).get();
            assertFalse(oxia.contains(corruptManifestKey));
            assertEquals(
                    Set.of(new ManagedProducerStateTopic("orders", validTopicId, 17)),
                    Set.copyOf(store.listManagedTopics().get()));
        }
    }

    @Test
    void testInventoryCancellationBoundsGetFanoutAndStopsNewReads() throws Exception {
        InMemoryOxia oxia = new InMemoryOxia();
        for (int index = 0; index < 100; index++) {
            Uuid topicId = Uuid.randomUuid();
            oxia.storeValue("producer-state-managed-topic/" + topicId, new byte[]{1});
        }
        oxia.holdGets(key -> key.startsWith("producer-state-managed-topic/"));

        try (OxiaDisklessProducerStateLifecycle store = new OxiaDisklessProducerStateLifecycle(oxia.client())) {
            CompletableFuture<List<ManagedProducerStateTopic>> inventory = store.listManagedTopics();
            assertEquals(32, oxia.heldGetCount());
            assertTrue(inventory.cancel(true));
            assertTrue(oxia.allHeldGetsCancelled());
            assertEquals(32, oxia.heldGetCount(), "cancellation must not start another read batch");
        }
    }

    @Test
    void testInventoryTimeoutCancelsImmediateListSource() throws Exception {
        InMemoryOxia oxia = new InMemoryOxia();
        CompletableFuture<List<String>> pendingList = oxia.deferNextList();

        try (OxiaDisklessProducerStateLifecycle store =
                     new OxiaDisklessProducerStateLifecycle(oxia.client(), 25)) {
            ExecutionException timeout = assertThrows(
                    ExecutionException.class,
                    () -> store.listManagedTopics().get(5, TimeUnit.SECONDS));
            assertInstanceOf(TimeoutException.class, timeout.getCause());
            assertTrue(pendingList.isCancelled());
        }
    }

    private static ManagedProducerStateTopic deletedTopicJournal(Uuid topicId) {
        return new ManagedProducerStateTopic("deleted-producer-state-" + topicId, topicId, 0);
    }

    private static final class ManualExecutor implements Executor {
        private final List<Runnable> tasks = new ArrayList<>();

        @Override
        public synchronized void execute(Runnable command) {
            tasks.add(command);
        }

        synchronized int pendingTaskCount() {
            return tasks.size();
        }

        void runNext() {
            Runnable task;
            synchronized (this) {
                task = tasks.remove(0);
            }
            task.run();
        }
    }

    private static int firstEventWithPrefix(List<String> events, String prefix) {
        for (int index = 0; index < events.size(); index++) {
            if (events.get(index).startsWith(prefix)) {
                return index;
            }
        }
        return -1;
    }

    private static long countEventsWithPrefix(List<String> events, String prefix) {
        return events.stream().filter(event -> event.startsWith(prefix)).count();
    }

    private static final class InMemoryOxia {
        private final Map<String, byte[]> data = new ConcurrentHashMap<>();
        private final Map<String, Long> versions = new ConcurrentHashMap<>();
        private final AtomicLong nextVersionId = new AtomicLong();
        private final List<String> indexedKeys = new ArrayList<>();
        private final List<String> events = new ArrayList<>();
        private final AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        private Predicate<String> deferredGetPredicate;
        private CompletableFuture<GetResult> deferredGet;
        private String deferredConditionalPutKey;
        private CompletableFuture<PutResult> deferredConditionalPut;
        private CompletableFuture<List<String>> deferredList;
        private Predicate<String> heldGetPredicate;
        private final List<CompletableFuture<GetResult>> heldGets = new ArrayList<>();

        InMemoryOxia() throws Exception {
            when(client.get(anyString())).thenAnswer(invocation -> {
                String key = invocation.getArgument(0);
                synchronized (this) {
                    if (deferredGet != null && deferredGetPredicate.test(key)) {
                        CompletableFuture<GetResult> result = deferredGet;
                        deferredGet = null;
                        deferredGetPredicate = null;
                        return result;
                    }
                    if (heldGetPredicate != null && heldGetPredicate.test(key)) {
                        CompletableFuture<GetResult> result = new CompletableFuture<>();
                        heldGets.add(result);
                        return result;
                    }
                    events.add("get:" + key);
                    return CompletableFuture.completedFuture(currentValue(key));
                }
            });
            when(client.put(anyString(), any(byte[].class))).thenAnswer(invocation -> {
                String key = invocation.getArgument(0);
                byte[] value = invocation.getArgument(1);
                synchronized (this) {
                    events.add("put:" + key);
                    return CompletableFuture.completedFuture(putValue(key, value));
                }
            });
            when(client.put(anyString(), any(byte[].class), anySet())).thenAnswer(invocation -> {
                String key = invocation.getArgument(0);
                byte[] value = invocation.getArgument(1);
                Set<PutOption> options = invocation.getArgument(2);
                synchronized (this) {
                    if (deferredConditionalPut != null && deferredConditionalPutKey.equals(key)) {
                        CompletableFuture<PutResult> result = deferredConditionalPut;
                        deferredConditionalPut = null;
                        deferredConditionalPutKey = null;
                        return result;
                    }
                    Long expectedVersionId = expectedVersionId(options);
                    long currentVersionId = currentVersionId(key);
                    if (expectedVersionId != null && expectedVersionId != currentVersionId) {
                        return CompletableFuture.failedFuture(
                                new UnexpectedVersionIdException(key, expectedVersionId));
                    }
                    events.add("conditional-put:" + key);
                    return CompletableFuture.completedFuture(putValue(key, value));
                }
            });
            when(client.delete(anyString())).thenAnswer(invocation -> {
                String key = invocation.getArgument(0);
                synchronized (this) {
                    events.add("delete:" + key);
                    return CompletableFuture.completedFuture(remove(key));
                }
            });
            when(client.delete(anyString(), anySet())).thenAnswer(invocation -> {
                String key = invocation.getArgument(0);
                Set<DeleteOption> options = invocation.getArgument(1);
                synchronized (this) {
                    Long expectedVersionId = expectedVersionId(options);
                    long currentVersionId = currentVersionId(key);
                    if (expectedVersionId != null && expectedVersionId != currentVersionId) {
                        return CompletableFuture.failedFuture(
                                new UnexpectedVersionIdException(key, expectedVersionId));
                    }
                    events.add("conditional-delete:" + key);
                    return CompletableFuture.completedFuture(remove(key));
                }
            });
            when(client.deleteRange(anyString(), anyString())).thenAnswer(invocation -> {
                String start = invocation.getArgument(0);
                String end = invocation.getArgument(1);
                synchronized (this) {
                    events.add("delete-range:" + start + ":" + end);
                    data.keySet().stream()
                            .filter(key -> key.compareTo(start) >= 0 && key.compareTo(end) < 0)
                            .toList()
                            .forEach(this::remove);
                    return CompletableFuture.completedFuture(null);
                }
            });
            when(client.list(anyString(), anyString())).thenAnswer(invocation -> {
                String start = invocation.getArgument(0);
                String end = invocation.getArgument(1);
                synchronized (this) {
                    if (deferredList != null) {
                        CompletableFuture<List<String>> result = deferredList;
                        deferredList = null;
                        return result;
                    }
                    events.add("list:" + start + ":" + end);
                    return CompletableFuture.completedFuture(data.keySet().stream()
                            .filter(key -> key.compareTo(start) >= 0 && key.compareTo(end) < 0)
                            .sorted()
                            .toList());
                }
            });
            when(client.list(anyString(), anyString(), anySet())).thenAnswer(invocation -> {
                synchronized (this) {
                    events.add("list-index");
                    return CompletableFuture.completedFuture(indexedKeys.stream()
                            .filter(data::containsKey)
                            .toList());
                }
            });
            doAnswer(invocation -> null).when(client).close();
        }

        synchronized void storeValue(String key, byte[] value) {
            putValue(key, value);
        }

        synchronized void indexKeys(List<String> keys) {
            indexedKeys.clear();
            indexedKeys.addAll(keys);
        }

        synchronized CompletableFuture<GetResult> deferNextGet(Predicate<String> predicate) {
            if (deferredGet != null) {
                throw new IllegalStateException("A get is already deferred");
            }
            deferredGetPredicate = predicate;
            deferredGet = new CompletableFuture<>();
            return deferredGet;
        }

        synchronized CompletableFuture<List<String>> deferNextList() {
            if (deferredList != null) {
                throw new IllegalStateException("A list is already deferred");
            }
            deferredList = new CompletableFuture<>();
            return deferredList;
        }

        synchronized CompletableFuture<PutResult> deferNextConditionalPut(String key) {
            if (deferredConditionalPut != null) {
                throw new IllegalStateException("A conditional put is already deferred");
            }
            deferredConditionalPutKey = key;
            deferredConditionalPut = new CompletableFuture<>();
            return deferredConditionalPut;
        }

        synchronized void holdGets(Predicate<String> predicate) {
            heldGetPredicate = predicate;
        }

        synchronized int heldGetCount() {
            return heldGets.size();
        }

        synchronized boolean allHeldGetsCancelled() {
            return heldGets.stream().allMatch(CompletableFuture::isCancelled);
        }

        synchronized boolean contains(String key) {
            return data.containsKey(key);
        }

        synchronized void removeValue(String key) {
            remove(key);
        }

        synchronized void clearEvents() {
            events.clear();
        }

        synchronized List<String> events() {
            return List.copyOf(events);
        }

        AsyncOxiaClient client() {
            return client;
        }

        private PutResult putValue(String key, byte[] value) {
            long versionId = nextVersionId.incrementAndGet();
            data.put(key, value.clone());
            versions.put(key, versionId);
            return new PutResult(key, version(versionId));
        }

        private GetResult currentValue(String key) {
            byte[] value = data.get(key);
            if (value == null) {
                return null;
            }
            return new GetResult(key, value.clone(), version(currentVersionId(key)));
        }

        private long currentVersionId(String key) {
            if (!data.containsKey(key)) {
                return OptionVersionId.KEY_NOT_EXISTS;
            }
            return versions.get(key);
        }

        private boolean remove(String key) {
            boolean removed = data.remove(key) != null;
            versions.remove(key);
            return removed;
        }

        private static Long expectedVersionId(Set<?> options) {
            return options.stream()
                    .filter(OptionVersionId.class::isInstance)
                    .map(OptionVersionId.class::cast)
                    .map(OptionVersionId::versionId)
                    .findFirst()
                    .orElse(null);
        }

        private static Version version(long versionId) {
            return new Version(versionId, 0L, 0L, 0L, Optional.empty(), Optional.empty());
        }
    }
}
