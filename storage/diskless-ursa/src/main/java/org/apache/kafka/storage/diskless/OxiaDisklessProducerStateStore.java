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
import org.apache.kafka.storage.diskless.DisklessProducerStateStore.ManagedProducerStateTopic;
import org.apache.kafka.storage.diskless.handlers.UrsaStorageConfig;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateSnapshotKeys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.ListOption;
import io.oxia.client.api.options.PutOption;

/** Oxia-backed persistence operations for Kafka-owned producer-state snapshots. */
public final class OxiaDisklessProducerStateStore implements DisklessProducerStateStore {
    private static final Logger log = LoggerFactory.getLogger(OxiaDisklessProducerStateStore.class);
    private static final long CONNECT_TIMEOUT_SECONDS = 10;
    private static final long INVENTORY_READ_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(10);
    private static final int DELETE_BATCH_SIZE = 32;
    private static final int INVENTORY_READ_BATCH_SIZE = 32;
    private static final int MAX_DELETE_PASSES = 10;
    private static final int MAX_MANIFEST_CAS_ATTEMPTS = 32;
    private static final String MANAGED_TOPIC_PREFIX = "producer-state-managed-topic/";
    private static final int MANIFEST_MAGIC = 0x4b50534d;
    private static final short MANIFEST_VERSION = 1;
    private static final int MAX_TOPIC_NAME_BYTES = 1024 * 1024;
    private static final String DELETED_TOPIC_NAME_PREFIX = "deleted-producer-state-";

    private final AsyncOxiaClient client;
    private final long inventoryReadTimeoutMs;

    public OxiaDisklessProducerStateStore(UrsaStorageConfig config) throws Exception {
        Objects.requireNonNull(config, "config must not be null");
        this.client = new OxiaServiceUrl(config.getUrsaOxiaServiceUrl())
                .client()
                .get(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        this.inventoryReadTimeoutMs = INVENTORY_READ_TIMEOUT_MILLIS;
    }

    OxiaDisklessProducerStateStore(AsyncOxiaClient client) {
        this(client, INVENTORY_READ_TIMEOUT_MILLIS);
    }

    OxiaDisklessProducerStateStore(AsyncOxiaClient client, long inventoryReadTimeoutMs) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        if (inventoryReadTimeoutMs <= 0) {
            throw new IllegalArgumentException("inventoryReadTimeoutMs must be positive");
        }
        this.inventoryReadTimeoutMs = inventoryReadTimeoutMs;
    }

    @Override
    public CompletableFuture<Void> reconcileTopic(String topicName, Uuid topicId, long sourceRevision) {
        ManagedProducerStateTopic desired = new ManagedProducerStateTopic(
                topicName, topicId, sourceRevision);
        return reconcileTopic(desired, MAX_MANIFEST_CAS_ATTEMPTS);
    }

    private CompletableFuture<Void> reconcileTopic(
            ManagedProducerStateTopic desired,
            int attemptsRemaining
    ) {
        if (attemptsRemaining == 0) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "Producer-state ownership manifest kept changing for topic " + desired.topicId()));
        }
        String topicId = desired.topicId().toString();
        String markerKey = ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicId);
        String manifestKey = managedTopicKey(desired.topicId());
        return client.get(markerKey).thenCompose(markerBeforeReconcile -> {
            if (markerBeforeReconcile != null) {
                return deletedTopicFailure(desired.topicId());
            }
            return client.get(manifestKey).thenCompose(existing -> {
                if (existing == null) {
                    return putManifest(
                            desired,
                            manifestKey,
                            markerKey,
                            PutOption.IfRecordDoesNotExist,
                            attemptsRemaining);
                }
                ManagedProducerStateTopic current = deserializeManifest(existing.value());
                requireManifestIdentity(manifestKey, desired.topicId(), current);
                if (!current.topicName().equals(desired.topicName())) {
                    return CompletableFuture.failedFuture(new IllegalStateException(
                            "Kafka topic ID " + desired.topicId() + " is already owned by topic "
                                    + current.topicName() + ", not " + desired.topicName()));
                }
                if (current.sourceRevision() >= desired.sourceRevision()) {
                    return verifyNoDeletionFence(markerKey, desired.topicId());
                }
                long versionId = requireVersionId(existing);
                return putManifest(
                        desired,
                        manifestKey,
                        markerKey,
                        PutOption.IfVersionIdEquals(versionId),
                        attemptsRemaining);
            });
        });
    }

    private CompletableFuture<Void> putManifest(
            ManagedProducerStateTopic desired,
            String manifestKey,
            String markerKey,
            PutOption condition,
            int attemptsRemaining
    ) {
        return client.put(manifestKey, serializeManifest(desired), Set.of(condition))
                .handle((putResult, putError) -> {
                    if (putError != null) {
                        Throwable failure = unwrapCompletionException(putError);
                        if (isConditionalConflict(failure)) {
                            return reconcileTopic(desired, attemptsRemaining - 1);
                        }
                        return CompletableFuture.<Void>failedFuture(failure);
                    }
                    long writtenVersionId;
                    try {
                        writtenVersionId = requireVersionId(putResult);
                    } catch (Throwable failure) {
                        return CompletableFuture.<Void>failedFuture(failure);
                    }
                    return verifyManifestNotDeleted(
                            desired.topicId(), manifestKey, markerKey, writtenVersionId);
                })
                .thenCompose(result -> result);
    }

    private CompletableFuture<Void> verifyNoDeletionFence(String markerKey, Uuid topicId) {
        return client.get(markerKey).thenCompose(marker -> marker == null
                ? CompletableFuture.completedFuture(null)
                : deletedTopicFailure(topicId));
    }

    private CompletableFuture<Void> verifyManifestNotDeleted(
            Uuid topicId,
            String manifestKey,
            String markerKey,
            long writtenVersionId
    ) {
        return client.get(markerKey)
                .<CompletableFuture<Void>>handle((markerAfterWrite, markerError) -> {
                    if (markerError == null && markerAfterWrite == null) {
                        return CompletableFuture.<Void>completedFuture(null);
                    }
                    return client.delete(
                            manifestKey,
                            Set.of(DeleteOption.IfVersionIdEquals(writtenVersionId)))
                            .<Void>handle((ignored, deleteError) -> {
                                Throwable cleanupFailure = deleteError == null
                                        ? null
                                        : unwrapCompletionException(deleteError);
                                if (cleanupFailure != null
                                        && !(cleanupFailure instanceof UnexpectedVersionIdException)) {
                                    if (markerError != null) {
                                        unwrapCompletionException(markerError).addSuppressed(cleanupFailure);
                                    } else {
                                        throw new CompletionException(cleanupFailure);
                                    }
                                }
                                if (markerError != null) {
                                    throw new CompletionException(unwrapCompletionException(markerError));
                                }
                                throw new CompletionException(deletedTopicException(topicId));
                            });
                })
                .thenCompose(result -> result);
    }

    @Override
    public CompletableFuture<List<ManagedProducerStateTopic>> listManagedTopics() {
        return new ManagedTopicInventory().start();
    }

    private static ManagedProducerStateTopic readManifest(String key, GetResult result) {
        if (result == null) {
            return null;
        }
        ManagedProducerStateTopic topic = deserializeManifest(result.value());
        requireManifestIdentity(key, topic.topicId(), topic);
        return topic;
    }

    @Override
    public CompletableFuture<Void> deleteTopicSnapshots(Uuid topicId) {
        Objects.requireNonNull(topicId, "topicId must not be null");
        String topicIdString = topicId.toString();
        String topicPrefix = ProducerStateSnapshotKeys.topicSnapshotPrefix(topicIdString);
        String deletedTopicMarkerKey = ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicIdString);
        return putDeletionJournalIfAbsent(topicId, deletedTopicMarkerKey)
                .thenCompose(ignored -> client.deleteRange(topicPrefix, topicPrefix + '\uffff'))
                .thenCompose(ignored -> deleteTopicSnapshots(topicIdString, topicPrefix, 0))
                .thenCompose(ignored -> moveManifestToDeletionJournal(
                        topicId, MAX_MANIFEST_CAS_ATTEMPTS));
    }

    private CompletableFuture<Void> putDeletionJournalIfAbsent(
            Uuid topicId,
            String deletedTopicMarkerKey
    ) {
        byte[] fallbackJournal = serializeManifest(deletedTopicJournal(topicId));
        return client.put(
                deletedTopicMarkerKey,
                fallbackJournal,
                Set.of(PutOption.IfRecordDoesNotExist))
                .handle((ignored, error) -> {
                    if (error == null) {
                        return null;
                    }
                    Throwable failure = unwrapCompletionException(error);
                    if (isConditionalConflict(failure)) {
                        return null;
                    }
                    throw new CompletionException(failure);
                });
    }

    private CompletableFuture<Void> deleteTopicSnapshots(
            String topicId,
            String topicPrefix,
            int completedPasses
    ) {
        return client.list(
                ProducerStateSnapshotKeys.topicIndexKey(topicId),
                ProducerStateSnapshotKeys.topicIndexEndExclusive(topicId),
                Set.of(ListOption.UseIndex(ProducerStateSnapshotKeys.topicIndexName()))
        ).thenCompose(keys -> {
            List<String> topicKeys = keys.stream()
                    .filter(key -> isTopicSnapshotKey(key, topicPrefix))
                    .toList();
            if (topicKeys.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            if (completedPasses >= MAX_DELETE_PASSES) {
                return CompletableFuture.failedFuture(new IllegalStateException(
                        "Producer-state snapshots are still being written for deleted topic prefix " + topicPrefix));
            }
            return deleteKeys(topicKeys)
                    .thenCompose(ignored -> deleteTopicSnapshots(topicId, topicPrefix, completedPasses + 1));
        });
    }

    private CompletableFuture<Void> deleteKeys(List<String> keys) {
        CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
        for (int start = 0; start < keys.size(); start += DELETE_BATCH_SIZE) {
            List<String> batch = keys.subList(start, Math.min(start + DELETE_BATCH_SIZE, keys.size()));
            result = result.thenCompose(ignored -> CompletableFuture.allOf(
                    batch.stream().map(client::delete).toArray(CompletableFuture[]::new)));
        }
        return result;
    }

    private CompletableFuture<Void> moveManifestToDeletionJournal(
            Uuid topicId,
            int attemptsRemaining
    ) {
        if (attemptsRemaining == 0) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "Producer-state ownership manifest kept changing while deleting topic " + topicId));
        }
        String manifestKey = managedTopicKey(topicId);
        String journalKey = ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicId.toString());
        return client.get(manifestKey).thenCompose(existing -> {
            if (existing == null) {
                return CompletableFuture.completedFuture(null);
            }
            long versionId = requireVersionId(existing);
            byte[] candidateJournalValue;
            try {
                ManagedProducerStateTopic current = deserializeManifest(existing.value());
                requireManifestIdentity(manifestKey, topicId, current);
                candidateJournalValue = serializeManifest(current);
            } catch (RuntimeException malformedManifest) {
                log.warn(
                        "Removing malformed producer-state ownership manifest {} after its deletion "
                                + "journal was durably fenced",
                        manifestKey,
                        malformedManifest);
                candidateJournalValue = serializeManifest(deletedTopicJournal(topicId));
            }
            byte[] journalValue = candidateJournalValue;
            return client.put(journalKey, journalValue)
                    .thenCompose(ignored -> client.delete(
                            manifestKey,
                            Set.of(DeleteOption.IfVersionIdEquals(versionId))))
                    .handle((ignored, deleteError) -> {
                        if (deleteError == null) {
                            return CompletableFuture.<Void>completedFuture(null);
                        }
                        Throwable failure = unwrapCompletionException(deleteError);
                        if (failure instanceof UnexpectedVersionIdException) {
                            return moveManifestToDeletionJournal(topicId, attemptsRemaining - 1);
                        }
                        return CompletableFuture.<Void>failedFuture(failure);
                    })
                    .thenCompose(result -> result);
        });
    }

    private final class ManagedTopicInventory {
        private final CompletableFuture<List<ManagedProducerStateTopic>> result = new CompletableFuture<>();
        private final Set<CompletableFuture<?>> sourceReads = ConcurrentHashMap.newKeySet();

        private CompletableFuture<List<ManagedProducerStateTopic>> start() {
            result.whenComplete((ignored, error) -> {
                if (error != null) {
                    cancelSourceReads();
                }
            });
            result.orTimeout(inventoryReadTimeoutMs, TimeUnit.MILLISECONDS);

            CompletableFuture<List<String>> manifestKeys = listKeys(
                    MANAGED_TOPIC_PREFIX,
                    MANAGED_TOPIC_PREFIX + '\uffff');
            CompletableFuture<List<String>> deletionJournalKeys = listKeys(
                    ProducerStateSnapshotKeys.deletedTopicMarkerPrefix(),
                    ProducerStateSnapshotKeys.deletedTopicMarkerEndExclusive());
            CompletableFuture.allOf(manifestKeys, deletionJournalKeys)
                    .whenComplete((ignored, listError) -> {
                        if (listError != null) {
                            fail(listError);
                            return;
                        }
                        if (!result.isDone()) {
                            readInventoryEntries(manifestKeys.join(), deletionJournalKeys.join());
                        }
                    });
            return result;
        }

        private CompletableFuture<List<String>> listKeys(String startInclusive, String endExclusive) {
            try {
                return trackSource(Objects.requireNonNull(
                        client.list(startInclusive, endExclusive),
                        "Oxia inventory list returned null future"));
            } catch (RuntimeException error) {
                return CompletableFuture.failedFuture(error);
            }
        }

        private void readInventoryEntries(
                List<String> manifestKeys,
                List<String> deletionJournalKeys
        ) {
            List<InventoryKey> inventoryKeys = new ArrayList<>(
                    manifestKeys.size() + deletionJournalKeys.size());
            manifestKeys.forEach(key -> inventoryKeys.add(new InventoryKey(key, false)));
            deletionJournalKeys.forEach(key -> inventoryKeys.add(new InventoryKey(key, true)));

            CompletableFuture<Map<Uuid, ManagedProducerStateTopic>> batches =
                    CompletableFuture.completedFuture(new HashMap<>());
            for (int start = 0; start < inventoryKeys.size(); start += INVENTORY_READ_BATCH_SIZE) {
                List<InventoryKey> batch = inventoryKeys.subList(
                        start,
                        Math.min(start + INVENTORY_READ_BATCH_SIZE, inventoryKeys.size()));
                batches = batches.thenCompose(topicsById -> readInventoryBatch(batch, topicsById));
            }
            batches.whenComplete((topicsById, readError) -> {
                if (readError != null) {
                    if (!result.isDone()) {
                        fail(readError);
                    }
                    return;
                }
                result.complete(topicsById.values().stream()
                        .sorted(Comparator.comparing(ManagedProducerStateTopic::topicName)
                                .thenComparing(topic -> topic.topicId().toString()))
                        .toList());
            });
        }

        private CompletableFuture<Map<Uuid, ManagedProducerStateTopic>> readInventoryBatch(
                List<InventoryKey> batch,
                Map<Uuid, ManagedProducerStateTopic> topicsById
        ) {
            if (result.isDone()) {
                return CompletableFuture.failedFuture(new CancellationException(
                        "Producer-state inventory is no longer active"));
            }
            List<CompletableFuture<ManagedProducerStateTopic>> reads = batch.stream()
                    .map(inventoryKey -> readInventoryEntry(
                            inventoryKey.key(), inventoryKey.deletionJournal()))
                    .toList();
            return CompletableFuture.allOf(reads.toArray(CompletableFuture[]::new))
                    .thenApply(ignored -> {
                        for (CompletableFuture<ManagedProducerStateTopic> read : reads) {
                            ManagedProducerStateTopic topic = read.join();
                            if (topic != null) {
                                topicsById.merge(
                                        topic.topicId(),
                                        topic,
                                        OxiaDisklessProducerStateStore::preferInventoryEntry);
                            }
                        }
                        return topicsById;
                    });
        }

        private CompletableFuture<ManagedProducerStateTopic> readInventoryEntry(
                String key,
                boolean deletionJournal
        ) {
            CompletableFuture<GetResult> source;
            try {
                source = trackSource(Objects.requireNonNull(
                        client.get(key),
                        "Oxia inventory get returned null future"));
            } catch (RuntimeException error) {
                return CompletableFuture.failedFuture(error);
            }
            return source.thenApply(getResult -> deletionJournal
                    ? readDeletionJournal(key, getResult)
                    : readManifestIsolatingCorruption(key, getResult));
        }

        private <T> CompletableFuture<T> trackSource(CompletableFuture<T> source) {
            sourceReads.add(source);
            if (result.isDone()) {
                source.cancel(true);
            }
            source.whenComplete((ignored, error) -> sourceReads.remove(source));
            return source;
        }

        private void fail(Throwable error) {
            result.completeExceptionally(unwrapCompletionException(error));
        }

        private void cancelSourceReads() {
            sourceReads.forEach(source -> source.cancel(true));
        }
    }

    private record InventoryKey(String key, boolean deletionJournal) {
    }

    private static ManagedProducerStateTopic readManifestIsolatingCorruption(
            String key,
            GetResult result
    ) {
        try {
            return readManifest(key, result);
        } catch (RuntimeException malformedManifest) {
            log.warn(
                    "Skipping malformed producer-state ownership manifest {}; other inventory "
                            + "entries will still be reconciled",
                    key,
                    malformedManifest);
            return null;
        }
    }

    private static ManagedProducerStateTopic readDeletionJournal(String key, GetResult result) {
        if (result == null) {
            return null;
        }
        String prefix = ProducerStateSnapshotKeys.deletedTopicMarkerPrefix();
        Uuid topicId;
        try {
            topicId = Uuid.fromString(key.substring(prefix.length()));
        } catch (RuntimeException malformedKey) {
            log.warn(
                    "Skipping malformed producer-state deletion journal key {}; other inventory "
                            + "entries will still be reconciled",
                    key,
                    malformedKey);
            return null;
        }
        try {
            ManagedProducerStateTopic topic = deserializeManifest(result.value());
            if (!topicId.equals(topic.topicId())) {
                throw new IllegalStateException(
                        "Producer-state deletion journal identity does not match key " + key);
            }
            return topic;
        } catch (RuntimeException malformedJournal) {
            log.warn(
                    "Using the topic ID from malformed producer-state deletion journal {}; the "
                            + "durable fence remains discoverable",
                    key,
                    malformedJournal);
            return deletedTopicJournal(topicId);
        }
    }

    private static ManagedProducerStateTopic preferInventoryEntry(
            ManagedProducerStateTopic first,
            ManagedProducerStateTopic second
    ) {
        if (second.sourceRevision() != first.sourceRevision()) {
            return second.sourceRevision() > first.sourceRevision() ? second : first;
        }
        boolean firstIsFallback = first.topicName().startsWith(DELETED_TOPIC_NAME_PREFIX);
        boolean secondIsFallback = second.topicName().startsWith(DELETED_TOPIC_NAME_PREFIX);
        return firstIsFallback && !secondIsFallback ? second : first;
    }

    private static ManagedProducerStateTopic deletedTopicJournal(Uuid topicId) {
        return new ManagedProducerStateTopic(DELETED_TOPIC_NAME_PREFIX + topicId, topicId, 0);
    }

    private static boolean isTopicSnapshotKey(String key, String topicPrefix) {
        if (!key.startsWith(topicPrefix)) {
            return false;
        }
        int index = topicPrefix.length();
        int partitionStart = index;
        while (index < key.length() && key.charAt(index) >= '0' && key.charAt(index) <= '9') {
            index++;
        }
        return index > partitionStart && (index == key.length() || key.charAt(index) == '/');
    }

    private static String managedTopicKey(Uuid topicId) {
        return MANAGED_TOPIC_PREFIX + topicId;
    }

    private static byte[] serializeManifest(ManagedProducerStateTopic topic) {
        byte[] topicName = topic.topicName().getBytes(StandardCharsets.UTF_8);
        if (topicName.length > MAX_TOPIC_NAME_BYTES) {
            throw new IllegalArgumentException("Topic name is too large to persist");
        }
        return ByteBuffer.allocate(
                        Integer.BYTES
                                + Short.BYTES
                                + Long.BYTES * 3
                                + Integer.BYTES
                                + topicName.length)
                .putInt(MANIFEST_MAGIC)
                .putShort(MANIFEST_VERSION)
                .putLong(topic.topicId().getMostSignificantBits())
                .putLong(topic.topicId().getLeastSignificantBits())
                .putLong(topic.sourceRevision())
                .putInt(topicName.length)
                .put(topicName)
                .array();
    }

    private static ManagedProducerStateTopic deserializeManifest(byte[] value) {
        int fixedBytes = Integer.BYTES + Short.BYTES + Long.BYTES * 3 + Integer.BYTES;
        if (value == null || value.length < fixedBytes) {
            throw new IllegalStateException("Invalid producer-state ownership manifest");
        }
        ByteBuffer buffer = ByteBuffer.wrap(value);
        int magic = buffer.getInt();
        short version = buffer.getShort();
        if (magic != MANIFEST_MAGIC || version != MANIFEST_VERSION) {
            throw new IllegalStateException("Unsupported producer-state ownership manifest format");
        }
        Uuid topicId = new Uuid(buffer.getLong(), buffer.getLong());
        long sourceRevision = buffer.getLong();
        int topicNameLength = buffer.getInt();
        if (topicNameLength < 0
                || topicNameLength > MAX_TOPIC_NAME_BYTES
                || topicNameLength != buffer.remaining()) {
            throw new IllegalStateException("Invalid topic name in producer-state ownership manifest");
        }
        byte[] topicName = new byte[topicNameLength];
        buffer.get(topicName);
        try {
            return new ManagedProducerStateTopic(
                    new String(topicName, StandardCharsets.UTF_8), topicId, sourceRevision);
        } catch (IllegalArgumentException error) {
            throw new IllegalStateException("Invalid producer-state ownership manifest", error);
        }
    }

    private static void requireManifestIdentity(
            String manifestKey,
            Uuid expectedTopicId,
            ManagedProducerStateTopic topic
    ) {
        if (!expectedTopicId.equals(topic.topicId()) || !managedTopicKey(topic.topicId()).equals(manifestKey)) {
            throw new IllegalStateException(
                    "Producer-state ownership manifest identity does not match key " + manifestKey);
        }
    }

    private static long requireVersionId(GetResult result) {
        if (result.version() == null) {
            throw new IllegalStateException("Oxia get result did not include a record version");
        }
        return result.version().versionId();
    }

    private static long requireVersionId(PutResult result) {
        if (result == null || result.version() == null) {
            throw new IllegalStateException("Oxia put result did not include a record version");
        }
        return result.version().versionId();
    }

    private static boolean isConditionalConflict(Throwable error) {
        return error instanceof UnexpectedVersionIdException || error instanceof KeyAlreadyExistsException;
    }

    private static <T> CompletableFuture<T> deletedTopicFailure(Uuid topicId) {
        return CompletableFuture.failedFuture(deletedTopicException(topicId));
    }

    private static IllegalStateException deletedTopicException(Uuid topicId) {
        return new IllegalStateException("Producer state was permanently deleted for topic " + topicId);
    }

    private static Throwable unwrapCompletionException(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
