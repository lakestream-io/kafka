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
import org.apache.kafka.storage.diskless.handlers.UrsaStorageConfig;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateSnapshotKeys;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.options.ListOption;

/** Oxia-backed persistence operations for Kafka-owned producer-state snapshots. */
public final class OxiaDisklessProducerStateStore implements DisklessProducerStateStore {
    private static final long CONNECT_TIMEOUT_SECONDS = 10;
    private static final int DELETE_BATCH_SIZE = 32;
    private static final int MAX_DELETE_PASSES = 10;
    private static final byte[] DELETED_TOPIC_MARKER = new byte[]{1};

    private final AsyncOxiaClient client;

    public OxiaDisklessProducerStateStore(UrsaStorageConfig config) throws Exception {
        Objects.requireNonNull(config, "config must not be null");
        this.client = new OxiaServiceUrl(config.getUrsaOxiaServiceUrl())
                .client()
                .get(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    OxiaDisklessProducerStateStore(AsyncOxiaClient client) {
        this.client = Objects.requireNonNull(client, "client must not be null");
    }

    @Override
    public CompletableFuture<Void> deleteTopicSnapshots(Uuid topicId) {
        Objects.requireNonNull(topicId, "topicId must not be null");
        String topicIdString = topicId.toString();
        String topicPrefix = ProducerStateSnapshotKeys.topicSnapshotPrefix(topicIdString);
        String deletedTopicMarkerKey = ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicIdString);
        return client.put(deletedTopicMarkerKey, DELETED_TOPIC_MARKER)
                .thenCompose(ignored -> client.deleteRange(topicPrefix, topicPrefix + '\uffff'))
                .thenCompose(ignored -> deleteTopicSnapshots(topicIdString, topicPrefix, 0));
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

    @Override
    public void close() throws Exception {
        client.close();
    }
}
