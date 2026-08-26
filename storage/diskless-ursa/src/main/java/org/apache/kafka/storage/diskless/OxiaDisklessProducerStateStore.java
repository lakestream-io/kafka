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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.oxia.client.api.AsyncOxiaClient;

/** Oxia-backed persistence operations for Kafka-owned producer-state snapshots. */
public final class OxiaDisklessProducerStateStore implements DisklessProducerStateStore {
    private static final long CONNECT_TIMEOUT_SECONDS = 10;

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
        String prefix = ProducerStateSnapshotKeys.topicSnapshotPrefix(topicId.toString());
        return client.deleteRange(prefix, prefix + '\uffff');
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
