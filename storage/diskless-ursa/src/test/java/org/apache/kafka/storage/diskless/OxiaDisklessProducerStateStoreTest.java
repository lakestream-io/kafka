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
import org.apache.kafka.storage.diskless.idempotent.ProducerStateSnapshotKeys;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.options.ListOption;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class OxiaDisklessProducerStateStoreTest {

    @Test
    void testDeleteTopicSnapshotsFiltersAndDeletesKeysAtAnyDepth() throws Exception {
        Uuid topicId = Uuid.fromString("65WMNfybQpCDVulYOxMCTw");
        String topicPrefix = ProducerStateSnapshotKeys.topicSnapshotPrefix(topicId.toString());
        String deletedTopicMarkerKey = ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicId.toString());
        String unzonedKey = ProducerStateSnapshotKeys.snapshotKey(topicId.toString(), 0);
        String zonedKey = ProducerStateSnapshotKeys.snapshotKey(topicId.toString(), 1, "rack/region/zone");
        String otherTopicKey = ProducerStateSnapshotKeys.snapshotKey(
                Uuid.fromString("VkZ5AkuESPGkMc2OxpKUjw").toString(), 0, "rack/region/zone");
        String malformedTopicScopedKey = topicPrefix + "metadata";
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        when(client.put(eq(deletedTopicMarkerKey), any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(mock(PutResult.class)));
        when(client.deleteRange(topicPrefix, topicPrefix + '\uffff'))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(client.list(
                ProducerStateSnapshotKeys.topicIndexKey(topicId.toString()),
                ProducerStateSnapshotKeys.topicIndexEndExclusive(topicId.toString()),
                Set.of(ListOption.UseIndex(ProducerStateSnapshotKeys.topicIndexName()))))
                .thenReturn(CompletableFuture.completedFuture(
                        List.of(unzonedKey, otherTopicKey, malformedTopicScopedKey, zonedKey)))
                .thenReturn(CompletableFuture.completedFuture(List.of()));
        when(client.delete(unzonedKey)).thenReturn(CompletableFuture.completedFuture(true));
        when(client.delete(zonedKey)).thenReturn(CompletableFuture.completedFuture(true));

        try (OxiaDisklessProducerStateStore store = new OxiaDisklessProducerStateStore(client)) {
            store.deleteTopicSnapshots(topicId).get();
        }

        verify(client).put(eq(deletedTopicMarkerKey), any(byte[].class));
        verify(client).deleteRange(topicPrefix, topicPrefix + '\uffff');
        verify(client, times(2)).list(
                ProducerStateSnapshotKeys.topicIndexKey(topicId.toString()),
                ProducerStateSnapshotKeys.topicIndexEndExclusive(topicId.toString()),
                Set.of(ListOption.UseIndex(ProducerStateSnapshotKeys.topicIndexName())));
        verify(client).delete(unzonedKey);
        verify(client).delete(zonedKey);
        verify(client, never()).delete(otherTopicKey);
        verify(client, never()).delete(malformedTopicScopedKey);
        verify(client).close();
        verifyNoMoreInteractions(client);
    }
}
