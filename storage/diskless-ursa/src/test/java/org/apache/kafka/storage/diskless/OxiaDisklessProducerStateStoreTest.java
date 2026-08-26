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

import java.util.concurrent.CompletableFuture;

import io.oxia.client.api.AsyncOxiaClient;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OxiaDisklessProducerStateStoreTest {

    @Test
    void testDeleteTopicSnapshotsCoversUnzonedAndZonedKeys() throws Exception {
        Uuid topicId = Uuid.fromString("65WMNfybQpCDVulYOxMCTw");
        String prefix = ProducerStateSnapshotKeys.topicSnapshotPrefix(topicId.toString());
        String rangeEnd = prefix + '\uffff';
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        when(client.deleteRange(prefix, rangeEnd)).thenReturn(CompletableFuture.completedFuture(null));

        try (OxiaDisklessProducerStateStore store = new OxiaDisklessProducerStateStore(client)) {
            store.deleteTopicSnapshots(topicId).get();
        }

        String unzonedKey = ProducerStateSnapshotKeys.snapshotKey(topicId.toString(), 3);
        String zonedKey = ProducerStateSnapshotKeys.snapshotKey(topicId.toString(), 3, "us-west-2a");
        String otherTopicKey = ProducerStateSnapshotKeys.snapshotKey(
                Uuid.fromString("VkZ5AkuESPGkMc2OxpKUjw").toString(), 3, "us-west-2a");
        assertTrue(inRange(unzonedKey, prefix, rangeEnd));
        assertTrue(inRange(zonedKey, prefix, rangeEnd));
        assertFalse(inRange(otherTopicKey, prefix, rangeEnd));
        verify(client).deleteRange(prefix, rangeEnd);
        verify(client).close();
    }

    private static boolean inRange(String key, String startInclusive, String endExclusive) {
        return key.compareTo(startInclusive) >= 0 && key.compareTo(endExclusive) < 0;
    }
}
