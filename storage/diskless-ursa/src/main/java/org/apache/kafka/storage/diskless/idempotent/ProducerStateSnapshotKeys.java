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
package org.apache.kafka.storage.diskless.idempotent;

import org.apache.kafka.storage.diskless.DisklessClientZone;

/** Oxia key builder for diskless producer-state snapshots. */
public final class ProducerStateSnapshotKeys {

    private static final String SNAPSHOT_PREFIX = "producer-state-snapshot/";
    private static final String DELETED_TOPIC_PREFIX = "producer-state-snapshot-deleted/";
    private static final String TOPIC_INDEX_NAME = "producer-state-snapshot-topic";

    private ProducerStateSnapshotKeys() {
    }

    public static String snapshotKey(String topicId, int partition) {
        return topicSnapshotPrefix(topicId) + partition;
    }

    /** Prefix shared by every unzoned and zoned snapshot for one exact topic incarnation. */
    public static String topicSnapshotPrefix(String topicId) {
        return SNAPSHOT_PREFIX + topicId + "-";
    }

    /** Durable marker that prevents a deleted topic incarnation from recreating snapshots. */
    public static String deletedTopicMarkerKey(String topicId) {
        return DELETED_TOPIC_PREFIX + topicId;
    }

    /** Prefix shared by every durable deleted-topic marker. */
    public static String deletedTopicMarkerPrefix() {
        return DELETED_TOPIC_PREFIX;
    }

    /** Exclusive upper bound used to enumerate every durable deleted-topic marker. */
    public static String deletedTopicMarkerEndExclusive() {
        return DELETED_TOPIC_PREFIX + '\uffff';
    }

    /** Oxia secondary index used to enumerate snapshots for one topic without a namespace scan. */
    public static String topicIndexName() {
        return TOPIC_INDEX_NAME;
    }

    public static String topicIndexKey(String topicId) {
        return topicId;
    }

    /** Exclusive upper bound that includes every entry with the exact topic ID index key. */
    public static String topicIndexEndExclusive(String topicId) {
        return topicId + '\uffff';
    }

    public static String snapshotKey(String topicId, int partition, String zone) {
        String legacyKey = snapshotKey(topicId, partition);
        if (DisklessClientZone.NO_ZONE.equals(zone)) {
            return legacyKey;
        }
        return legacyKey + "/" + zone;
    }
}
