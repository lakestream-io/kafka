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
package kafka.server;

import kafka.cluster.Partition;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.storage.internals.log.LogAppendInfo;

/**
 * Intercepts successful local log appends to trigger optional SDT side effects.
 */
public interface ReplicaManagerInterceptor extends AutoCloseable {

    // this method shouldn't do any blocking operation
    void onAppend(MemoryRecords records, LogAppendInfo appendInfo, Partition partition);

    default void onAppend(
            MemoryRecords records,
            LogAppendInfo appendInfo,
            Partition partition,
            long publisherGeneration) {
        onAppend(records, appendInfo, partition);
    }

    /**
     * Called after a classic partition becomes leader, even when no subsequent append occurs.
     * Implementations must not block the metadata-application thread.
     */
    default void onLeadershipAcquired(Partition partition, long publisherGeneration) {
    }

    /**
     * Called after a classic partition loses local leadership.
     * Implementations must fence partition-scoped background work without blocking the
     * metadata-application thread.
     */
    default void onLeadershipLost(TopicIdPartition topicIdPartition) {
    }

    /**
     * Called after a diskless append is durable and its storage-native log metadata is available.
     * Implementations must not block the completion thread.
     */
    default void onDisklessAppend(
            TopicIdPartition topicIdPartition,
            long streamId,
            long highWatermark) {
    }

    default void onDisklessAppend(
            TopicIdPartition topicIdPartition,
            long streamId,
            long highWatermark,
            long ownershipGeneration) {
        onDisklessAppend(topicIdPartition, streamId, highWatermark);
    }

    /**
     * Called before this broker releases storage ownership for a partition.
     * Implementations must fence any partition-scoped background work before returning.
     */
    default void onPartitionOwnershipLost(TopicIdPartition topicIdPartition) {
    }

    default void onPartitionOwnershipLost(
            TopicIdPartition topicIdPartition,
            long ownershipGeneration) {
        onPartitionOwnershipLost(topicIdPartition);
    }

}
