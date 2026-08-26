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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;

import java.util.Objects;

/**
 * Stable Lakestream catalog naming for Kafka diskless partition logs.
 */
public final class KafkaLogNaming {

    public static final String NAMESPACE = "default";

    private KafkaLogNaming() {
    }

    /**
     * Stable catalog name for one Kafka topic incarnation.
     *
     * <p>The topic ID is intentionally part of the name. Kafka permits a deleted topic to be
     * recreated with the same name, and cleanup of the deleted topic is best effort. A name-only
     * key would let the recreated topic attach to the deleted incarnation's log when that cleanup
     * failed.
     */
    public static String streamName(TopicIdPartition tp) {
        Objects.requireNonNull(tp, "topicIdPartition must not be null");
        if (Uuid.ZERO_UUID.equals(tp.topicId())) {
            throw new IllegalArgumentException("topicIdPartition must contain a non-zero topic ID");
        }
        return tp.topic() + "-topic-id-" + tp.topicId();
    }

    /** Stable physical partition name used by the existing SDT publication contract. */
    public static String logName(TopicIdPartition tp) {
        return NAMESPACE + "/" + partitionName(tp);
    }

    /** Physical partition-name component used by {@link #logName}. */
    public static String partitionName(TopicIdPartition tp) {
        return streamName(tp) + "-partition-" + tp.partition();
    }

}
