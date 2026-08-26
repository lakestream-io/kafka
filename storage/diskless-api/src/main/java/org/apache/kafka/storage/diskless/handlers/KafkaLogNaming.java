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

/**
 * Stable Lakestream catalog naming for Kafka diskless partition logs.
 *
 * <p>The namespace components and legacy Oxia metadata prefix are persisted compatibility contracts.
 * Changing them would make existing partition logs appear missing.
 */
public final class KafkaLogNaming {

    public static final String TENANT = "public";
    public static final String NAMESPACE = "default";
    public static final String DOMAIN = "persistent";

    public static final String LEGACY_CATALOG_METADATA_PREFIX = "/managed-ledgers/";

    private KafkaLogNaming() {
    }

    /**
     * Stable key passed to the Lakestream stream-ID generator.
     */
    public static String logName(TopicIdPartition tp) {
        return logName(tp.topic(), tp.partition());
    }

    public static String logName(String topic, int partition) {
        return TENANT + "/" + NAMESPACE + "/" + DOMAIN + "/" + topic + "-partition-" + partition;
    }

    /**
     * Oxia catalog metadata key for the partition log.
     */
    public static String logMetadataPath(TopicIdPartition tp) {
        return LEGACY_CATALOG_METADATA_PREFIX + logName(tp);
    }
}
