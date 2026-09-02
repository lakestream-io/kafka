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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.network.ListenerName;

import java.util.Collections;
import java.util.Map;
import java.util.OptionalInt;
import java.util.function.Function;

public class MetadataCacheDisklessStorageView implements DisklessStorageMetadataView {

    private final Function<String, Map<String, String>> topicConfigSupplier;
    private final Function<ListenerName, Iterable<Node>> aliveBrokerNodesSupplier;
    private final Function<String, Uuid> topicIdSupplier;
    private final Function<String, OptionalInt> partitionCountSupplier;
    private final boolean disklessStorageSystemEnabled;

    public MetadataCacheDisklessStorageView(
            Function<String, Map<String, String>> topicConfigSupplier,
            boolean disklessStorageSystemEnabled) {
        this(
                topicConfigSupplier,
                ln -> Collections.emptyList(),
                t -> Uuid.ZERO_UUID,
                t -> OptionalInt.empty(),
                disklessStorageSystemEnabled);
    }

    public MetadataCacheDisklessStorageView(
            Function<String, Map<String, String>> topicConfigSupplier,
            Function<ListenerName, Iterable<Node>> aliveBrokerNodesSupplier,
            Function<String, Uuid> topicIdSupplier,
            Function<String, OptionalInt> partitionCountSupplier,
            boolean disklessStorageSystemEnabled) {
        this.topicConfigSupplier = topicConfigSupplier;
        this.aliveBrokerNodesSupplier = aliveBrokerNodesSupplier;
        this.topicIdSupplier = topicIdSupplier;
        this.partitionCountSupplier = partitionCountSupplier;
        this.disklessStorageSystemEnabled = disklessStorageSystemEnabled;
    }

    @Override
    public boolean isDisklessStorageTopic(String topic) {
        return disklessStorageSystemEnabled && DisklessTopics.isDiskless(topic, getTopicConfig(topic));
    }

    @Override
    public Map<String, String> getTopicConfig(String topic) {
        Map<String, String> config = topicConfigSupplier.apply(topic);
        return config != null ? config : Map.of();
    }

    @Override
    public Iterable<Node> getAliveBrokerNodes(ListenerName listenerName) {
        Iterable<Node> nodes = aliveBrokerNodesSupplier.apply(listenerName);
        return nodes != null ? nodes : Collections.emptyList();
    }

    @Override
    public Uuid getTopicId(String topicName) {
        Uuid id = topicIdSupplier.apply(topicName);
        return id != null ? id : Uuid.ZERO_UUID;
    }

    @Override
    public OptionalInt partitionCount(String topic) {
        OptionalInt partitionCount = partitionCountSupplier.apply(topic);
        return partitionCount != null ? partitionCount : OptionalInt.empty();
    }
}
