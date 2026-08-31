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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaLogNamingTest {

    @Test
    void testLogName() {
        Uuid topicId = Uuid.randomUuid();
        TopicIdPartition tp = new TopicIdPartition(topicId, new TopicPartition("test-topic", 2));
        assertEquals(
                "default/test-topic-topic-id-" + topicId + "-partition-2",
                KafkaLogNaming.logName(tp)
        );
    }

    @Test
    void testStreamNameIncludesTopicIncarnation() {
        Uuid topicId = Uuid.randomUuid();
        TopicIdPartition tp = new TopicIdPartition(topicId, new TopicPartition("test-topic", 0));
        assertEquals(
                "test-topic-topic-id-" + topicId,
                KafkaLogNaming.streamName(tp)
        );
    }

    @Test
    void testSameNameRecreatedTopicUsesDifferentStreamAndLogNames() {
        TopicPartition partition = new TopicPartition("recreated-topic", 0);
        TopicIdPartition deleted = new TopicIdPartition(Uuid.randomUuid(), partition);
        TopicIdPartition recreated = new TopicIdPartition(Uuid.randomUuid(), partition);

        assertNotEquals(KafkaLogNaming.streamName(deleted), KafkaLogNaming.streamName(recreated));
        assertNotEquals(KafkaLogNaming.logName(deleted), KafkaLogNaming.logName(recreated));
    }

    @Test
    void testUnknownTopicIncarnationIsRejected() {
        TopicIdPartition tp = new TopicIdPartition(Uuid.ZERO_UUID, new TopicPartition("test-topic", 0));

        assertThrows(IllegalArgumentException.class, () -> KafkaLogNaming.logName(tp));
    }

    @Test
    void testStreamPropertiesPreserveLogicalKafkaTopicName() {
        Map<String, String> properties = KafkaLogNaming.streamProperties(
                "orders", Map.of("retention.ms", "60000"));

        assertEquals("orders", properties.get(KafkaLogNaming.KAFKA_TOPIC_NAME_PROPERTY));
        assertEquals("60000", properties.get("retention.ms"));
    }
}
