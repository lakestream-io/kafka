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

import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import io.streamnative.lakestream.api.ExternalStreamRegistry;
import io.streamnative.lakestream.api.StreamIdentifier;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UrsaDisklessTopicLifecycleTest {

    @Test
    void testDelegatesRegistrationAndUnregistrationToSemanticRegistry() throws Exception {
        ExternalStreamRegistry registry = mock(ExternalStreamRegistry.class);
        Uuid topicId = Uuid.fromString("65WMNfybQpCDVulYOxMCTw");
        StreamIdentifier identifier = UrsaDisklessTopicLifecycle.streamIdentifier("orders", topicId);
        Map<String, String> properties = Map.of("retention.ms", "60000");
        when(registry.registerExternalStream(identifier, 3, properties))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(registry.unregisterExternalStream(identifier))
                .thenReturn(CompletableFuture.completedFuture(null));

        try (UrsaDisklessTopicLifecycle lifecycle = new UrsaDisklessTopicLifecycle(registry)) {
            lifecycle.registerTopic("orders", topicId, 3, properties).get();
            lifecycle.unregisterTopic("orders", topicId).get();
        }

        verify(registry).registerExternalStream(identifier, 3, properties);
        verify(registry).unregisterExternalStream(identifier);
        verify(registry).close();
    }

    @Test
    void testSameNameTopicIncarnationsUseDifferentStreamIdentifiers() {
        StreamIdentifier first = UrsaDisklessTopicLifecycle.streamIdentifier(
                "orders", Uuid.fromString("65WMNfybQpCDVulYOxMCTw"));
        StreamIdentifier second = UrsaDisklessTopicLifecycle.streamIdentifier(
                "orders", Uuid.fromString("VkZ5AkuESPGkMc2OxpKUjw"));

        assertNotEquals(first, second);
    }
}
