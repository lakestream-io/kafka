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

import org.apache.kafka.server.config.ServerLogConfigs;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Covers the consolidated Oxia URL configuration surface: {@code ursa.catalog.oxia.service.url}
 * and {@code ursa.oxia.service.url} are the only two knobs left after
 * {@code ursa.storage.oxia.service.url} and {@code ursa.storage.namespace} were removed.
 */
class UrsaStorageConfigTest {

    @Test
    void testCatalogOxiaServiceUrlDefault() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of());

        assertEquals(ServerLogConfigs.URSA_CATALOG_OXIA_SERVICE_URL_DEFAULT, config.getCatalogOxiaServiceUrl());
    }

    @Test
    void testUrsaOxiaServiceUrlDefault() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of());

        assertEquals(ServerLogConfigs.URSA_OXIA_SERVICE_URL_DEFAULT, config.getUrsaOxiaServiceUrl());
    }

    @Test
    void testCatalogOxiaServiceUrlOverride() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_CATALOG_OXIA_SERVICE_URL_CONFIG, "oxia://catalog:6648/kafka"
        ));

        assertEquals("oxia://catalog:6648/kafka", config.getCatalogOxiaServiceUrl());
    }

    @Test
    void testUrsaOxiaServiceUrlOverride() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_OXIA_SERVICE_URL_CONFIG, "oxia://storage:6648/kafka"
        ));

        assertEquals("oxia://storage:6648/kafka", config.getUrsaOxiaServiceUrl());
    }

    @Test
    void testRemovedOxiaServiceUrlAndNamespaceKeysAreIgnored() throws Exception {
        // ursa.storage.oxia.service.url and ursa.storage.namespace were removed in favor of
        // ursa.catalog.oxia.service.url / ursa.oxia.service.url. Confirm stray legacy keys no
        // longer influence the resolved defaults (referenced here as raw strings since the
        // config constants themselves no longer exist).
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                "ursa.storage.oxia.service.url", "legacy-host:9999",
                "ursa.storage.namespace", "legacy-namespace"
        ));

        assertEquals(ServerLogConfigs.URSA_CATALOG_OXIA_SERVICE_URL_DEFAULT, config.getCatalogOxiaServiceUrl());
        assertEquals(ServerLogConfigs.URSA_OXIA_SERVICE_URL_DEFAULT, config.getUrsaOxiaServiceUrl());
    }
}
