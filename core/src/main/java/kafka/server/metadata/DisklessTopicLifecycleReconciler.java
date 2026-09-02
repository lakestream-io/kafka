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
package kafka.server.metadata;

import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.storage.diskless.DisklessTopicLifecycle;

import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * Active-controller reconciler for post-commit diskless topic lifecycle operations.
 *
 * <p>This is a placeholder while the reconciler is rewritten against the collapsed
 * {@link DisklessTopicLifecycle} SPI: it publishes nothing and drives no lifecycle operation yet.
 */
public final class DisklessTopicLifecycleReconciler implements MetadataPublisher {

    private final int nodeId;

    public DisklessTopicLifecycleReconciler(
            int nodeId,
            DisklessTopicLifecycle lifecycle,
            BiConsumer<String, Throwable> faultHandler,
            long sweepIntervalMs) {
        this.nodeId = nodeId;
        Objects.requireNonNull(lifecycle, "lifecycle must not be null");
        Objects.requireNonNull(faultHandler, "faultHandler must not be null");
        if (sweepIntervalMs <= 0) {
            throw new IllegalArgumentException("sweepIntervalMs must be positive");
        }
    }

    @Override
    public String name() {
        return "DisklessTopicLifecycleReconciler id=" + nodeId;
    }

    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
    }

    @Override
    public void onControllerChange(LeaderAndEpoch newLeaderAndEpoch) {
    }

    @Override
    public void close() {
    }
}
