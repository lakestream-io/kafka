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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Controller-side lifecycle operations for Kafka-owned producer-state snapshots. */
public interface DisklessProducerStateLifecycle extends AutoCloseable {

    /**
     * A Kafka topic incarnation whose producer-state snapshots are durably owned by this store.
     *
     * <p>The source revision is the KRaft metadata revision last reconciled into the ownership
     * manifest. A controller must not delete an inventory entry until its metadata image has
     * reached at least this revision. This guards against a newly elected, lagging controller
     * deleting producer state created from a newer image. An active cleanup journal can use
     * revision zero when its active manifest was absent or corrupt: its permanent deletion fence,
     * rather than that fallback revision, establishes that the topic incarnation cannot be live.
     */
    record ManagedProducerStateTopic(String topicName, Uuid topicId, long sourceRevision) {
        public ManagedProducerStateTopic {
            Objects.requireNonNull(topicName, "topicName must not be null");
            Objects.requireNonNull(topicId, "topicId must not be null");
            if (topicName.isBlank()) {
                throw new IllegalArgumentException("topicName must not be blank");
            }
            if (Uuid.ZERO_UUID.equals(topicId)) {
                throw new IllegalArgumentException("topicId must not be zero");
            }
            if (sourceRevision < 0) {
                throw new IllegalArgumentException("sourceRevision must not be negative");
            }
        }
    }

    /**
     * Reconciles the durable producer-state ownership manifest for one topic incarnation.
     *
     * <p>Source revisions are monotonic: an invocation from a lagging controller must never
     * replace a manifest written at a newer revision. Once {@link #deleteTopicSnapshots(Uuid)}
     * durably fences a topic ID, a concurrent or delayed reconciliation must not make that topic
     * active again.
     */
    CompletableFuture<Void> reconcileTopic(String topicName, Uuid topicId, long sourceRevision);

    /**
     * Lists every topic incarnation with an active producer-state ownership or cleanup record.
     *
     * <p>An interrupted deletion remains enumerable through its active cleanup journal. A
     * completed deletion keeps only its permanent fence, which is deliberately excluded from
     * inventory so historical topic incarnations do not make every reconciliation scan grow.
     */
    CompletableFuture<List<ManagedProducerStateTopic>> listManagedTopics();

    /**
     * Permanently fences one Kafka topic incarnation and deletes all of its producer snapshots.
     *
     * <p>The durable deletion fence remains after cleanup, so the same immutable Kafka topic ID
     * can never recreate producer state. The implementation first persists an active cleanup
     * journal, then installs the fence and waits for ephemeral writer claims to drain before
     * deleting snapshots and the ownership manifest. The cleanup journal is removed only after
     * cleanup completes, so an interrupted deletion remains discoverable and retryable.
     */
    CompletableFuture<Void> deleteTopicSnapshots(Uuid topicId);
}
