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

import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/** Utilities for storing and reading raw Kafka {@link MemoryRecords} payloads. */
public final class KafkaRecordsPayload {

    private KafkaRecordsPayload() {
    }

    /**
     * Copies records into a reference-counted payload owned by the caller.
     *
     * <p>The copy is required because a Lakestream append is asynchronous and must not borrow the
     * request buffer after the request lifecycle has ended.
     */
    public static ByteBuf copyForAppend(MemoryRecords records) {
        ByteBuffer source = records.buffer().duplicate();
        return Unpooled.copiedBuffer(source);
    }

    /** Validates that records can be stored as one complete raw payload with the supplied record count. */
    public static void validateForAppend(MemoryRecords records, int expectedRecordCount) {
        ByteBuffer source = records.buffer();
        if (!source.hasRemaining()) {
            throw new IllegalArgumentException("Kafka append contains no MemoryRecords payload");
        }
        validateRecords(records, source.remaining(), expectedRecordCount);
    }

    /**
     * Copies, validates, and rebases a raw Kafka records payload.
     *
     * <p>The returned records do not retain or otherwise depend on {@code payload}. The storage
     * entry header is authoritative for both the base offset and record count.
     */
    public static MemoryRecords copyAndRebase(
            ByteBuf payload,
            long baseOffset,
            int expectedRecordCount) {
        validateEntryMetadata(payload, expectedRecordCount);

        int payloadSize = payload.readableBytes();
        ByteBuffer copy = copyPayload(payload, payloadSize);
        MemoryRecords records = MemoryRecords.readableRecords(copy);
        ValidatedBatches validated = validateRecords(records, payloadSize, expectedRecordCount);
        rebase(validated, baseOffset);
        return records;
    }

    private static void validateEntryMetadata(ByteBuf payload, int expectedRecordCount) {
        if (payload == null || !payload.isReadable()) {
            throw new IllegalArgumentException("Kafka storage entry contains no MemoryRecords payload");
        }
        if (expectedRecordCount <= 0) {
            throw new IllegalArgumentException(
                    "Kafka storage entry must contain at least one record, but expected " + expectedRecordCount);
        }
    }

    private static ByteBuffer copyPayload(ByteBuf payload, int payloadSize) {
        int readerIndex = payload.readerIndex();
        ByteBuffer copy = ByteBuffer.allocate(payloadSize);
        copy.put(payload.nioBuffer(readerIndex, payloadSize).duplicate());
        copy.flip();
        return copy;
    }

    private static ValidatedBatches validateRecords(
            MemoryRecords records,
            int payloadSize,
            int expectedRecordCount) {
        int validBytes = 0;
        int decodedRecordCount = 0;
        List<MutableRecordBatch> batches = new ArrayList<>();
        List<Integer> batchRecordCounts = new ArrayList<>();
        for (MutableRecordBatch batch : records.batches()) {
            batches.add(batch);
            int batchRecordCount = validateBatch(batch);
            batchRecordCounts.add(batchRecordCount);
            validBytes = Math.addExact(validBytes, batch.sizeInBytes());
            decodedRecordCount = Math.addExact(decodedRecordCount, batchRecordCount);
        }

        if (batches.isEmpty()) {
            throw new IllegalArgumentException("Kafka MemoryRecords contains no complete record batch");
        }
        if (validBytes != payloadSize) {
            throw new IllegalArgumentException(
                    "Kafka MemoryRecords contains trailing or incomplete bytes: valid="
                            + validBytes + ", payload=" + payloadSize);
        }
        if (decodedRecordCount != expectedRecordCount) {
            throw new IllegalArgumentException(
                    "Kafka record count does not match the storage header: decoded="
                            + decodedRecordCount + ", expected=" + expectedRecordCount);
        }
        return new ValidatedBatches(batches, batchRecordCounts);
    }

    private static int validateBatch(MutableRecordBatch batch) {
        batch.ensureValid();
        validateBatchType(batch);
        boolean validateV2Offsets = batch.magic() >= RecordBatch.MAGIC_VALUE_V2;
        validateBaseOffset(batch, validateV2Offsets);
        int batchRecordCount = validateBatchRecords(batch, validateV2Offsets);
        validateBatchRecordCount(batch, batchRecordCount);
        validateBatchOffsetRange(batch, batchRecordCount, validateV2Offsets);
        return batchRecordCount;
    }

    private static void validateBatchType(MutableRecordBatch batch) {
        if (batch.isControlBatch()) {
            throw new IllegalArgumentException("Kafka MemoryRecords contains a control batch");
        }
        if (batch.isTransactional()) {
            throw new IllegalArgumentException("Kafka MemoryRecords contains a transactional batch");
        }
    }

    private static void validateBaseOffset(MutableRecordBatch batch, boolean validateV2Offsets) {
        if (validateV2Offsets && batch.baseOffset() != 0L) {
            throw new IllegalArgumentException(
                    "Kafka producer record batch must have base offset 0, but was " + batch.baseOffset());
        }
    }

    private static int validateBatchRecords(MutableRecordBatch batch, boolean validateV2Offsets) {
        int batchRecordCount = 0;
        long expectedRecordOffset = batch.baseOffset();
        for (Record record : batch) {
            record.ensureValid();
            if (validateV2Offsets) {
                validateRecordOffset(record, expectedRecordOffset);
                expectedRecordOffset = Math.addExact(expectedRecordOffset, 1L);
            }
            batchRecordCount = Math.addExact(batchRecordCount, 1);
        }
        if (batchRecordCount <= 0) {
            throw new IllegalArgumentException("Kafka MemoryRecords contains an empty record batch");
        }
        return batchRecordCount;
    }

    private static void validateRecordOffset(Record record, long expectedRecordOffset) {
        if (record.offset() != expectedRecordOffset) {
            throw new IllegalArgumentException(
                    "Kafka record batch contains a non-sequential offset: expected="
                            + expectedRecordOffset + ", actual=" + record.offset());
        }
    }

    private static void validateBatchRecordCount(MutableRecordBatch batch, int batchRecordCount) {
        Integer declaredRecordCount = batch.countOrNull();
        if (declaredRecordCount != null && declaredRecordCount != batchRecordCount) {
            throw new IllegalArgumentException(
                    "Kafka record batch count does not match its contents: decoded="
                            + batchRecordCount + ", declared=" + declaredRecordCount);
        }
    }

    private static void validateBatchOffsetRange(
            MutableRecordBatch batch,
            int batchRecordCount,
            boolean validateV2Offsets) {
        if (!validateV2Offsets) {
            return;
        }
        long recordCountFromOffsets = batch.lastOffset() - batch.baseOffset() + 1L;
        if (recordCountFromOffsets != batchRecordCount) {
            throw new IllegalArgumentException(
                    "Kafka record batch offset range does not match its contents: range="
                            + recordCountFromOffsets + ", decoded=" + batchRecordCount);
        }
    }

    private static void rebase(ValidatedBatches validated, long baseOffset) {
        long nextBatchOffset = baseOffset;
        for (int i = 0; i < validated.batches().size(); i++) {
            MutableRecordBatch batch = validated.batches().get(i);
            int batchRecordCount = validated.batchRecordCounts().get(i);
            long lastOffset = Math.addExact(nextBatchOffset, batchRecordCount - 1L);
            batch.setLastOffset(lastOffset);
            nextBatchOffset = Math.addExact(lastOffset, 1L);
        }
    }

    private record ValidatedBatches(
            List<MutableRecordBatch> batches,
            List<Integer> batchRecordCounts) {
    }
}
