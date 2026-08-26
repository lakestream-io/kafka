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

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaRecordsPayloadTest {

    private static final long TIMESTAMP = 1_700_000_000_000L;

    @Test
    void testCopyForAppendContainsOnlyIndependentRawMemoryRecords() {
        MemoryRecords records = records("one", "two");
        byte[] expected = toByteArray(records.buffer());

        ByteBuf payload = KafkaRecordsPayload.copyForAppend(records);
        try {
            assertArrayEquals(expected, ByteBufUtil.getBytes(payload));
            assertEquals(1, payload.refCnt());

            ByteBuffer original = records.buffer();
            byte firstByte = original.get(original.position());
            original.put(original.position(), (byte) (firstByte + 1));
            assertEquals(firstByte, payload.getByte(payload.readerIndex()));
            original.put(original.position(), firstByte);
        } finally {
            payload.release();
        }
    }

    @Test
    void testCopyAndRebaseValidatesAndRebasesEveryBatchWithoutTakingOwnership() {
        MemoryRecords firstBatch = records(Compression.gzip().build(), "one", "two");
        MemoryRecords secondBatch = records("three");
        byte[] rawRecords = concatenate(firstBatch, secondBatch);
        ByteBuf payload = Unpooled.buffer(rawRecords.length + 3);
        payload.writeZero(3).writeBytes(rawRecords).readerIndex(3);

        try {
            int readerIndex = payload.readerIndex();
            int refCount = payload.refCnt();
            MemoryRecords rebased = KafkaRecordsPayload.copyAndRebase(payload, 41L, 3);

            List<Long> batchBaseOffsets = new ArrayList<>();
            List<Long> recordOffsets = new ArrayList<>();
            for (RecordBatch batch : rebased.batches()) {
                batchBaseOffsets.add(batch.baseOffset());
                for (Record record : batch) {
                    recordOffsets.add(record.offset());
                }
            }
            assertEquals(List.of(41L, 43L), batchBaseOffsets);
            assertEquals(List.of(41L, 42L, 43L), recordOffsets);
            assertEquals(readerIndex, payload.readerIndex());
            assertEquals(refCount, payload.refCnt());
            assertArrayEquals(rawRecords, ByteBufUtil.getBytes(payload, readerIndex, rawRecords.length));
        } finally {
            payload.release();
        }
    }

    @Test
    void testRejectsEmptyAndIncompletePayloads() {
        assertInvalid(Unpooled.buffer(0), 1, "no MemoryRecords payload");
        assertInvalid(Unpooled.wrappedBuffer(new byte[Long.BYTES]), 1, "no complete record batch");
    }

    @Test
    void testRejectsTrailingBytes() {
        byte[] records = toByteArray(records("one").buffer());
        ByteBuf payload = Unpooled.buffer(records.length + 1).writeBytes(records).writeByte(1);

        assertInvalid(payload, 1, "trailing or incomplete bytes");
    }

    @Test
    void testRejectsAnyPrefixBeforeMemoryRecords() {
        ByteBuf rawRecords = KafkaRecordsPayload.copyForAppend(records("one"));
        ByteBuf prefixed = Unpooled.buffer(Integer.BYTES + rawRecords.readableBytes());
        prefixed.writeInt(0);
        prefixed.writeBytes(rawRecords, rawRecords.readerIndex(), rawRecords.readableBytes());
        rawRecords.release();

        try {
            assertThrows(RuntimeException.class, () -> KafkaRecordsPayload.copyAndRebase(prefixed, 0L, 1));
            assertEquals(1, prefixed.refCnt());
        } finally {
            prefixed.release();
        }
    }

    @Test
    void testRejectsCorruptBatchCrc() {
        byte[] records = toByteArray(records("one").buffer());
        records[records.length - 1] ^= 1;
        ByteBuf payload = Unpooled.wrappedBuffer(records);
        try {
            assertThrows(RuntimeException.class, () -> KafkaRecordsPayload.copyAndRebase(payload, 0L, 1));
            assertEquals(1, payload.refCnt());
        } finally {
            payload.release();
        }
    }

    @Test
    void testRejectsStorageHeaderRecordCountMismatch() {
        ByteBuf payload = KafkaRecordsPayload.copyForAppend(records("one", "two"));

        assertInvalid(payload, 1, "decoded=2, expected=1");
    }

    @Test
    void testRejectsNonCanonicalV2Offsets() {
        ByteBuf invalidRange = KafkaRecordsPayload.copyForAppend(recordsWithOffsets(0L, 7L, 0L, 1L));
        assertInvalid(invalidRange, 2, "offset range does not match");

        ByteBuf nonSequential = KafkaRecordsPayload.copyForAppend(recordsWithOffsets(0L, 1L, 0L, 2L));
        assertInvalid(nonSequential, 2, "non-sequential offset");
    }

    @Test
    void testRejectsNonZeroBaseOffsetAndTransactionalBatch() {
        ByteBuf nonZeroBaseOffset = KafkaRecordsPayload.copyForAppend(recordsWithOffsets(5L, 5L, 5L));
        assertInvalid(nonZeroBaseOffset, 1, "base offset 0");

        MemoryRecords transactionalRecords = MemoryRecords.withTransactionalRecords(
                Compression.NONE,
                13L,
                (short) 2,
                0,
                new SimpleRecord(TIMESTAMP, "value".getBytes(StandardCharsets.UTF_8)));
        ByteBuf transactional = KafkaRecordsPayload.copyForAppend(transactionalRecords);
        assertInvalid(transactional, 1, "transactional batch");
    }

    private static void assertInvalid(ByteBuf payload, int expectedRecordCount, String expectedMessage) {
        try {
            IllegalArgumentException error = assertThrows(
                    IllegalArgumentException.class,
                    () -> KafkaRecordsPayload.copyAndRebase(payload, 0L, expectedRecordCount));
            assertTrue(error.getMessage().contains(expectedMessage));
            assertEquals(1, payload.refCnt());
        } finally {
            payload.release();
        }
    }

    private static MemoryRecords records(String... values) {
        return records(Compression.NONE, values);
    }

    private static MemoryRecords records(Compression compression, String... values) {
        SimpleRecord[] records = new SimpleRecord[values.length];
        for (int i = 0; i < values.length; i++) {
            records[i] = new SimpleRecord(
                    TIMESTAMP,
                    "key".getBytes(StandardCharsets.UTF_8),
                    values[i].getBytes(StandardCharsets.UTF_8));
        }
        return MemoryRecords.withRecords(compression, records);
    }

    private static MemoryRecords recordsWithOffsets(long baseOffset, long lastOffset, long... recordOffsets) {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.builder(
                buffer,
                RecordBatch.MAGIC_VALUE_V2,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                baseOffset);
        for (long recordOffset : recordOffsets) {
            builder.appendWithOffset(
                    recordOffset,
                    new SimpleRecord(TIMESTAMP, "value".getBytes(StandardCharsets.UTF_8)));
        }
        builder.overrideLastOffset(lastOffset);
        return builder.build();
    }

    private static byte[] concatenate(MemoryRecords... recordSets) {
        int size = 0;
        for (MemoryRecords records : recordSets) {
            size = Math.addExact(size, records.buffer().remaining());
        }
        ByteBuffer combined = ByteBuffer.allocate(size);
        for (MemoryRecords records : recordSets) {
            combined.put(records.buffer().duplicate());
        }
        return combined.array();
    }

    private static byte[] toByteArray(ByteBuffer buffer) {
        ByteBuffer duplicate = buffer.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }
}
