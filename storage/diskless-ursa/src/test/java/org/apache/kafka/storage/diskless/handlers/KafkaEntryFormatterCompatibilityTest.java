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
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.EndTransactionMarker;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaEntryFormatterCompatibilityTest {

    private static final long TIMESTAMP = 1_700_000_000_000L;
    private static final byte[] UKFE_V1_PREFIX = HexFormat.of().parseHex("00000008554b464501000000");
    private static final byte[] PRE_V1_OPAQUE_HEADER = HexFormat.of().parseHex("00000003010203");

    @Test
    void testNonIdempotentRecordsUseNeutralV1Envelope() {
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(
                        TIMESTAMP,
                        "key".getBytes(StandardCharsets.UTF_8),
                        "value".getBytes(StandardCharsets.UTF_8)));

        assertEncodedEntry(records);
    }

    @Test
    void testIdempotentRecordsUseNeutralV1Envelope() {
        MemoryRecordsBuilder builder = MemoryRecords.builder(
                ByteBuffer.allocate(1024),
                RecordBatch.MAGIC_VALUE_V2,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0L,
                TIMESTAMP,
                1234L,
                (short) 2,
                7);
        builder.append(new SimpleRecord(TIMESTAMP, "key-1".getBytes(), "value-1".getBytes()));
        builder.append(new SimpleRecord(TIMESTAMP, "key-2".getBytes(), "value-2".getBytes()));

        assertEncodedEntry(builder.build());
    }

    @Test
    void testControlRecordsUseNeutralV1Envelope() {
        MemoryRecords records = MemoryRecords.withEndTransactionMarker(
                0L,
                TIMESTAMP,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                1234L,
                (short) 2,
                new EndTransactionMarker(ControlRecordType.COMMIT, 0));

        assertEncodedEntry(records);
    }

    @Test
    void testDecodesPreV1OpaqueHeaderWithoutMutatingEntry() {
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(TIMESTAMP, "key".getBytes(), "value".getBytes()));
        byte[] payload = toByteArray(records.buffer());
        ByteBuf entry = Unpooled.buffer(3 + PRE_V1_OPAQUE_HEADER.length + payload.length);
        entry.writeZero(3);
        entry.writeBytes(PRE_V1_OPAQUE_HEADER);
        entry.writeBytes(payload);
        entry.readerIndex(3);

        try {
            int readerIndex = entry.readerIndex();
            assertArrayEquals(payload, toByteArray(KafkaEntryFormatter.decode(entry)));
            assertEquals(readerIndex, entry.readerIndex());
        } finally {
            entry.release();
        }
    }

    @Test
    void testRejectsUnsupportedV1HeaderValues() {
        assertInvalidHeader("00000008554b464502000000", "version 2");
        assertInvalidHeader("00000008554b464501010000", "flags 1");
        assertInvalidHeader("00000008554b464501000001", "reserved value 1");
        assertInvalidHeader("00000004554b4645", "header length 4");
        assertInvalidHeader("00000008554b464501000000", "no Kafka MemoryRecords payload");
    }

    @Test
    void testRejectsInvalidHeaderLengths() {
        assertInvalidEntry(Unpooled.wrappedBuffer(new byte[Integer.BYTES - 1]));

        ByteBuf negativeLength = Unpooled.buffer(Integer.BYTES);
        negativeLength.writeInt(-1);
        assertInvalidEntry(negativeLength);

        ByteBuf truncatedHeader = Unpooled.buffer(Integer.BYTES + 2);
        truncatedHeader.writeInt(3);
        truncatedHeader.writeZero(2);
        assertInvalidEntry(truncatedHeader);
    }

    private static void assertEncodedEntry(MemoryRecords records) {
        byte[] expectedPayload = toByteArray(records.buffer());
        ByteBuf encoded = KafkaEntryFormatter.encode(records);
        try {
            assertEquals(UKFE_V1_PREFIX.length + expectedPayload.length, encoded.readableBytes());
            assertArrayEquals(UKFE_V1_PREFIX, ByteBufUtil.getBytes(encoded, 0, UKFE_V1_PREFIX.length));
            assertArrayEquals(
                    expectedPayload,
                    ByteBufUtil.getBytes(encoded, UKFE_V1_PREFIX.length, expectedPayload.length));
            int readerIndex = encoded.readerIndex();
            assertArrayEquals(expectedPayload, toByteArray(KafkaEntryFormatter.decode(encoded)));
            assertEquals(readerIndex, encoded.readerIndex());
            assertEquals(1, encoded.refCnt());
        } finally {
            encoded.release();
        }
    }

    private static void assertInvalidHeader(String hex, String message) {
        ByteBuf entry = Unpooled.wrappedBuffer(HexFormat.of().parseHex(hex));
        try {
            IllegalArgumentException error = assertThrows(
                    IllegalArgumentException.class, () -> KafkaEntryFormatter.decode(entry));
            assertTrue(error.getMessage().contains(message));
        } finally {
            entry.release();
        }
    }

    private static void assertInvalidEntry(ByteBuf entry) {
        try {
            assertThrows(IllegalArgumentException.class, () -> KafkaEntryFormatter.decode(entry));
        } finally {
            entry.release();
        }
    }

    private static byte[] toByteArray(ByteBuffer buffer) {
        ByteBuffer duplicate = buffer.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }
}
