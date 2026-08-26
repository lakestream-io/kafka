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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.EndTransactionMarker;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.storage.diskless.handlers.RecordAnalyzer.RecordAnalysisResult;

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

class KafkaEntryFormatterCompatibilityTest {

    private static final TopicPartition TEST_PARTITION = new TopicPartition("test-topic", 0);
    private static final long TIMESTAMP = 1_700_000_000_000L;

    // Captured from the former protocol-library encoder. These vectors include the four-byte metadata length.
    private static final byte[] NON_IDEMPOTENT_LEGACY_PREFIX = HexFormat.of().parseHex(
            "000000320a0e6b61666b612d70726f647563657210001880d095ffbc3122150a0c656e7472792e666f726d617412056b61666b615801");
    private static final byte[] IDEMPOTENT_LEGACY_PREFIX = HexFormat.of().parseHex(
            "0000002b0a043132333410071880d095ffbc3122150a0c656e7472792e666f726d617412056b61666b615802c00108");
    private static final byte[] TRANSACTION_MARKER_LEGACY_PREFIX = HexFormat.of().parseHex(
            "0000007c0a043132333410001880d095ffbc3122150a0c656e7472792e666f726d617412056b61666b61"
                    + "22160a0e74786e2e70726f6475636572496412043132333422160a1174786e2e70726f64756365724570"
                    + "6f636812013222190a0f74786e2e636f6e74726f6c547970651206434f4d4d49545801a00115b00100"
                    + "b80100");

    @Test
    void testNonIdempotentMetadataMatchesLegacyEnvelope() {
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(
                        TIMESTAMP,
                        "key".getBytes(StandardCharsets.UTF_8),
                        "value".getBytes(StandardCharsets.UTF_8)));

        assertEncodedEntry(records, NON_IDEMPOTENT_LEGACY_PREFIX);
    }

    @Test
    void testIdempotentMetadataMatchesLegacyEnvelope() {
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

        assertEncodedEntry(builder.build(), IDEMPOTENT_LEGACY_PREFIX);
    }

    @Test
    void testTransactionMarkerMetadataMatchesLegacyEnvelope() {
        MemoryRecords records = MemoryRecords.withEndTransactionMarker(
                0L,
                TIMESTAMP,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                1234L,
                (short) 2,
                new EndTransactionMarker(ControlRecordType.COMMIT, 0));

        assertEncodedEntry(records, TRANSACTION_MARKER_LEGACY_PREFIX);
    }

    @Test
    void testDecodesLegacyEnvelopeWithoutMutatingEntry() {
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(TIMESTAMP, "key".getBytes(), "value".getBytes()));
        byte[] payload = toByteArray(records.buffer());
        ByteBuf entry = Unpooled.buffer(3 + NON_IDEMPOTENT_LEGACY_PREFIX.length + payload.length);
        entry.writeZero(3);
        entry.writeBytes(NON_IDEMPOTENT_LEGACY_PREFIX);
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
    void testRejectsInvalidMetadataLengths() {
        assertInvalidEntry(Unpooled.wrappedBuffer(new byte[Integer.BYTES - 1]));

        ByteBuf negativeLength = Unpooled.buffer(Integer.BYTES);
        negativeLength.writeInt(-1);
        assertInvalidEntry(negativeLength);

        ByteBuf truncatedMetadata = Unpooled.buffer(Integer.BYTES + 2);
        truncatedMetadata.writeInt(3);
        truncatedMetadata.writeZero(2);
        assertInvalidEntry(truncatedMetadata);
    }

    private static void assertEncodedEntry(MemoryRecords records, byte[] expectedPrefix) {
        RecordAnalysisResult analysis = RecordAnalyzer.analyzeAndValidateRecords(records, TEST_PARTITION, 0);
        byte[] expectedPayload = toByteArray(records.buffer());
        ByteBuf encoded = KafkaEntryFormatter.encode(records, analysis);
        try {
            assertEquals(expectedPrefix.length + expectedPayload.length, encoded.readableBytes());
            assertArrayEquals(expectedPrefix, ByteBufUtil.getBytes(encoded, 0, expectedPrefix.length));
            assertArrayEquals(
                    expectedPayload,
                    ByteBufUtil.getBytes(encoded, expectedPrefix.length, expectedPayload.length));
        } finally {
            encoded.release();
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
