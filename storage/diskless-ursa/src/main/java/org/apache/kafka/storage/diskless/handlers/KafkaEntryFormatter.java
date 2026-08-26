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

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/** Encodes Kafka record batches in the versioned Ursa Kafka framed-entry format. */
public final class KafkaEntryFormatter {

    private static final int FRAME_MAGIC = 0x554b4645; // ASCII "UKFE"
    private static final int FRAME_HEADER_LENGTH = 8;
    private static final short FRAME_VERSION = 1;

    private KafkaEntryFormatter() {
    }

    /**
     * Encodes Kafka records as {@code [frame header length][UKFE v1 header][Kafka MemoryRecords]}.
     *
     * @param records the Kafka records to encode
     * @return ByteBuf containing the encoded entry (caller must release)
     */
    public static ByteBuf encode(MemoryRecords records) {
        ByteBuffer recordsBuffer = records.buffer().duplicate();
        ByteBuf encoded = Unpooled.buffer(Integer.BYTES + FRAME_HEADER_LENGTH + recordsBuffer.remaining());
        boolean success = false;
        try {
            encoded.writeInt(FRAME_HEADER_LENGTH);
            encoded.writeInt(FRAME_MAGIC);
            encoded.writeByte(FRAME_VERSION);
            encoded.writeByte(0); // flags
            encoded.writeShort(0); // reserved
            encoded.writeBytes(recordsBuffer);
            success = true;
            return encoded;
        } finally {
            if (!success) {
                encoded.release();
            }
        }
    }

    /**
     * Decodes an entry to extract its Kafka records payload.
     *
     * <p>Pre-v1 entries are accepted as an opaque length-prefixed header followed by Kafka records.
     * Their header contents are deliberately not interpreted.
     *
     * @param entry the ByteBuf containing the encoded entry
     * @return a view containing the Kafka records; it remains valid only while the caller owns the entry buffer
     */
    public static ByteBuffer decode(ByteBuf entry) {
        int readerIndex = entry.readerIndex();
        int readableBytes = entry.readableBytes();
        if (readableBytes < Integer.BYTES) {
            throw new IllegalArgumentException("Storage entry is too small to contain its frame header length");
        }

        int frameHeaderLength = entry.getInt(readerIndex);
        int payloadSize = readableBytes - Integer.BYTES - frameHeaderLength;
        if (frameHeaderLength < 0 || payloadSize < 0) {
            throw new IllegalArgumentException(
                    "Invalid storage entry frame header length " + frameHeaderLength
                            + " for " + readableBytes + " bytes");
        }

        validateVersionedHeader(entry, readerIndex + Integer.BYTES, frameHeaderLength);
        if (payloadSize == 0) {
            throw new IllegalArgumentException("Storage entry contains no Kafka MemoryRecords payload");
        }
        return entry.nioBuffer(readerIndex + Integer.BYTES + frameHeaderLength, payloadSize);
    }

    private static void validateVersionedHeader(ByteBuf entry, int headerIndex, int headerLength) {
        if (headerLength < Integer.BYTES || entry.getInt(headerIndex) != FRAME_MAGIC) {
            return;
        }
        if (headerLength != FRAME_HEADER_LENGTH) {
            throw new IllegalArgumentException("Invalid UKFE frame header length " + headerLength);
        }

        short version = entry.getUnsignedByte(headerIndex + Integer.BYTES);
        if (version != FRAME_VERSION) {
            throw new IllegalArgumentException("Unsupported UKFE frame version " + version);
        }
        int flags = entry.getUnsignedByte(headerIndex + Integer.BYTES + Byte.BYTES);
        int reserved = entry.getUnsignedShort(headerIndex + Integer.BYTES + 2 * Byte.BYTES);
        if (flags != 0 || reserved != 0) {
            throw new IllegalArgumentException(
                    "Unsupported UKFE v1 frame flags " + flags + " or reserved value " + reserved);
        }
    }
}
