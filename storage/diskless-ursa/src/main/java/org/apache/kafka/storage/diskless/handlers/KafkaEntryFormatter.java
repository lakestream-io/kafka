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

import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.storage.diskless.handlers.RecordAnalyzer.RecordAnalysisResult;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Encodes Kafka record batches in the storage envelope consumed by existing Ursa compactors.
 *
 * <p>The envelope is intentionally implemented without a dependency on a messaging protocol runtime. Its metadata
 * field numbers are a persisted compatibility contract, so existing WAL entries and compacted objects remain
 * readable while Kafka's Ursa plugin stays self-contained.
 */
public final class KafkaEntryFormatter {

    public static final String ENTRY_FORMAT_KEY = "entry.format";
    public static final String ENTRY_FORMAT_VALUE = "kafka";

    private static final int PRODUCER_NAME_FIELD = 1;
    private static final int SEQUENCE_ID_FIELD = 2;
    private static final int PUBLISH_TIME_FIELD = 3;
    private static final int PROPERTY_FIELD = 4;
    private static final int NUMBER_OF_RECORDS_FIELD = 11;
    private static final int MARKER_TYPE_FIELD = 20;
    private static final int TRANSACTION_ID_LEAST_BITS_FIELD = 22;
    private static final int TRANSACTION_ID_MOST_BITS_FIELD = 23;
    private static final int HIGHEST_SEQUENCE_ID_FIELD = 24;

    private static final int PROPERTY_KEY_FIELD = 1;
    private static final int PROPERTY_VALUE_FIELD = 2;

    private static final int VARINT_WIRE_TYPE = 0;
    private static final int LENGTH_DELIMITED_WIRE_TYPE = 2;
    private static final int TRANSACTION_COMMIT_MARKER = 21;
    private static final String DEFAULT_PRODUCER_NAME = "kafka-producer";

    private KafkaEntryFormatter() {
    }

    /**
     * Encodes Kafka records as {@code [metadata length][metadata][Kafka records]}.
     *
     * @param records the Kafka records to encode
     * @param analysisResult the analysis result containing metadata from the records
     * @return ByteBuf containing the encoded entry (caller must release)
     */
    public static ByteBuf encode(MemoryRecords records, RecordAnalysisResult analysisResult) {
        ByteBuf metadata = buildMetadata(records, analysisResult);
        try {
            ByteBuffer recordsBuffer = records.buffer().duplicate();
            ByteBuf encoded = Unpooled.buffer(Integer.BYTES + metadata.readableBytes() + recordsBuffer.remaining());
            encoded.writeInt(metadata.readableBytes());
            encoded.writeBytes(metadata, metadata.readerIndex(), metadata.readableBytes());
            encoded.writeBytes(recordsBuffer);
            return encoded;
        } finally {
            metadata.release();
        }
    }

    /**
     * Decodes an entry to extract its Kafka records payload.
     *
     * @param entry the ByteBuf containing the encoded entry
     * @return a view containing the Kafka records; it remains valid only while the caller owns the entry buffer
     */
    public static ByteBuffer decode(ByteBuf entry) {
        int readableBytes = entry.readableBytes();
        if (readableBytes < Integer.BYTES) {
            throw new IllegalArgumentException("Storage entry is too small to contain its metadata length");
        }

        int metadataSize = entry.getInt(entry.readerIndex());
        int payloadSize = readableBytes - Integer.BYTES - metadataSize;
        if (metadataSize < 0 || payloadSize < 0) {
            throw new IllegalArgumentException(
                    "Invalid storage entry metadata size " + metadataSize + " for " + readableBytes + " bytes");
        }

        return entry.nioBuffer(entry.readerIndex() + Integer.BYTES + metadataSize, payloadSize);
    }

    private static ByteBuf buildMetadata(MemoryRecords records, RecordAnalysisResult analysisResult) {
        MetadataFields fields = metadataFields(records.firstBatch(), analysisResult);
        ByteBuf metadata = Unpooled.buffer();
        boolean success = false;
        try {
            writeStringField(metadata, PRODUCER_NAME_FIELD, fields.producerName());
            writeVarintField(metadata, SEQUENCE_ID_FIELD, fields.sequenceId());
            writeVarintField(metadata, PUBLISH_TIME_FIELD, fields.publishTime());
            for (MetadataProperty property : fields.properties()) {
                writeProperty(metadata, property);
            }
            writeVarintField(metadata, NUMBER_OF_RECORDS_FIELD, analysisResult.recordCount());
            writeMarker(metadata, fields.markerType());
            writeTransactionId(metadata, fields.includeTransactionId());
            writeHighestSequenceId(metadata, fields.highestSequenceId());
            success = true;
            return metadata;
        } finally {
            if (!success) {
                metadata.release();
            }
        }
    }

    private static MetadataFields metadataFields(RecordBatch firstBatch, RecordAnalysisResult analysisResult) {
        long maxTimestamp = analysisResult.maxTimestamp();
        long publishTime = maxTimestamp >= 0 ? maxTimestamp : System.currentTimeMillis();
        List<MetadataProperty> properties = new ArrayList<>();
        properties.add(new MetadataProperty(ENTRY_FORMAT_KEY, ENTRY_FORMAT_VALUE));
        if (firstBatch == null) {
            return new MetadataFields(DEFAULT_PRODUCER_NAME, 0, publishTime, properties, null, false, null);
        }

        ProducerFields producerFields = producerFields(firstBatch);
        TransactionFields transactionFields = transactionFields(firstBatch, properties);
        return new MetadataFields(
                producerFields.producerName(),
                producerFields.sequenceId(),
                publishTime,
                properties,
                transactionFields.markerType(),
                transactionFields.includeTransactionId(),
                producerFields.highestSequenceId());
    }

    private static ProducerFields producerFields(RecordBatch batch) {
        if (!batch.hasProducerId()) {
            return new ProducerFields(DEFAULT_PRODUCER_NAME, 0, null);
        }
        Long highestSequenceId = batch.lastSequence() >= 0 ? (long) batch.lastSequence() : null;
        return new ProducerFields(
                String.valueOf(batch.producerId()),
                Math.max(batch.baseSequence(), 0),
                highestSequenceId);
    }

    private static TransactionFields transactionFields(RecordBatch batch, List<MetadataProperty> properties) {
        if (batch.isControlBatch()) {
            Record record = batch.iterator().next();
            ControlRecordType type = ControlRecordType.parse(record.key());
            addTransactionProperties(properties, batch, type.name());
            boolean isTransactionMarker = type == ControlRecordType.COMMIT || type == ControlRecordType.ABORT;
            return new TransactionFields(isTransactionMarker ? TRANSACTION_COMMIT_MARKER : null, isTransactionMarker);
        }
        if (batch.isTransactional()) {
            addTransactionProperties(properties, batch, null);
        }
        return new TransactionFields(null, false);
    }

    private static void addTransactionProperties(List<MetadataProperty> properties, RecordBatch batch, String controlType) {
        properties.add(new MetadataProperty("txn.producerId", String.valueOf(batch.producerId())));
        properties.add(new MetadataProperty("txn.producerEpoch", String.valueOf(batch.producerEpoch())));
        if (controlType != null) {
            properties.add(new MetadataProperty("txn.controlType", controlType));
        }
    }

    private static void writeMarker(ByteBuf metadata, Integer markerType) {
        if (markerType != null) {
            writeVarintField(metadata, MARKER_TYPE_FIELD, markerType);
        }
    }

    private static void writeTransactionId(ByteBuf metadata, boolean includeTransactionId) {
        if (includeTransactionId) {
            writeVarintField(metadata, TRANSACTION_ID_LEAST_BITS_FIELD, 0);
            writeVarintField(metadata, TRANSACTION_ID_MOST_BITS_FIELD, 0);
        }
    }

    private static void writeHighestSequenceId(ByteBuf metadata, Long highestSequenceId) {
        if (highestSequenceId != null) {
            writeVarintField(metadata, HIGHEST_SEQUENCE_ID_FIELD, highestSequenceId);
        }
    }

    private static void writeProperty(ByteBuf target, MetadataProperty property) {
        ByteBuf encodedProperty = Unpooled.buffer();
        try {
            writeStringField(encodedProperty, PROPERTY_KEY_FIELD, property.key());
            writeStringField(encodedProperty, PROPERTY_VALUE_FIELD, property.value());
            writeTag(target, PROPERTY_FIELD, LENGTH_DELIMITED_WIRE_TYPE);
            writeVarint(target, encodedProperty.readableBytes());
            target.writeBytes(encodedProperty, encodedProperty.readerIndex(), encodedProperty.readableBytes());
        } finally {
            encodedProperty.release();
        }
    }

    private static void writeStringField(ByteBuf target, int fieldNumber, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeTag(target, fieldNumber, LENGTH_DELIMITED_WIRE_TYPE);
        writeVarint(target, bytes.length);
        target.writeBytes(bytes);
    }

    private static void writeVarintField(ByteBuf target, int fieldNumber, long value) {
        writeTag(target, fieldNumber, VARINT_WIRE_TYPE);
        writeVarint(target, value);
    }

    private static void writeTag(ByteBuf target, int fieldNumber, int wireType) {
        writeVarint(target, ((long) fieldNumber << 3) | wireType);
    }

    private static void writeVarint(ByteBuf target, long value) {
        long remaining = value;
        while ((remaining & ~0x7fL) != 0) {
            target.writeByte((int) ((remaining & 0x7fL) | 0x80L));
            remaining >>>= 7;
        }
        target.writeByte((int) remaining);
    }

    private record MetadataProperty(String key, String value) {
    }

    private record MetadataFields(
            String producerName,
            long sequenceId,
            long publishTime,
            List<MetadataProperty> properties,
            Integer markerType,
            boolean includeTransactionId,
            Long highestSequenceId
    ) {
    }

    private record ProducerFields(String producerName, long sequenceId, Long highestSequenceId) {
    }

    private record TransactionFields(Integer markerType, boolean includeTransactionId) {
    }
}
