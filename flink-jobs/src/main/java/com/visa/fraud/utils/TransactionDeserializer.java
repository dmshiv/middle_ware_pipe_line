/*
 * =============================================================================
 * TRANSACTION JSON DESERIALIZER — Kafka → Flink Bridge
 * =============================================================================
 *
 * PURPOSE:
 *   Converts JSON bytes from Kafka into Transaction POJOs for Flink processing.
 *
 * WHY CUSTOM DESERIALIZER:
 *   Flink's KafkaSource needs a DeserializationSchema to know HOW to convert
 *   raw Kafka message bytes into Java objects. This bridges the gap between
 *   Kafka (byte arrays) and Flink (typed DataStream<Transaction>).
 *
 * JSON FIELD MAPPING:
 *   Kafka messages use snake_case (Python convention from simulator/enrichment).
 *   Java uses camelCase. Jackson's @JsonProperty handles the mapping, but we
 *   use a configured ObjectMapper with SNAKE_CASE naming strategy instead
 *   (less annotations, cleaner code).
 *
 * ERROR HANDLING:
 *   Malformed JSON messages are logged and skipped (return null).
 *   In production, these would go to a dead-letter queue (DLQ).
 * =============================================================================
 */
package com.visa.fraud.utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.visa.fraud.models.Transaction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TransactionDeserializer implements DeserializationSchema<Transaction> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TransactionDeserializer.class);

    // ObjectMapper is thread-safe and reusable — create once
    private transient ObjectMapper objectMapper;

    /**
     * Initialize ObjectMapper on first use (lazy init because ObjectMapper
     * is not serializable — Flink serializes this class when distributing to TaskManagers).
     */
    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
            // SNAKE_CASE: Maps "transaction_id" (Python) → "transactionId" (Java)
            objectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
            // Don't fail on unknown fields — enrichment may add fields we don't model
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }
        return objectMapper;
    }

    @Override
    public Transaction deserialize(byte[] message) throws IOException {
        try {
            return getObjectMapper().readValue(message, Transaction.class);
        } catch (Exception e) {
            // Log and skip malformed messages (dead-letter queue in production)
            LOG.warn("Failed to deserialize transaction: {}", new String(message), e);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(Transaction nextElement) {
        // Kafka streams are infinite — never end
        return false;
    }

    @Override
    public TypeInformation<Transaction> getProducedType() {
        return TypeInformation.of(Transaction.class);
    }
}
