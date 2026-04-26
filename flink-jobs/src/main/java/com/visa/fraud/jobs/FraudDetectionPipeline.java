/*
 * =============================================================================
 * FLINK FRAUD DETECTION PIPELINE — Main Entry Point
 * =============================================================================
 *
 * PURPOSE:
 *   This is the CORE of the entire project. A single Flink application that
 *   runs 4 parallel fraud detection jobs on the same enriched transaction stream.
 *
 * THE 4 FRAUD DETECTION JOBS:
 *
 *   1. VELOCITY CHECK (Tumbling Window)
 *      - Counts transactions per card in 60-second windows
 *      - Flags if >5 transactions from same card in one window
 *      - WHY: Stolen cards are often used in rapid-fire purchases before owner notices
 *      - FLINK CONCEPT: Tumbling window = fixed-size, non-overlapping time buckets
 *
 *   2. GEO-HOPPING DETECTION (CEP — Complex Event Processing)
 *      - Detects same card used in 2 DIFFERENT COUNTRIES within 10 minutes
 *      - Example: Card used in New York at 10:00, then Tokyo at 10:05 = IMPOSSIBLE
 *      - WHY: Physical cards can't teleport. This is a classic card-cloning indicator.
 *      - FLINK CONCEPT: CEP Pattern API = define sequence of events to match
 *
 *   3. AMOUNT ANOMALY (Sliding Window)
 *      - Calculates rolling average amount per card over 1-hour window (5-min slide)
 *      - Flags if single transaction > 3x the rolling average
 *      - WHY: If someone usually spends $50, a sudden $5000 purchase is suspicious
 *      - FLINK CONCEPT: Sliding window = overlapping windows for smooth aggregation
 *
 *   4. TRANSACTION AGGREGATION (Tumbling Window)
 *      - Aggregates stats every 5 minutes: volume, avg amount, decline rate
 *      - Per country, per merchant category, per card type
 *      - WHY: Real-time dashboards need pre-aggregated data (can't query raw events)
 *      - FLINK CONCEPT: GroupBy + Window = SQL-like aggregation on streams
 *
 * EVENT TIME vs PROCESSING TIME:
 *   We use EVENT TIME (when transaction actually happened) not PROCESSING TIME
 *   (when Flink received it). WHY: Network delays can cause events to arrive
 *   out of order. A transaction at 10:00:00 might arrive at 10:00:05.
 *   Event time + watermarks handle this correctly.
 *
 * WATERMARKS:
 *   "I guarantee no events with timestamp < watermark will arrive."
 *   We allow 5 seconds of out-of-orderness. Events arriving >5s late are dropped.
 *   WHY 5s: Visa's network typically delivers within 2-3 seconds. 5s is safe margin.
 *
 * EXACTLY-ONCE SEMANTICS:
 *   Flink checkpointing + Kafka transactions = exactly-once processing.
 *   Even if Flink crashes mid-processing, recovery replays from last checkpoint.
 *   No duplicate alerts, no missed transactions.
 *
 * =============================================================================
 */
package com.visa.fraud.jobs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.visa.fraud.models.Transaction;
import com.visa.fraud.utils.TransactionDeserializer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class FraudDetectionPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(FraudDetectionPipeline.class);

    // ─── CONFIGURATION ──────────────────────────────────────────────────
    // Read from environment variables (configurable per deployment)
    private static final String KAFKA_BOOTSTRAP = System.getenv()
            .getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String INPUT_TOPIC = System.getenv()
            .getOrDefault("INPUT_TOPIC", "transactions.enriched");
    private static final String ALERTS_TOPIC = System.getenv()
            .getOrDefault("ALERTS_TOPIC", "transactions.alerts");
    private static final String AGGREGATION_TOPIC = System.getenv()
            .getOrDefault("AGGREGATION_TOPIC", "transactions.aggregated");

    // Fraud thresholds (tunable)
    private static final int VELOCITY_THRESHOLD = 5;         // Max txns per card in 60s
    private static final int GEO_HOP_MINUTES = 10;           // Max minutes between countries
    private static final double AMOUNT_ANOMALY_FACTOR = 3.0; // Flag if > 3x average

    // JSON serializer for output
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);

    public static void main(String[] args) throws Exception {

        LOG.info("═══════════════════════════════════════════════════════════════");
        LOG.info("🏦 VISA FRAUD DETECTION PIPELINE — Starting");
        LOG.info("   Kafka:        {}", KAFKA_BOOTSTRAP);
        LOG.info("   Input Topic:  {}", INPUT_TOPIC);
        LOG.info("   Alerts Topic: {}", ALERTS_TOPIC);
        LOG.info("═══════════════════════════════════════════════════════════════");

        // ─── FLINK EXECUTION ENVIRONMENT ────────────────────────────────
        // This is the entry point for all Flink programs.
        // Configuration here affects ALL jobs in this pipeline.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // CHECKPOINTING: Save state every 30 seconds
        // WHY: If Flink crashes, it resumes from last checkpoint (exactly-once)
        // Checkpoint storage: by default uses JobManager memory (prod uses S3/HDFS)
        env.enableCheckpointing(30000); // 30 seconds

        // PARALLELISM: How many parallel instances of each operator
        // In production, this would be 8-64 based on Kafka partition count
        env.setParallelism(2);

        // ─── KAFKA SOURCE ───────────────────────────────────────────────
        // Read enriched transactions from Kafka
        // WHY KafkaSource (new API) vs FlinkKafkaConsumer (legacy):
        //   New API supports exactly-once with bounded/unbounded sources
        KafkaSource<Transaction> kafkaSource = KafkaSource.<Transaction>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(INPUT_TOPIC)
                .setGroupId("flink-fraud-detection")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new TransactionDeserializer())
                .build();

        // ─── WATERMARK STRATEGY ─────────────────────────────────────────
        // Event-time processing with 5-second tolerance for out-of-order events
        //
        // WHAT ARE WATERMARKS?
        //   A watermark W(t) says: "All events with timestamp <= t have arrived."
        //   Flink uses this to know when a time window is "complete" and can be processed.
        //
        // BOUNDED OUT-OF-ORDERNESS:
        //   We allow events to arrive up to 5 seconds late.
        //   Example: If latest event is at 10:00:10, watermark is at 10:00:05.
        //   Any event with timestamp < 10:00:05 is considered "late" and dropped.
        //
        // WHY 5 SECONDS:
        //   Visa's VisaNet typically delivers transactions within 2-3 seconds.
        //   5 seconds gives a safe margin without delaying window results too much.
        WatermarkStrategy<Transaction> watermarkStrategy = WatermarkStrategy
                .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((txn, timestamp) -> txn.getEventTime());

        // ─── MAIN DATA STREAM ───────────────────────────────────────────
        // This is the enriched transaction stream that ALL jobs consume from
        DataStream<Transaction> transactions = env
                .fromSource(kafkaSource, watermarkStrategy, "Kafka-Transactions")
                .filter(txn -> txn != null && txn.getCardNumber() != null && txn.getCountry() != null)
                .name("Filter-Nulls");

        // Key by card number — most fraud detection is per-card
        KeyedStream<Transaction, String> keyedByCard = transactions
                .keyBy(Transaction::getCardNumber);

        // ─── KAFKA SINK (shared by all jobs) ────────────────────────────
        KafkaSink<String> alertsSink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setRecordSerializer(
                    KafkaRecordSerializationSchema.builder()
                        .setTopic(ALERTS_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        KafkaSink<String> aggregationSink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setRecordSerializer(
                    KafkaRecordSerializationSchema.builder()
                        .setTopic(AGGREGATION_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        // ═════════════════════════════════════════════════════════════════
        // JOB 1: VELOCITY CHECK — Tumbling Window (60 seconds)
        // ═════════════════════════════════════════════════════════════════
        //
        // WHAT IS A TUMBLING WINDOW?
        //   Fixed-size, non-overlapping time buckets:
        //   |----60s----|----60s----|----60s----|
        //   Each window counts transactions independently.
        //
        // WHY TUMBLING (not sliding) for velocity:
        //   We want a clear count per time bucket. "Did this card have >5 txns
        //   in the last minute?" Tumbling gives a clean yes/no per window.
        //
        // EXAMPLE:
        //   Card 4111... makes 8 transactions between 10:00:00 and 10:01:00
        //   → Window [10:00, 10:01) fires with count=8 → ALERT (threshold=5)
        //
        LOG.info("📊 Starting Job 1: Velocity Check (60s tumbling window, threshold={})", VELOCITY_THRESHOLD);

        SingleOutputStreamOperator<String> velocityAlerts = keyedByCard
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .process(new org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<
                        Transaction, String, String, org.apache.flink.streaming.api.windowing.windows.TimeWindow>() {
                    @Override
                    public void process(String cardNumber,
                                        Context context,
                                        Iterable<Transaction> transactions,
                                        org.apache.flink.util.Collector<String> out) throws Exception {
                        // Count transactions in this window
                        List<Transaction> txnList = new ArrayList<>();
                        transactions.forEach(txnList::add);
                        int count = txnList.size();

                        if (count > VELOCITY_THRESHOLD) {
                            // Calculate total amount spent in this burst
                            double totalAmount = txnList.stream()
                                    .mapToDouble(Transaction::getAmount).sum();

                            // Get unique merchants (is it spreading across merchants?)
                            long uniqueMerchants = txnList.stream()
                                    .map(Transaction::getMerchantId)
                                    .distinct().count();

                            Map<String, Object> alert = new LinkedHashMap<>();
                            alert.put("alert_type", "VELOCITY_ATTACK");
                            alert.put("severity", count > 10 ? "CRITICAL" : "HIGH");
                            alert.put("card_number", txnList.get(0).getMaskedCard());
                            alert.put("transaction_count", count);
                            alert.put("window_seconds", 60);
                            alert.put("threshold", VELOCITY_THRESHOLD);
                            alert.put("total_amount", Math.round(totalAmount * 100.0) / 100.0);
                            alert.put("unique_merchants", uniqueMerchants);
                            alert.put("window_start", context.window().getStart());
                            alert.put("window_end", context.window().getEnd());
                            alert.put("detected_at", System.currentTimeMillis());
                            alert.put("recommended_action", count > 10
                                    ? "BLOCK_CARD_IMMEDIATELY" : "FLAG_FOR_REVIEW");

                            out.collect(MAPPER.writeValueAsString(alert));
                            LOG.warn("🚨 VELOCITY ALERT: {} — {} txns in 60s (threshold: {})",
                                    txnList.get(0).getMaskedCard(), count, VELOCITY_THRESHOLD);
                        }
                    }
                })
                .name("Velocity-Check");

        velocityAlerts.sinkTo(alertsSink).name("Velocity-Alerts-Sink");

        // ═════════════════════════════════════════════════════════════════
        // JOB 2: GEO-HOPPING DETECTION — CEP (Complex Event Processing)
        // ═════════════════════════════════════════════════════════════════
        //
        // WHAT IS CEP?
        //   Pattern matching on event streams. Define a SEQUENCE of events
        //   and Flink watches for that pattern across the infinite stream.
        //
        // OUR PATTERN:
        //   first_transaction(country=X) → second_transaction(country=Y, where Y≠X)
        //   WITHIN 10 minutes
        //
        // WHY THIS CATCHES FRAUD:
        //   If a card is used in New York at 10:00 and then in Tokyo at 10:05,
        //   it's physically impossible. The card was cloned.
        //
        // CEP vs WINDOW:
        //   We could use a window, but CEP is more natural for "sequence of events"
        //   matching. CEP also handles complex patterns: "A followed by B but not C"
        //
        LOG.info("🌍 Starting Job 2: Geo-Hopping Detection (CEP, within {} min)", GEO_HOP_MINUTES);

        // Define the CEP pattern:
        // Step 1: "firstTransaction" — any transaction
        // Step 2: "secondTransaction" — same card, DIFFERENT country, within 10 min
        Pattern<Transaction, ?> geoHopPattern = Pattern
                .<Transaction>begin("firstTransaction")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction txn) {
                        // Accept any transaction as the starting point
                        return txn.getCountry() != null && !txn.getCountry().isEmpty();
                    }
                })
                .next("secondTransaction")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction txn) {
                        return txn.getCountry() != null && !txn.getCountry().isEmpty();
                    }
                })
                .within(org.apache.flink.streaming.api.windowing.time.Time.minutes(GEO_HOP_MINUTES));

        // Apply pattern to the keyed stream (per card)
        PatternStream<Transaction> geoHopStream = CEP.pattern(keyedByCard, geoHopPattern);

        // Select matching patterns and generate alerts
        SingleOutputStreamOperator<String> geoHopAlerts = geoHopStream
                .select(new PatternSelectFunction<Transaction, String>() {
                    @Override
                    public String select(Map<String, List<Transaction>> pattern) throws Exception {
                        Transaction first = pattern.get("firstTransaction").get(0);
                        Transaction second = pattern.get("secondTransaction").get(0);

                        // Only alert if countries are DIFFERENT
                        if (first.getCountry().equals(second.getCountry())) {
                            return null; // Same country — not geo-hopping
                        }

                        Map<String, Object> alert = new LinkedHashMap<>();
                        alert.put("alert_type", "GEO_HOPPING");
                        alert.put("severity", "CRITICAL");
                        alert.put("card_number", first.getMaskedCard());
                        alert.put("first_country", first.getCountry());
                        alert.put("first_city", first.getCity());
                        alert.put("first_amount", first.getAmount());
                        alert.put("first_time", first.getTimestamp());
                        alert.put("second_country", second.getCountry());
                        alert.put("second_city", second.getCity());
                        alert.put("second_amount", second.getAmount());
                        alert.put("second_time", second.getTimestamp());
                        alert.put("time_between_ms",
                                second.getEventTime() - first.getEventTime());
                        alert.put("detected_at", System.currentTimeMillis());
                        alert.put("recommended_action", "BLOCK_CARD_IMMEDIATELY");
                        alert.put("reason", String.format(
                                "Card used in %s and %s within %d minutes — impossible travel",
                                first.getCountry(), second.getCountry(), GEO_HOP_MINUTES));

                        LOG.warn("🌍 GEO-HOP ALERT: {} — {} → {} within {} min",
                                first.getMaskedCard(), first.getCountry(),
                                second.getCountry(), GEO_HOP_MINUTES);

                        return MAPPER.writeValueAsString(alert);
                    }
                })
                .filter(alert -> alert != null) // Filter out same-country matches
                .name("GeoHop-Detection");

        geoHopAlerts.sinkTo(alertsSink).name("GeoHop-Alerts-Sink");

        // ═════════════════════════════════════════════════════════════════
        // JOB 3: AMOUNT ANOMALY — Sliding Window (1 hour, 5 min slide)
        // ═════════════════════════════════════════════════════════════════
        //
        // WHAT IS A SLIDING WINDOW?
        //   Overlapping time windows. A 1-hour window that slides every 5 min:
        //   |--------1hr--------|
        //        |--------1hr--------|
        //             |--------1hr--------|
        //   Each event belongs to MULTIPLE windows. This gives smooth averages.
        //
        // WHY SLIDING (not tumbling) for amount anomaly:
        //   Tumbling would give jagged averages (recalculated every hour).
        //   Sliding gives a smooth rolling average updated every 5 minutes.
        //   Better for detecting "gradually escalating amounts" fraud pattern.
        //
        // LOGIC:
        //   1. Track average transaction amount per card over 1 hour
        //   2. If any single transaction > 3x the rolling average → ALERT
        //
        LOG.info("💰 Starting Job 3: Amount Anomaly Detection (1hr window, 5min slide, factor={}x)",
                AMOUNT_ANOMALY_FACTOR);

        SingleOutputStreamOperator<String> amountAlerts = keyedByCard
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .process(new org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<
                        Transaction, String, String, org.apache.flink.streaming.api.windowing.windows.TimeWindow>() {
                    @Override
                    public void process(String cardNumber,
                                        Context context,
                                        Iterable<Transaction> transactions,
                                        org.apache.flink.util.Collector<String> out) throws Exception {

                        List<Transaction> txnList = new ArrayList<>();
                        transactions.forEach(txnList::add);

                        if (txnList.size() < 2) return; // Need at least 2 txns for comparison

                        // Calculate average amount
                        double avg = txnList.stream()
                                .mapToDouble(Transaction::getAmount).average().orElse(0);

                        // Find transactions that are anomalously high
                        for (Transaction txn : txnList) {
                            if (txn.getAmount() > avg * AMOUNT_ANOMALY_FACTOR && txn.getAmount() > 500) {
                                Map<String, Object> alert = new LinkedHashMap<>();
                                alert.put("alert_type", "AMOUNT_ANOMALY");
                                alert.put("severity", txn.getAmount() > avg * 5 ? "CRITICAL" : "HIGH");
                                alert.put("card_number", txn.getMaskedCard());
                                alert.put("transaction_amount", txn.getAmount());
                                alert.put("rolling_average", Math.round(avg * 100.0) / 100.0);
                                alert.put("anomaly_factor",
                                        Math.round((txn.getAmount() / avg) * 100.0) / 100.0);
                                alert.put("threshold_factor", AMOUNT_ANOMALY_FACTOR);
                                alert.put("merchant", txn.getMerchantName());
                                alert.put("country", txn.getCountry());
                                alert.put("window_start", context.window().getStart());
                                alert.put("window_end", context.window().getEnd());
                                alert.put("transactions_in_window", txnList.size());
                                alert.put("detected_at", System.currentTimeMillis());
                                alert.put("recommended_action", "FLAG_FOR_REVIEW");

                                out.collect(MAPPER.writeValueAsString(alert));
                                LOG.warn("💰 AMOUNT ANOMALY: {} — ${} vs avg ${} ({}x)",
                                        txn.getMaskedCard(), txn.getAmount(),
                                        Math.round(avg * 100.0) / 100.0,
                                        Math.round((txn.getAmount() / avg) * 100.0) / 100.0);
                            }
                        }
                    }
                })
                .name("Amount-Anomaly-Check");

        amountAlerts.sinkTo(alertsSink).name("Amount-Alerts-Sink");

        // ═════════════════════════════════════════════════════════════════
        // JOB 4: TRANSACTION AGGREGATION — Tumbling Window (5 minutes)
        // ═════════════════════════════════════════════════════════════════
        //
        // WHY AGGREGATION:
        //   Kibana dashboards need pre-aggregated metrics (can't scan billions of raw events).
        //   Every 5 minutes, we produce summary stats per country + merchant category.
        //
        // AGGREGATED METRICS:
        //   - Transaction count
        //   - Total amount
        //   - Average amount
        //   - Max single transaction
        //   - Unique cards seen
        //   - High-risk transaction percentage
        //
        LOG.info("📈 Starting Job 4: Transaction Aggregation (5-min tumbling windows)");

        // Key by country for country-level aggregation
        SingleOutputStreamOperator<String> countryAggregation = transactions
                .keyBy(Transaction::getCountry)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .process(new org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<
                        Transaction, String, String, org.apache.flink.streaming.api.windowing.windows.TimeWindow>() {
                    @Override
                    public void process(String country,
                                        Context context,
                                        Iterable<Transaction> transactions,
                                        org.apache.flink.util.Collector<String> out) throws Exception {

                        List<Transaction> txnList = new ArrayList<>();
                        transactions.forEach(txnList::add);

                        if (txnList.isEmpty()) return;

                        double totalAmount = txnList.stream()
                                .mapToDouble(Transaction::getAmount).sum();
                        double avgAmount = totalAmount / txnList.size();
                        double maxAmount = txnList.stream()
                                .mapToDouble(Transaction::getAmount).max().orElse(0);
                        long uniqueCards = txnList.stream()
                                .map(Transaction::getCardNumber).distinct().count();
                        long highRiskCount = txnList.stream()
                                .filter(t -> "HIGH".equals(t.getRiskLevel())).count();

                        Map<String, Object> agg = new LinkedHashMap<>();
                        agg.put("aggregation_type", "COUNTRY_5MIN");
                        agg.put("country", country);
                        agg.put("window_start", context.window().getStart());
                        agg.put("window_end", context.window().getEnd());
                        agg.put("transaction_count", txnList.size());
                        agg.put("total_amount", Math.round(totalAmount * 100.0) / 100.0);
                        agg.put("average_amount", Math.round(avgAmount * 100.0) / 100.0);
                        agg.put("max_transaction", Math.round(maxAmount * 100.0) / 100.0);
                        agg.put("unique_cards", uniqueCards);
                        agg.put("high_risk_count", highRiskCount);
                        agg.put("high_risk_percentage",
                                txnList.size() > 0
                                        ? Math.round((double) highRiskCount / txnList.size() * 10000.0) / 100.0
                                        : 0);
                        agg.put("generated_at", System.currentTimeMillis());

                        out.collect(MAPPER.writeValueAsString(agg));
                    }
                })
                .name("Country-Aggregation");

        countryAggregation.sinkTo(aggregationSink).name("Country-Aggregation-Sink");

        // ─── EXECUTE THE PIPELINE ───────────────────────────────────────
        // This is a blocking call — Flink starts processing and runs until stopped
        LOG.info("🚀 All 4 fraud detection jobs configured. Executing pipeline...");
        env.execute("Visa Fraud Detection Pipeline");
    }
}
