#!/bin/bash
# =============================================================================
# KAFKA TOPIC CREATION SCRIPT
# =============================================================================
# Creates all required Kafka topics with appropriate partition counts.
#
# WHY MULTIPLE TOPICS (not one big topic):
#   - Separation of concerns: raw vs enriched vs alerts
#   - Different retention policies per topic
#   - Different consumer groups can read independently
#   - Flink reads ONLY enriched (not raw, not blocked)
#
# PARTITION COUNT:
#   - More partitions = more parallelism (Flink can have 1 consumer per partition)
#   - Key = card_number ensures all txns for same card go to same partition
#   - 6 partitions is good for local dev (matches Flink parallelism)
# =============================================================================

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

echo "══════════════════════════════════════════════════════════════"
echo "📋 Creating Kafka topics..."
echo "   Bootstrap: $KAFKA_BOOTSTRAP"
echo "══════════════════════════════════════════════════════════════"

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka..."
until kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --list > /dev/null 2>&1; do
    sleep 2
done
echo "✅ Kafka is ready"

# ─── CREATE TOPICS ───────────────────────────────────────────────────────────
# transactions.raw — All raw transactions from simulator (before enrichment)
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --create --if-not-exists \
    --topic transactions.raw \
    --partitions 6 \
    --replication-factor 1 \
    --config retention.ms=86400000       # 24 hours retention
echo "  ✅ transactions.raw (6 partitions)"

# transactions.enriched — After Hazelcast enrichment (Flink reads this)
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --create --if-not-exists \
    --topic transactions.enriched \
    --partitions 6 \
    --replication-factor 1 \
    --config retention.ms=86400000
echo "  ✅ transactions.enriched (6 partitions)"

# transactions.blocked — Instantly declined (blocklist hits)
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --create --if-not-exists \
    --topic transactions.blocked \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000      # 7 days (audit trail)
echo "  ✅ transactions.blocked (3 partitions)"

# transactions.alerts — Fraud alerts from Flink
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --create --if-not-exists \
    --topic transactions.alerts \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=2592000000     # 30 days (investigation)
echo "  ✅ transactions.alerts (3 partitions)"

# transactions.aggregated — 5-minute aggregations from Flink
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --create --if-not-exists \
    --topic transactions.aggregated \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000
echo "  ✅ transactions.aggregated (3 partitions)"

echo "══════════════════════════════════════════════════════════════"
echo "✅ All Kafka topics created!"
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --list
echo "══════════════════════════════════════════════════════════════"
