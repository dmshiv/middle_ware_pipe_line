"""
=============================================================================
ENRICHMENT SERVICE — Transaction Enrichment + Hazelcast Blocklist Check
=============================================================================

PURPOSE:
    Sits between Kafka "transactions.raw" and "transactions.enriched".
    Every transaction flows through here BEFORE reaching Flink.

WHAT IT DOES:
    1. BLOCKLIST CHECK (Hazelcast): Is this card number on the hot card blocklist?
       - If YES → instantly decline, publish to "transactions.blocked" topic
       - If NO  → continue to enrichment
       - WHY HAZELCAST: Sub-5ms lookup. When a card is reported stolen,
         ALL authorization nodes must know INSTANTLY. Hazelcast's distributed
         IMap with Near Cache + EntryListeners gives push-based updates.

    2. MERCHANT ENRICHMENT (Hazelcast): Look up merchant details from the
       distributed cache (merchant registry IMap).
       - Adds: risk_score, merchant_history, fraud_rate for that merchant
       - WHY CACHE: Enrichment data doesn't change often. Caching in Hazelcast
         avoids hitting a database on every single transaction (65K+ TPS at Visa).

    3. GEO ENRICHMENT: Resolve country/city to region, timezone, risk_tier.
       - High-risk countries get higher base risk scores.

    4. PUBLISH: Enriched transaction → "transactions.enriched" Kafka topic
       for Flink to consume.

WHY THIS IS A SEPARATE SERVICE:
    - Separation of concerns: enrichment logic vs fraud detection logic
    - Can scale independently (enrichment is I/O bound, Flink is CPU bound)
    - Hazelcast client connections are managed here, not scattered everywhere
=============================================================================
"""

import json
import os
import time
import logging
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer, KafkaError

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("EnrichmentService")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
HAZELCAST_HOST = os.getenv("HAZELCAST_HOST", "localhost")
HAZELCAST_PORT = int(os.getenv("HAZELCAST_PORT", "5701"))
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "transactions.raw")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "transactions.enriched")
BLOCKED_TOPIC = os.getenv("BLOCKED_TOPIC", "transactions.blocked")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "transactions.alerts")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "enrichment-service-group")

# ─────────────────────────────────────────────────────────────────────────────
# HAZELCAST CLIENT — Connects to the Hazelcast cluster
# ─────────────────────────────────────────────────────────────────────────────
# WHY HAZELCAST OVER REDIS FOR THIS USE CASE:
#   1. Near Cache: Frequently accessed blocklist entries cached locally in THIS
#      process's memory. Zero network latency for hot data.
#   2. EntryListener: When a new card is added to blocklist, ALL connected
#      clients get notified via push (not poll). Critical for fraud prevention.
#   3. Distributed computing: Can run server-side processing on the cluster
#      (EntryProcessor) — filter/aggregate without moving data over network.
#   4. Multi-map support: Different data structures (IMap, ISet, IQueue) in
#      one cluster. Redis needs separate data modeling.
# ─────────────────────────────────────────────────────────────────────────────

import hazelcast


def create_hazelcast_client():
    """
    Create and configure the Hazelcast client.

    NEAR CACHE CONFIG:
    - max_idle_seconds=60: Evict entries not accessed for 60s
    - time_to_live_seconds=120: Force refresh every 2 min (even if accessed)
    - invalidate_on_change=True: Server pushes invalidation when data changes
      (THIS is the key feature — instant blocklist propagation)
    """
    try:
        blocklist_nc = hazelcast.config.NearCacheConfig()
        blocklist_nc.max_idle = 60
        blocklist_nc.time_to_live = 120
        blocklist_nc.invalidate_on_change = True

        merchant_nc = hazelcast.config.NearCacheConfig()
        merchant_nc.max_idle = 300
        merchant_nc.time_to_live = 600
        merchant_nc.invalidate_on_change = True

        client = hazelcast.HazelcastClient(
            cluster_name="visa-fraud-cluster",
            cluster_members=[f"{HAZELCAST_HOST}:{HAZELCAST_PORT}"],
            # Near Cache for the blocklist — sub-millisecond lookups
            near_caches={
                "hot-card-blocklist": blocklist_nc,
                "merchant-registry": merchant_nc,
            },
        )
        logger.info(f"✅ Connected to Hazelcast cluster at {HAZELCAST_HOST}:{HAZELCAST_PORT}")
        return client
    except Exception as e:
        logger.error(f"❌ Failed to connect to Hazelcast: {e}")
        return None


def initialize_blocklist(blocklist_map):
    """
    Pre-populate the hot card blocklist with known stolen/compromised cards.

    In production, this would be fed by:
    - Bank fraud reports (card reported stolen)
    - Visa's neural network risk scores (card flagged as compromised)
    - Law enforcement requests (card used in crime)
    - Chargeback patterns (too many disputes = block the card)

    Data stored: card_number → {reason, blocked_at, reported_by, severity}
    """
    blocked_cards = {
        "4000000000009999": {
            "reason": "REPORTED_STOLEN",
            "blocked_at": datetime.now(timezone.utc).isoformat(),
            "reported_by": "ISSUER_BANK_OF_AMERICA",
            "severity": "CRITICAL",
        },
        "4000000000009998": {
            "reason": "COMPROMISED_DATA_BREACH",
            "blocked_at": datetime.now(timezone.utc).isoformat(),
            "reported_by": "VISA_RISK_ENGINE",
            "severity": "HIGH",
        },
        "4000000000009997": {
            "reason": "FRAUD_PATTERN_DETECTED",
            "blocked_at": datetime.now(timezone.utc).isoformat(),
            "reported_by": "FLINK_CEP_ENGINE",
            "severity": "HIGH",
        },
    }

    for card, details in blocked_cards.items():
        blocklist_map.put(card, json.dumps(details))

    logger.info(f"🔒 Initialized blocklist with {len(blocked_cards)} blocked cards")


def initialize_merchant_registry(merchant_map):
    """
    Pre-populate the merchant registry cache.

    WHY IN HAZELCAST:
    - Merchant data is read on EVERY transaction (enrichment)
    - Rarely changes (maybe daily updates)
    - Perfect for distributed cache: read-heavy, write-light
    """
    merchants = {
        "MERCH-001": {"name": "Amazon.com", "risk_score": 0.1, "fraud_rate": 0.002, "verified": True},
        "MERCH-002": {"name": "Walmart Supercenter", "risk_score": 0.05, "fraud_rate": 0.001, "verified": True},
        "MERCH-003": {"name": "Shell Gas Station", "risk_score": 0.15, "fraud_rate": 0.008, "verified": True},
        "MERCH-004": {"name": "Starbucks Coffee", "risk_score": 0.03, "fraud_rate": 0.0005, "verified": True},
        "MERCH-005": {"name": "Apple Store", "risk_score": 0.08, "fraud_rate": 0.003, "verified": True},
        "MERCH-006": {"name": "Harrods London", "risk_score": 0.1, "fraud_rate": 0.004, "verified": True},
        "MERCH-007": {"name": "Tokyo Electronics", "risk_score": 0.12, "fraud_rate": 0.005, "verified": True},
        "MERCH-008": {"name": "Berlin Pharmacy", "risk_score": 0.04, "fraud_rate": 0.001, "verified": True},
        "MERCH-009": {"name": "Paris Boutique", "risk_score": 0.09, "fraud_rate": 0.003, "verified": True},
        "MERCH-010": {"name": "Sydney Market", "risk_score": 0.06, "fraud_rate": 0.002, "verified": True},
        "MERCH-011": {"name": "ATM Withdrawal SP", "risk_score": 0.3, "fraud_rate": 0.02, "verified": True},
        "MERCH-012": {"name": "Lagos Electronics", "risk_score": 0.4, "fraud_rate": 0.03, "verified": False},
        "MERCH-013": {"name": "Moscow Jewelers", "risk_score": 0.35, "fraud_rate": 0.025, "verified": False},
        "MERCH-014": {"name": "Dubai Gold Souk", "risk_score": 0.25, "fraud_rate": 0.015, "verified": True},
    }

    for merchant_id, details in merchants.items():
        merchant_map.put(merchant_id, json.dumps(details))

    logger.info(f"🏪 Initialized merchant registry with {len(merchants)} merchants")


# ─────────────────────────────────────────────────────────────────────────────
# GEO-RISK TIERS — Countries classified by fraud risk
# WHY: Visa and all card networks maintain country risk classifications.
# Transactions from high-risk countries get extra scrutiny.
# ─────────────────────────────────────────────────────────────────────────────
COUNTRY_RISK_TIERS = {
    "US": {"tier": "LOW", "risk_multiplier": 1.0, "region": "NORTH_AMERICA", "timezone": "UTC-5"},
    "CA": {"tier": "LOW", "risk_multiplier": 1.0, "region": "NORTH_AMERICA", "timezone": "UTC-5"},
    "UK": {"tier": "LOW", "risk_multiplier": 1.0, "region": "EUROPE", "timezone": "UTC+0"},
    "DE": {"tier": "LOW", "risk_multiplier": 1.0, "region": "EUROPE", "timezone": "UTC+1"},
    "FR": {"tier": "LOW", "risk_multiplier": 1.1, "region": "EUROPE", "timezone": "UTC+1"},
    "JP": {"tier": "LOW", "risk_multiplier": 1.0, "region": "ASIA_PACIFIC", "timezone": "UTC+9"},
    "AU": {"tier": "LOW", "risk_multiplier": 1.0, "region": "ASIA_PACIFIC", "timezone": "UTC+10"},
    "AE": {"tier": "MEDIUM", "risk_multiplier": 1.3, "region": "MIDDLE_EAST", "timezone": "UTC+4"},
    "BR": {"tier": "MEDIUM", "risk_multiplier": 1.5, "region": "SOUTH_AMERICA", "timezone": "UTC-3"},
    "CN": {"tier": "MEDIUM", "risk_multiplier": 1.4, "region": "ASIA_PACIFIC", "timezone": "UTC+8"},
    "IN": {"tier": "MEDIUM", "risk_multiplier": 1.3, "region": "ASIA_PACIFIC", "timezone": "UTC+5.5"},
    "RU": {"tier": "HIGH", "risk_multiplier": 2.0, "region": "EUROPE", "timezone": "UTC+3"},
    "NG": {"tier": "HIGH", "risk_multiplier": 2.5, "region": "AFRICA", "timezone": "UTC+1"},
}


def mask_card_number(card_number: str) -> str:
    """PCI-DSS: Mask card number for logging."""
    if len(card_number) >= 8:
        return card_number[:4] + "X" * (len(card_number) - 8) + card_number[-4:]
    return "XXXX"


def compute_risk_score(txn: dict, merchant_info: dict, country_risk: dict) -> float:
    """
    Compute a composite risk score for the transaction (0.0 = safe, 1.0 = definite fraud).

    FACTORS:
    - Merchant fraud rate (historical)
    - Country risk tier (geo-risk)
    - Transaction amount (higher = riskier)
    - Channel (online = riskier than POS)
    - Time of day (late night = riskier)

    WHY A COMPOSITE SCORE:
    No single factor determines fraud. A $5000 purchase at a verified US merchant
    is very different from a $5000 purchase at an unverified merchant in a high-risk
    country. The composite score captures this nuance.
    """
    score = 0.0

    # Factor 1: Merchant fraud rate (0-40% of score)
    if merchant_info:
        merchant_data = json.loads(merchant_info) if isinstance(merchant_info, str) else merchant_info
        score += merchant_data.get("fraud_rate", 0.01) * 10  # Scale up
        if not merchant_data.get("verified", True):
            score += 0.15  # Unverified merchants are riskier

    # Factor 2: Country risk (0-25% of score)
    score += (country_risk.get("risk_multiplier", 1.0) - 1.0) * 0.2

    # Factor 3: Amount anomaly (0-20% of score)
    amount = txn.get("amount", 0)
    if amount > 5000:
        score += 0.2
    elif amount > 2000:
        score += 0.1
    elif amount > 1000:
        score += 0.05

    # Factor 4: Channel risk (0-10% of score)
    if txn.get("channel") == "ONLINE":
        score += 0.05  # Card-not-present = higher risk (no chip/PIN verification)

    # Factor 5: Time of day risk (0-5% of score)
    try:
        hour = datetime.fromisoformat(txn["timestamp"]).hour
        if 1 <= hour <= 5:  # Late night transactions are riskier
            score += 0.05
    except (KeyError, ValueError):
        pass

    return min(round(score, 4), 1.0)


def enrich_transaction(txn: dict, blocklist_map, merchant_map) -> tuple:
    """
    Main enrichment pipeline for a single transaction.

    Returns: (enriched_txn, is_blocked)
      - enriched_txn: transaction with additional fields
      - is_blocked: True if card is on blocklist

    ENRICHMENT STEPS:
    1. Blocklist check (Hazelcast Near Cache — sub-1ms)
    2. Merchant data lookup (Hazelcast Near Cache)
    3. Geo-risk enrichment (in-memory dict)
    4. Risk score computation
    5. Card number masking (PCI-DSS)
    """
    card_number = txn.get("card_number", "")

    # ─── STEP 1: BLOCKLIST CHECK (FASTEST PATH) ──────────────────────────
    # This MUST be the first check. Blocked cards should be rejected
    # before ANY processing happens (saves compute resources).
    block_check_start = time.time()
    blocked_info = blocklist_map.get(card_number)
    block_check_ms = (time.time() - block_check_start) * 1000

    if blocked_info:
        blocked_data = json.loads(blocked_info) if isinstance(blocked_info, str) else blocked_info
        txn["enrichment"] = {
            "blocked": True,
            "block_reason": blocked_data.get("reason", "UNKNOWN"),
            "block_severity": blocked_data.get("severity", "HIGH"),
            "block_check_latency_ms": round(block_check_ms, 2),
            "decision": "INSTANT_DECLINE",
            "enriched_at": datetime.now(timezone.utc).isoformat(),
        }
        txn["masked_card"] = mask_card_number(card_number)
        logger.warning(
            f"🚫 BLOCKED: {mask_card_number(card_number)} | "
            f"Reason: {blocked_data.get('reason')} | "
            f"Lookup: {block_check_ms:.2f}ms"
        )
        return txn, True

    # ─── STEP 2: MERCHANT ENRICHMENT ─────────────────────────────────────
    merchant_id = txn.get("merchant_id", "")
    merchant_info = merchant_map.get(merchant_id) if merchant_id else None

    # ─── STEP 3: GEO-RISK ENRICHMENT ─────────────────────────────────────
    country = txn.get("country", "US")
    country_risk = COUNTRY_RISK_TIERS.get(country, {"tier": "UNKNOWN", "risk_multiplier": 1.5, "region": "UNKNOWN"})

    # ─── STEP 4: RISK SCORE COMPUTATION ──────────────────────────────────
    risk_score = compute_risk_score(txn, merchant_info, country_risk)

    # ─── STEP 5: BUILD ENRICHED TRANSACTION ──────────────────────────────
    txn["enrichment"] = {
        "blocked": False,
        "risk_score": risk_score,
        "risk_level": "HIGH" if risk_score > 0.5 else "MEDIUM" if risk_score > 0.2 else "LOW",
        "country_risk_tier": country_risk["tier"],
        "country_risk_multiplier": country_risk["risk_multiplier"],
        "region": country_risk.get("region", "UNKNOWN"),
        "merchant_verified": True,
        "block_check_latency_ms": round(block_check_ms, 2),
        "enriched_at": datetime.now(timezone.utc).isoformat(),
    }

    # Add merchant info if available
    if merchant_info:
        merchant_data = json.loads(merchant_info) if isinstance(merchant_info, str) else merchant_info
        txn["enrichment"]["merchant_fraud_rate"] = merchant_data.get("fraud_rate", 0)
        txn["enrichment"]["merchant_risk_score"] = merchant_data.get("risk_score", 0)
        txn["enrichment"]["merchant_verified"] = merchant_data.get("verified", False)

    txn["masked_card"] = mask_card_number(card_number)

    return txn, False


def run_enrichment_service():
    """
    Main service loop.
    Consumes from "transactions.raw", enriches, publishes to "transactions.enriched"
    or "transactions.blocked".
    """
    # ─── KAFKA CONSUMER CONFIG ────────────────────────────────────────────
    consumer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "latest",       # Start from latest (don't replay old txns)
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,
        "max.poll.interval.ms": 300000,
    }

    # ─── KAFKA PRODUCER CONFIG ────────────────────────────────────────────
    producer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "enrichment-service",
        "linger.ms": 5,
        "compression.type": "snappy",
        "acks": "all",
    }

    logger.info("=" * 70)
    logger.info("🔍 ENRICHMENT SERVICE STARTING")
    logger.info(f"   Kafka:      {KAFKA_BOOTSTRAP}")
    logger.info(f"   Hazelcast:  {HAZELCAST_HOST}:{HAZELCAST_PORT}")
    logger.info(f"   Input:      {INPUT_TOPIC}")
    logger.info(f"   Output:     {OUTPUT_TOPIC}")
    logger.info(f"   Blocked:    {BLOCKED_TOPIC}")
    logger.info("=" * 70)

    # ─── CONNECT TO HAZELCAST ─────────────────────────────────────────────
    hz_client = create_hazelcast_client()
    if not hz_client:
        logger.error("❌ Cannot start without Hazelcast. Exiting.")
        return

    blocklist_map = hz_client.get_map("hot-card-blocklist").blocking()
    merchant_map = hz_client.get_map("merchant-registry").blocking()

    # Initialize data
    initialize_blocklist(blocklist_map)
    initialize_merchant_registry(merchant_map)

    # Write health marker for Docker healthcheck
    with open("/tmp/healthy", "w") as f:
        f.write("ready")

    # ─── START CONSUMING ──────────────────────────────────────────────────
    consumer = Consumer(consumer_config)
    producer = Producer(producer_config)
    consumer.subscribe([INPUT_TOPIC])

    processed = 0
    blocked = 0
    start_time = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                txn = json.loads(msg.value().decode("utf-8"))
                enriched_txn, is_blocked = enrich_transaction(txn, blocklist_map, merchant_map)

                if is_blocked:
                    # ─── BLOCKED: Send to blocked topic ────────────────────
                    producer.produce(
                        topic=BLOCKED_TOPIC,
                        key=msg.key(),
                        value=json.dumps(enriched_txn),
                    )
                    blocked += 1
                else:
                    # ─── ENRICHED: Send to enriched topic for Flink ────────
                    producer.produce(
                        topic=OUTPUT_TOPIC,
                        key=msg.key(),
                        value=json.dumps(enriched_txn),
                    )

                processed += 1

                # Log stats every 50 transactions
                if processed % 50 == 0:
                    elapsed = time.time() - start_time
                    rate = processed / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"📊 Processed: {processed} | "
                        f"Blocked: {blocked} | "
                        f"Rate: {rate:.0f}/sec | "
                        f"Risk: {enriched_txn.get('enrichment', {}).get('risk_level', '?')}"
                    )

                producer.poll(0)  # Trigger delivery callbacks

            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in message: {e}")
            except Exception as e:
                logger.error(f"Error processing transaction: {e}", exc_info=True)

    except KeyboardInterrupt:
        logger.info(f"\n🛑 Enrichment service stopped. Processed: {processed}, Blocked: {blocked}")
    finally:
        consumer.close()
        producer.flush(timeout=10)
        if hz_client:
            hz_client.shutdown()
        logger.info("✅ Enrichment service shut down cleanly")


if __name__ == "__main__":
    run_enrichment_service()
