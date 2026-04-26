"""
=============================================================================
TRANSACTION SIMULATOR — Mimics VisaNet Card Transaction Traffic
=============================================================================

PURPOSE:
    Generates realistic credit/debit card transactions at configurable rates.
    Simulates what Visa's authorization network sees in production:
    - Normal everyday purchases (95% of traffic)
    - Suspicious fraud patterns (3% — velocity attacks, geo-hopping)
    - Known stolen/blocked cards (2% — should be instantly declined)

WHY WE NEED THIS:
    In a real Visa environment, billions of transactions flow through daily.
    We can't use real card data (PCI-DSS compliance), so we simulate it.
    This lets us test our entire fraud detection pipeline end-to-end.

HOW IT WORKS:
    1. Picks a card profile (normal, suspicious, or blocked)
    2. Generates a realistic transaction with amount, merchant, location
    3. Publishes to Kafka topic "transactions.raw"
    4. Flink and other downstream services consume from there

CONFIGURABLE:
    - TXN_RATE: transactions per second (default: 50)
    - FRAUD_RATIO: percentage of suspicious transactions (default: 3%)
    - BLOCKED_RATIO: percentage of blocked card transactions (default: 2%)
=============================================================================
"""

import json
import time
import random
import uuid
import os
import logging
from datetime import datetime, timezone
from confluent_kafka import Producer

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("TransactionSimulator")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION — Read from environment variables (12-factor app pattern)
# ─────────────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TXN_RATE = int(os.getenv("TXN_RATE", "50"))           # transactions per second
FRAUD_RATIO = float(os.getenv("FRAUD_RATIO", "0.03"))  # 3% suspicious
BLOCKED_RATIO = float(os.getenv("BLOCKED_RATIO", "0.02"))  # 2% blocked cards
TOPIC = os.getenv("KAFKA_TOPIC", "transactions.raw")

# ─────────────────────────────────────────────────────────────────────────────
# CARD PROFILES — Simulates different cardholder behaviors
# ─────────────────────────────────────────────────────────────────────────────

# Normal cards: everyday spending, consistent location, reasonable amounts
NORMAL_CARDS = [
    {"card_number": "4111111111111111", "card_holder": "Alice Johnson", "home_country": "US", "card_type": "VISA_CREDIT"},
    {"card_number": "4222222222222222", "card_holder": "Bob Williams", "home_country": "US", "card_type": "VISA_DEBIT"},
    {"card_number": "4333333333333333", "card_holder": "Carol Davis", "home_country": "UK", "card_type": "VISA_CREDIT"},
    {"card_number": "4444444444444444", "card_holder": "David Chen", "home_country": "CA", "card_type": "VISA_CREDIT"},
    {"card_number": "4555555555555555", "card_holder": "Eva Martinez", "home_country": "US", "card_type": "VISA_DEBIT"},
    {"card_number": "4666666666666666", "card_holder": "Frank Brown", "home_country": "AU", "card_type": "VISA_CREDIT"},
    {"card_number": "4777777777777777", "card_holder": "Grace Lee", "home_country": "JP", "card_type": "VISA_CREDIT"},
    {"card_number": "4888888888888888", "card_holder": "Henry Wilson", "home_country": "DE", "card_type": "VISA_DEBIT"},
    {"card_number": "4999999999999999", "card_holder": "Irene Taylor", "home_country": "FR", "card_type": "VISA_CREDIT"},
    {"card_number": "4100100100100100", "card_holder": "Jack Anderson", "home_country": "US", "card_type": "VISA_CREDIT"},
]

# Suspicious cards: will trigger fraud detection rules
# These cards generate patterns like rapid-fire transactions, geo-hopping
SUSPICIOUS_CARDS = [
    {"card_number": "4000000000000001", "card_holder": "SUS_VelocityAttacker", "home_country": "US", "card_type": "VISA_CREDIT"},
    {"card_number": "4000000000000002", "card_holder": "SUS_GeoHopper", "home_country": "US", "card_type": "VISA_CREDIT"},
    {"card_number": "4000000000000003", "card_holder": "SUS_HighAmount", "home_country": "UK", "card_type": "VISA_CREDIT"},
]

# Blocked cards: known stolen/compromised — should be instantly declined via Hazelcast blocklist
BLOCKED_CARDS = [
    {"card_number": "4000000000009999", "card_holder": "BLOCKED_StolenCard1", "home_country": "US", "card_type": "VISA_CREDIT"},
    {"card_number": "4000000000009998", "card_holder": "BLOCKED_StolenCard2", "home_country": "RU", "card_type": "VISA_CREDIT"},
    {"card_number": "4000000000009997", "card_holder": "BLOCKED_Compromised", "home_country": "CN", "card_type": "VISA_DEBIT"},
]

# ─────────────────────────────────────────────────────────────────────────────
# MERCHANTS — Realistic merchant data with MCC (Merchant Category Codes)
# MCC codes are used by Visa to classify businesses
# ─────────────────────────────────────────────────────────────────────────────
MERCHANTS = [
    {"id": "MERCH-001", "name": "Amazon.com", "mcc": "5942", "category": "ONLINE_RETAIL", "country": "US", "city": "Seattle"},
    {"id": "MERCH-002", "name": "Walmart Supercenter", "mcc": "5411", "category": "GROCERY", "country": "US", "city": "Dallas"},
    {"id": "MERCH-003", "name": "Shell Gas Station", "mcc": "5541", "category": "FUEL", "country": "US", "city": "Houston"},
    {"id": "MERCH-004", "name": "Starbucks Coffee", "mcc": "5814", "category": "RESTAURANT", "country": "US", "city": "Chicago"},
    {"id": "MERCH-005", "name": "Apple Store", "mcc": "5732", "category": "ELECTRONICS", "country": "US", "city": "San Francisco"},
    {"id": "MERCH-006", "name": "Harrods London", "mcc": "5311", "category": "DEPARTMENT_STORE", "country": "UK", "city": "London"},
    {"id": "MERCH-007", "name": "Tokyo Electronics", "mcc": "5732", "category": "ELECTRONICS", "country": "JP", "city": "Tokyo"},
    {"id": "MERCH-008", "name": "Berlin Pharmacy", "mcc": "5912", "category": "PHARMACY", "country": "DE", "city": "Berlin"},
    {"id": "MERCH-009", "name": "Paris Boutique", "mcc": "5651", "category": "CLOTHING", "country": "FR", "city": "Paris"},
    {"id": "MERCH-010", "name": "Sydney Market", "mcc": "5411", "category": "GROCERY", "country": "AU", "city": "Sydney"},
    {"id": "MERCH-011", "name": "ATM Withdrawal SP", "mcc": "6011", "category": "ATM", "country": "BR", "city": "São Paulo"},
    {"id": "MERCH-012", "name": "Lagos Electronics", "mcc": "5732", "category": "ELECTRONICS", "country": "NG", "city": "Lagos"},
    {"id": "MERCH-013", "name": "Moscow Jewelers", "mcc": "5944", "category": "JEWELRY", "country": "RU", "city": "Moscow"},
    {"id": "MERCH-014", "name": "Dubai Gold Souk", "mcc": "5944", "category": "JEWELRY", "country": "AE", "city": "Dubai"},
]

# ─────────────────────────────────────────────────────────────────────────────
# AMOUNT RANGES — Different spending patterns per merchant category
# WHY: Normal grocery purchases are $5-$200, but electronics can be $50-$3000
# Fraud detection uses these baselines to spot anomalies
# ─────────────────────────────────────────────────────────────────────────────
AMOUNT_RANGES = {
    "GROCERY":          (5.0, 200.0),
    "FUEL":             (15.0, 120.0),
    "RESTAURANT":       (3.0, 150.0),
    "ONLINE_RETAIL":    (10.0, 500.0),
    "ELECTRONICS":      (50.0, 3000.0),
    "DEPARTMENT_STORE": (20.0, 1500.0),
    "PHARMACY":         (5.0, 100.0),
    "CLOTHING":         (15.0, 800.0),
    "ATM":              (20.0, 500.0),
    "JEWELRY":          (100.0, 10000.0),
}

# Country coordinates for geo-location enrichment
COUNTRY_COORDS = {
    "US": (37.0902, -95.7129),
    "UK": (55.3781, -3.4360),
    "CA": (56.1304, -106.3468),
    "AU": (-25.2744, 133.7751),
    "JP": (36.2048, 138.2529),
    "DE": (51.1657, 10.4515),
    "FR": (46.2276, 2.2137),
    "BR": (-14.2350, -51.9253),
    "NG": (9.0820, 8.6753),
    "RU": (61.5240, 105.3188),
    "AE": (23.4241, 53.8478),
    "CN": (35.8617, 104.1954),
    "IN": (20.5937, 78.9629),
}


def mask_card_number(card_number: str) -> str:
    """
    PCI-DSS COMPLIANCE: Never log or transmit full card numbers.
    Only first 4 and last 4 digits are shown. Middle digits are masked.
    Example: 4111111111111111 → 4111XXXXXXXX1111
    """
    if len(card_number) >= 8:
        return card_number[:4] + "X" * (len(card_number) - 8) + card_number[-4:]
    return "XXXX"


def generate_normal_transaction() -> dict:
    """
    Generate a normal, legitimate transaction.
    - Picks a random normal card
    - Picks a merchant (biased toward home country — most people shop locally)
    - Amount is within normal range for that merchant category
    """
    card = random.choice(NORMAL_CARDS)
    # 70% chance merchant is in cardholder's home country (realistic behavior)
    if random.random() < 0.7:
        home_merchants = [m for m in MERCHANTS if m["country"] == card["home_country"]]
        merchant = random.choice(home_merchants) if home_merchants else random.choice(MERCHANTS)
    else:
        merchant = random.choice(MERCHANTS)

    amount_range = AMOUNT_RANGES.get(merchant["category"], (10.0, 500.0))
    amount = round(random.uniform(*amount_range), 2)
    lat, lon = COUNTRY_COORDS.get(merchant["country"], (0.0, 0.0))

    return {
        "transaction_id": str(uuid.uuid4()),
        "card_number": card["card_number"],
        "card_holder": card["card_holder"],
        "card_type": card["card_type"],
        "amount": amount,
        "currency": "USD",
        "merchant_id": merchant["id"],
        "merchant_name": merchant["name"],
        "merchant_category": merchant["category"],
        "mcc": merchant["mcc"],
        "country": merchant["country"],
        "city": merchant["city"],
        "latitude": lat + random.uniform(-0.5, 0.5),
        "longitude": lon + random.uniform(-0.5, 0.5),
        "transaction_type": random.choice(["PURCHASE", "PURCHASE", "PURCHASE", "CONTACTLESS"]),
        "channel": random.choice(["POS", "POS", "ONLINE", "MOBILE"]),
        "risk_flag": "NORMAL",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_time": int(time.time() * 1000),  # epoch milliseconds for Flink
    }


def generate_suspicious_transaction() -> dict:
    """
    Generate a suspicious transaction designed to trigger Flink fraud rules.

    Three fraud patterns:
    1. VELOCITY ATTACK: Same card used 10+ times rapidly (tests tumbling window)
    2. GEO-HOPPING: Same card in different countries within minutes (tests CEP)
    3. HIGH AMOUNT: Amount 5-10x normal average (tests sliding window anomaly)
    """
    pattern = random.choice(["velocity", "geo_hop", "high_amount"])

    if pattern == "velocity":
        # VELOCITY ATTACK: Uses the same suspicious card repeatedly
        # Flink should flag when >5 txns from same card in 60s window
        card = SUSPICIOUS_CARDS[0]
        merchant = random.choice(MERCHANTS)
        amount = round(random.uniform(50, 500), 2)
    elif pattern == "geo_hop":
        # GEO-HOPPING: Same card used in wildly different countries
        # Flink CEP should detect: US → Japan → Russia within minutes = impossible travel
        card = SUSPICIOUS_CARDS[1]
        merchant = random.choice([m for m in MERCHANTS if m["country"] != card["home_country"]])
        amount = round(random.uniform(100, 2000), 2)
    else:
        # HIGH AMOUNT: Abnormally large purchase (anomaly detection)
        # Flink sliding window should flag: amount > 3x rolling average
        card = SUSPICIOUS_CARDS[2]
        merchant = random.choice(MERCHANTS)
        amount = round(random.uniform(5000, 25000), 2)  # Way above normal

    lat, lon = COUNTRY_COORDS.get(merchant["country"], (0.0, 0.0))

    return {
        "transaction_id": str(uuid.uuid4()),
        "card_number": card["card_number"],
        "card_holder": card["card_holder"],
        "card_type": card["card_type"],
        "amount": amount,
        "currency": "USD",
        "merchant_id": merchant["id"],
        "merchant_name": merchant["name"],
        "merchant_category": merchant["category"],
        "mcc": merchant["mcc"],
        "country": merchant["country"],
        "city": merchant["city"],
        "latitude": lat + random.uniform(-0.5, 0.5),
        "longitude": lon + random.uniform(-0.5, 0.5),
        "transaction_type": "PURCHASE",
        "channel": random.choice(["POS", "ONLINE"]),
        "risk_flag": f"SUSPICIOUS_{pattern.upper()}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_time": int(time.time() * 1000),
    }


def generate_blocked_transaction() -> dict:
    """
    Generate a transaction from a known blocked/stolen card.
    These should be INSTANTLY declined by the Hazelcast blocklist lookup
    before even reaching Flink (sub-5ms rejection).
    """
    card = random.choice(BLOCKED_CARDS)
    merchant = random.choice(MERCHANTS)
    amount = round(random.uniform(100, 5000), 2)
    lat, lon = COUNTRY_COORDS.get(merchant["country"], (0.0, 0.0))

    return {
        "transaction_id": str(uuid.uuid4()),
        "card_number": card["card_number"],
        "card_holder": card["card_holder"],
        "card_type": card["card_type"],
        "amount": amount,
        "currency": "USD",
        "merchant_id": merchant["id"],
        "merchant_name": merchant["name"],
        "merchant_category": merchant["category"],
        "mcc": merchant["mcc"],
        "country": merchant["country"],
        "city": merchant["city"],
        "latitude": lat + random.uniform(-0.5, 0.5),
        "longitude": lon + random.uniform(-0.5, 0.5),
        "transaction_type": "PURCHASE",
        "channel": random.choice(["POS", "ONLINE", "ATM"]),
        "risk_flag": "BLOCKED_CARD",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_time": int(time.time() * 1000),
    }


def kafka_delivery_report(err, msg):
    """
    Callback for Kafka producer delivery reports.
    WHY: Kafka is async — we need to confirm messages actually reached the broker.
    In production, failed deliveries would trigger alerts.
    """
    if err is not None:
        logger.error(f"❌ Delivery failed: {err}")
    else:
        logger.debug(f"✅ Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def run_simulator():
    """
    Main simulator loop.
    Runs continuously, generating transactions at the configured rate.
    Each transaction is published to Kafka topic "transactions.raw".
    """
    # ─── KAFKA PRODUCER CONFIGURATION ─────────────────────────────────────
    # WHY these settings:
    # - linger.ms=5: batch messages for 5ms before sending (throughput optimization)
    # - batch.size=32768: batch up to 32KB per partition
    # - compression.type=snappy: compress batches (Visa-scale = lots of data)
    # - acks=all: wait for all replicas to confirm (no data loss)
    producer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "transaction-simulator",
        "linger.ms": 5,
        "batch.size": 32768,
        "compression.type": "snappy",
        "acks": "all",
    }

    logger.info("=" * 70)
    logger.info("🏦 VISA TRANSACTION SIMULATOR STARTING")
    logger.info(f"   Kafka:         {KAFKA_BOOTSTRAP}")
    logger.info(f"   Topic:         {TOPIC}")
    logger.info(f"   Rate:          {TXN_RATE} txn/sec")
    logger.info(f"   Fraud ratio:   {FRAUD_RATIO * 100}%")
    logger.info(f"   Blocked ratio: {BLOCKED_RATIO * 100}%")
    logger.info("=" * 70)

    producer = Producer(producer_config)

    txn_count = 0
    start_time = time.time()

    try:
        while True:
            batch_start = time.time()

            for _ in range(TXN_RATE):
                # ─── DECIDE TRANSACTION TYPE ───────────────────────────────
                # Roll a random number to pick which type of transaction to generate
                roll = random.random()

                if roll < BLOCKED_RATIO:
                    txn = generate_blocked_transaction()
                elif roll < BLOCKED_RATIO + FRAUD_RATIO:
                    txn = generate_suspicious_transaction()
                else:
                    txn = generate_normal_transaction()

                # ─── PUBLISH TO KAFKA ─────────────────────────────────────
                # Key = card_number ensures all txns for same card go to same partition
                # WHY: Flink needs all events for a card in order (per-key ordering)
                producer.produce(
                    topic=TOPIC,
                    key=txn["card_number"],
                    value=json.dumps(txn),
                    callback=kafka_delivery_report,
                )

                txn_count += 1

                # Log every 100th transaction for visibility
                if txn_count % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = txn_count / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"📊 Sent {txn_count} txns | "
                        f"Rate: {rate:.0f}/sec | "
                        f"Last: {mask_card_number(txn['card_number'])} "
                        f"${txn['amount']:.2f} @ {txn['merchant_name']} "
                        f"[{txn['risk_flag']}]"
                    )

            # ─── FLUSH + RATE LIMITING ────────────────────────────────────
            # Flush ensures all buffered messages are sent to Kafka
            producer.flush()

            # Sleep to maintain target rate (1 second between batches)
            batch_elapsed = time.time() - batch_start
            if batch_elapsed < 1.0:
                time.sleep(1.0 - batch_elapsed)

    except KeyboardInterrupt:
        logger.info(f"\n🛑 Simulator stopped. Total transactions sent: {txn_count}")
    finally:
        # ─── CLEAN SHUTDOWN ───────────────────────────────────────────────
        # Flush remaining messages before exit (don't lose data)
        remaining = producer.flush(timeout=10)
        if remaining > 0:
            logger.warning(f"⚠️  {remaining} messages were not delivered")
        logger.info("✅ Producer closed cleanly")


if __name__ == "__main__":
    run_simulator()
