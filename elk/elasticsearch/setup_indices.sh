#!/bin/bash
# =============================================================================
# ELASTICSEARCH INDEX SETUP SCRIPT
# =============================================================================
# Run this AFTER Elasticsearch is healthy to:
#   1. Create index templates (field mappings)
#   2. Create ILM (Index Lifecycle Management) policy
#   3. Create initial indices
#
# WHY ILM:
#   Transaction data grows fast. ILM automatically manages index lifecycle:
#   - HOT phase (0-7 days): Fast SSD, full replicas — active queries
#   - WARM phase (7-30 days): Cheaper storage, read-only — historical queries
#   - DELETE phase (30+ days): Automatically deleted — regulatory compliance
# =============================================================================

ES_HOST="${ELASTICSEARCH_HOST:-http://localhost:9200}"

echo "══════════════════════════════════════════════════════════════"
echo "📦 Setting up Elasticsearch indices and templates..."
echo "   Host: $ES_HOST"
echo "══════════════════════════════════════════════════════════════"

# Wait for Elasticsearch to be ready
echo "⏳ Waiting for Elasticsearch..."
until curl -s "$ES_HOST/_cluster/health" | grep -q '"status":"green"\|"status":"yellow"'; do
    sleep 2
done
echo "✅ Elasticsearch is ready"

# ─── CREATE ILM POLICY ──────────────────────────────────────────────────────
echo "📋 Creating ILM policy: transactions-ilm-policy"
curl -s -X PUT "$ES_HOST/_ilm/policy/transactions-ilm-policy" \
  -H "Content-Type: application/json" \
  -d '{
    "policy": {
      "phases": {
        "hot": {
          "min_age": "0ms",
          "actions": {
            "rollover": {
              "max_age": "7d",
              "max_size": "5gb"
            }
          }
        },
        "warm": {
          "min_age": "7d",
          "actions": {
            "readonly": {},
            "forcemerge": {
              "max_num_segments": 1
            }
          }
        },
        "delete": {
          "min_age": "30d",
          "actions": {
            "delete": {}
          }
        }
      }
    }
  }'
echo ""

# ─── CREATE INDEX TEMPLATES ──────────────────────────────────────────────────
echo "📋 Creating index template: transactions"
curl -s -X PUT "$ES_HOST/_index_template/transactions-template" \
  -H "Content-Type: application/json" \
  -d '{
    "index_patterns": ["transactions-*"],
    "template": {
      "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "index.lifecycle.name": "transactions-ilm-policy"
      },
      "mappings": {
        "properties": {
          "transaction_id": { "type": "keyword" },
          "card_number": { "type": "keyword" },
          "masked_card": { "type": "keyword" },
          "amount": { "type": "double" },
          "country": { "type": "keyword" },
          "city": { "type": "text" },
          "merchant_name": { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
          "merchant_category": { "type": "keyword" },
          "channel": { "type": "keyword" },
          "risk_flag": { "type": "keyword" },
          "location": { "type": "geo_point" },
          "timestamp": { "type": "date" },
          "amount_bucket": { "type": "keyword" },
          "is_international": { "type": "boolean" },
          "enrichment": {
            "properties": {
              "risk_score": { "type": "double" },
              "risk_level": { "type": "keyword" },
              "country_risk_tier": { "type": "keyword" },
              "blocked": { "type": "boolean" }
            }
          }
        }
      }
    }
  }'
echo ""

echo "📋 Creating index template: fraud-alerts"
curl -s -X PUT "$ES_HOST/_index_template/fraud-alerts-template" \
  -H "Content-Type: application/json" \
  -d '{
    "index_patterns": ["fraud-alerts*"],
    "template": {
      "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
      "mappings": {
        "properties": {
          "alert_type": { "type": "keyword" },
          "severity": { "type": "keyword" },
          "card_number": { "type": "keyword" },
          "recommended_action": { "type": "keyword" },
          "detected_at": { "type": "long" }
        }
      }
    }
  }'
echo ""

echo "📋 Creating index template: transaction-aggregations"
curl -s -X PUT "$ES_HOST/_index_template/aggregations-template" \
  -H "Content-Type: application/json" \
  -d '{
    "index_patterns": ["transaction-aggregations-*"],
    "template": {
      "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
      "mappings": {
        "properties": {
          "aggregation_type": { "type": "keyword" },
          "country": { "type": "keyword" },
          "transaction_count": { "type": "integer" },
          "total_amount": { "type": "double" },
          "average_amount": { "type": "double" }
        }
      }
    }
  }'
echo ""

echo "══════════════════════════════════════════════════════════════"
echo "✅ Elasticsearch setup complete!"
echo "══════════════════════════════════════════════════════════════"
