END-TO-END PROOF TEST — All via UI
Step 1: See raw transactions being produced (Kafka → Kibana)
Open Kibana → http://localhost:5601

☰ → Discover
If you haven't created data views yet:
☰ → Stack Management → Data Views → Create data view
Name: All Transactions | Pattern: transactions-* | Timestamp: @timestamp → Save
Create another: Name: Blocked Transactions | Pattern: transactions-blocked-* | Timestamp: @timestamp → Save
Go back to Discover → select "All Transactions"
Set time range: Last 15 minutes (top right)
Hit Refresh — you'll see hundreds of docs. Note the count (e.g., 2,500)
Wait 30 seconds → hit Refresh again → count increased (e.g., 2,650)
This proves: Simulator → Kafka → Enrichment → Logstash → Elasticsearch is flowing live.

Expand any document — you'll see the enriched fields:
card_number: 4532XXXXXXXX1234
amount: 247.50
merchant_name: Amazon, Walmart, etc.
merchant_category: RETAIL, GROCERY, etc.
country, city, currency
risk_score: a number showing fraud risk
is_blocked: false (this is a normal transaction)
Step 2: Prove fraud detection — See BLOCKED transactions
Stay in Kibana Discover
Switch data view (top-left dropdown) to "Blocked Transactions"
Refresh — you should see 40+ blocked transactions
Expand one — look for:
is_blocked: true
block_reason: one of:
REPORTED_STOLEN — card reported stolen
COMPROMISED_DATA_BREACH — card was in a data breach
FRAUD_PATTERN_DETECTED — flagged by fraud algorithm
card_number: one of the blocklisted cards (4000XXXXXXXX9997, 9998, 9999)
This proves: Enrichment Service checked the card against Hazelcast blocklist → found it → blocked it → sent to transactions.blocked topic → Logstash → Elasticsearch.

Step 3: See the blocklist cache in Hazelcast
Open Hazelcast MC → http://localhost:8085

You should see the cluster with 2 members (green)
Click "Storage" → "Maps" (left sidebar)
Click card-blocklist
Entry Count: shows how many cards are blocklisted
Hits / Misses: shows real-time lookups from the enrichment service
Every time enrichment processes a transaction, it does a get() on this map
High hit ratio = the NearCache is working (sub-millisecond lookups)
Click merchant-registry
Shows merchant metadata used for enrichment (names, categories, risk scores)
Go to "Clients" section
You'll see enrichment-service connected — this is our Python service doing the lookups
This proves: Hazelcast is storing the blocklist + merchant data, enrichment is actively querying it.

Step 4: See Flink processing the stream in real-time
Open Flink UI → http://localhost:8081

Click "Running Jobs" → click "Visa Fraud Detection Pipeline"

You'll see the visual DAG — the boxes and arrows showing data flow

Look at each operator box:

Source: KafkaSource — has "Records Sent" increasing
Fraud detectors — have "Records Received" and "Records Sent" increasing
Sink — writing results back to Kafka
Click on the KafkaSource box → under metrics you'll see:

numRecordsIn — total records consumed from transactions.enriched
numRecordsOut — records passed to fraud detection operators
These numbers keep ticking up = Flink is reading enriched transactions from Kafka
Click "Checkpoints" tab:

Shows checkpoints completing every ~30 seconds
State Size — memory used for stateful fraud detection
This proves exactly-once processing (if Flink crashes, it recovers from checkpoint)
This proves: Flink is consuming enriched transactions, running 4 fraud detection algorithms, and writing alerts/aggregations back to Kafka.

Step 5: See Prometheus scraping metrics
Open Prometheus → http://localhost:9090

Click Status → Targets
Look at which targets show UP (green) — these are the services Prometheus monitors
Go back to the query box, type: up → click Execute → click Graph tab
You see a flat line at 1.0 for each service — means all healthy
Try: process_resident_memory_bytes → Execute → Graph
Shows memory usage of each service over time
Elasticsearch will be the highest (~512MB), Hazelcast next
This proves: Prometheus is actively scraping and collecting metrics from all services.

Step 6: See Grafana visualize everything
Open Grafana → http://localhost:3000 (login: admin/admin)

☰ → Connections → Data sources → click Prometheus → scroll down → Test
Should say "Data source is working"
☰ → Explore
Select Prometheus data source
Type up in the metric field → Run query
You see a table/graph showing all services as 1 (up) or 0 (down)
This proves: Grafana is connected to Prometheus and can visualize all pipeline metrics.

Step 7: Watch it live — the real-time proof
Go back to Kibana Discover with "All Transactions" selected. Do this:

Note the document count (e.g., 3,000)
Click the auto-refresh button (clock icon, top right)
Set it to every 5 seconds
Watch the count increase live — every 5 seconds you'll see ~40-50 new documents
This is the ultimate proof — you're watching the entire pipeline work in real-time:

Simulator generates fake Visa card transactions
Kafka delivers them
Enrichment checks Hazelcast blocklist, adds merchant data
Flink runs fraud detection algorithms
Logstash pipes everything to Elasticsearch
Kibana shows it live

##some extra info or guide 

Now open in your browser:

Prometheus Targets → http://localhost:9090/targets — All 3 targets should show UP (green)
Grafana Dashboard → http://localhost:3000 (admin/admin) → Go to Dashboards → Fraud Detection Pipeline — You 
should see 
9 panels with real Flink metrics (heap usage, CPU load, task slots, records in/out, job uptime, checkpoint
 duration, restarts)
Grafana Metrics Explorer → Click Explore → type flink_ in the metric field — you'll see 
dozens of available Flink metrics

[Simulator]           → "I generated a $247.50 transaction at Walmart"
    ↓ Kafka (transactions.raw)
[Enrichment Service]  → "Let me check Hazelcast... card NOT on blocklist, adding merchant data"
    ↓ Kafka (transactions.enriched)
[Flink]               → "Running velocity/geo-hop/amount checks... looks normal"
[Logstash]            → "Indexing to Elasticsearch"
    ↓
[Elasticsearch]       → "Stored. 3,001 documents now."
[Kibana]              → "Showing it on your screen!"

--- Meanwhile, a BLOCKED card ---

[Simulator]           → "Card 4000XXXXXXXX9999 trying to buy $500..."
    ↓ Kafka (transactions.raw)
[Enrichment Service]  → "Checking Hazelcast... 🚫 FOUND on blocklist: REPORTED_STOLEN!"
    ↓ Kafka (transactions.blocked)
[Logstash]            → "Indexing to blocked index"
[Kibana]              → "Showing in Blocked Transactions view!"