#!/bin/bash
# ================================================================
# create_topics.sh
# Creates all Kafka topics with correct partitioning,
# retention and log compaction settings.
#
# Retention:
#   raw topics:       7 days  (604800000 ms)
#   aggregate topics: 30 days (2592000000 ms)
#
# Log compaction:
#   aggregate topics: cleanup.policy=compact
#   raw topics:       cleanup.policy=delete
# ================================================================

echo "Creating Kafka topics..."

# ── Raw topics (7 days retention, delete policy) ─────────────────
docker exec kafka kafka-topics \
  --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --topic polymarket-predictions-raw \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete \
  --config compression.type=snappy

docker exec kafka kafka-topics \
  --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --topic weather-actuals-raw \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete \
  --config compression.type=snappy

# ── Aggregate topics (30 days retention, compact policy) ─────────
docker exec kafka kafka-topics \
  --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --topic market-weather-correlations \
  --config retention.ms=2592000000 \
  --config cleanup.policy=delete \
  --config compression.type=snappy

docker exec kafka kafka-topics \
  --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --topic market-accuracy-aggregates \
  --config retention.ms=2592000000 \
  --config cleanup.policy=compact \
  --config compression.type=snappy

docker exec kafka kafka-topics \
  --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --topic arbitrage-alerts \
  --config retention.ms=2592000000 \
  --config cleanup.policy=compact \
  --config compression.type=snappy

# ── Update configs for existing topics ───────────────────────────
echo "Updating configs for existing topics..."

docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --alter --entity-type topics \
  --entity-name polymarket-predictions-raw \
  --add-config retention.ms=604800000,cleanup.policy=delete

docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --alter --entity-type topics \
  --entity-name weather-actuals-raw \
  --add-config retention.ms=604800000,cleanup.policy=delete

docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --alter --entity-type topics \
  --entity-name market-weather-correlations \
  --add-config retention.ms=2592000000,cleanup.policy=delete

docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --alter --entity-type topics \
  --entity-name market-accuracy-aggregates \
  --add-config retention.ms=2592000000,cleanup.policy=compact

docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --alter --entity-type topics \
  --entity-name arbitrage-alerts \
  --add-config retention.ms=2592000000,cleanup.policy=compact

# ── Verify ────────────────────────────────────────────────────────
echo ""
echo "All topics:"
docker exec kafka kafka-topics \
  --list \
  --bootstrap-server localhost:9092

echo ""
echo "Topic configurations:"
for topic in polymarket-predictions-raw weather-actuals-raw \
             market-weather-correlations market-accuracy-aggregates \
             arbitrage-alerts; do
  echo ""
  echo "--- $topic ---"
  docker exec kafka kafka-topics \
    --describe \
    --bootstrap-server localhost:9092 \
    --topic "$topic" 2>/dev/null | grep "Configs:"
done

echo ""
echo "Done!"