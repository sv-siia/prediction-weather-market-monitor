"""
manifold_producer.py
---------------------
Polls Manifold Markets API for active weather prediction markets.
Dynamically extracts city names from market questions.
Covers: rain, snow, temperature, sun, wind, fog, hail, storm, frost.
Produces validated records to Kafka topic: polymarket-predictions-raw

Arbitrage detection:
  - Cross-market: two markets about same location/event sum > 1.0
  - Mispricing: probability far from historical base rate
"""

import json
import time
import re
import os
import sys
import logging
import psycopg2
from datetime import datetime, timezone
from collections import defaultdict
from kafka import KafkaProducer
from tenacity import retry, stop_after_attempt, wait_exponential
import requests

# ── Import shared city validator ──────────────────────────────────
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.city_validator import is_valid_city
from utils.city_geocoder import geocode_city
from utils.city_cache import load_cache
from utils.schema_registry import SchemaRegistryClient
from utils.metrics import (
    start_metrics_server,
    manifold_polls_total, manifold_markets_fetched_total,
    manifold_markets_produced_total, manifold_arbitrage_detected_total,
    manifold_poll_duration_seconds, manifold_cities_tracked,
    manifold_api_errors_total,
    kafka_messages_produced_total, kafka_produce_errors_total,
)

# ── Logging ───────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────
KAFKA_BROKER      = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC       = os.getenv("KAFKA_TOPIC_PREDICTIONS", "polymarket-predictions-raw")
SCHEMA_REGISTRY   = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8085")
POLL_INTERVAL  = int(os.getenv("POLYMARKET_POLL_INTERVAL", "300"))
BASE_URL       = "https://api.manifold.markets/v0"
POSTGRES_HOST  = os.getenv("POSTGRES_HOST",     "localhost")
POSTGRES_PORT  = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB    = os.getenv("POSTGRES_DB",       "prediction_market")
POSTGRES_USER  = os.getenv("POSTGRES_USER",     "admin")
POSTGRES_PASS  = os.getenv("POSTGRES_PASSWORD", "changeme")

# Arbitrage threshold — if two opposite markets sum > 1.01
ARBITRAGE_THRESHOLD = 0.01

# Base rates for weather types (historical averages)
# Used to detect mispricing arbitrage
BASE_RATES = {
    "RAIN":        0.35,
    "SNOW":        0.15,
    "TEMPERATURE": 0.50,
    "SUNSHINE":    0.55,
    "WIND":        0.40,
    "FOG":         0.20,
    "HAIL":        0.10,
    "FROST":       0.20,
    "WEATHER":     0.40,
}

WEATHER_TERMS = [
    "Will it rain in", "Will there be rain", "rain in",
    "precipitation in", "Will there be precipitation",
    "Will it snow in", "Will there be snow", "snow in",
    "blizzard in", "Will there be a blizzard",
    "temperature in", "Will the temperature",
    "Will it be above", "Will it be below",
    "exceed degrees", "degrees celsius", "degrees fahrenheit",
    "Will it be hot", "Will it be cold", "Will it freeze",
    "Will there be frost", "Will it be sunny",
    "Will there be sunshine", "sunshine in",
    "Will it be cloudy", "Will it be overcast",
    "Will it be windy", "wind in",
    "Will there be a storm", "storm in",
    "Will there be a hurricane", "Will there be a tornado",
    "Will there be fog", "Will there be hail",
    "hail in", "Will it be foggy",
    "heat wave", "Will there be a heat wave",
]


# ── Pipeline health ───────────────────────────────────────────────
def write_pipeline_health(
        status: str, messages: int,
        errors: int, details: dict = None) -> None:
    """Writes producer health status to PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT,
            dbname=POSTGRES_DB, user=POSTGRES_USER,
            password=POSTGRES_PASS,
        )
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_health
                    (component, status, messages_processed,
                     error_count, last_message_at, details)
                VALUES (%s, %s, %s, %s, NOW(), %s)
            """, (
                "manifold_producer", status,
                messages, errors,
                json.dumps(details or {}),
            ))
        conn.commit()
        conn.close()
        log.debug(f"Pipeline health recorded: {status}")
    except Exception as e:
        log.warning(f"Could not write pipeline_health: {e}")


# ── Extract city from question ────────────────────────────────────
def extract_city(question: str, geocache: dict = None) -> str | None:
    """
    Extracts city name from Manifold market question.
    Uses shared is_valid_city() for validation.
    """
    q = question.strip()

    patterns = [
        r"[Ww]ill it (?:rain|snow|precipitate|freeze) in ([A-Z][^,\?\.]+?)(?:\s+on\s+\d|\s+today|\s+tomorrow|\s+tonight|\?|,|\s+in\s+\d)",
        r"[Ww]ill there be (?:rain|snow|precipitation|frost|fog|hail|sunshine|a storm|a blizzard|a hurricane|wind) in ([A-Z][^,\?\.]+?)(?:\s+on\s+\d|\s+today|\s+tomorrow|\?|,)",
        r"[Tt]emperature in ([A-Z][^,\?\.]+?)(?:\s+on\s+\d|\s+today|\s+exceed|\s+be|\?|,)",
        r"[Ww]ill the temperature in ([A-Z][^,\?\.]+?)(?:\s+on\s+\d|\s+today|\s+exceed|\s+be|\?|,)",
        r"[Ww]ill it be (?:sunny|cloudy|overcast|hot|cold|warm|windy|foggy|above|below) in ([A-Z][^,\?\.]+?)(?:\s+on\s+\d|\s+today|\s+tomorrow|\?|,)",
        r"[Ww]ill it be (?:above|below) [\d\.]+ ?(?:degrees|°)?[CF]? in ([A-Z][^,\?\.]+?)(?:\s+on\s+\d|\s+today|\?|,)",
        r"\bin ([A-Z][a-zA-Z\s\-]+?)(?:\s+on\s+\d|\s+today|\s+tomorrow|\?|,)",
    ]

    for pattern in patterns:
        match = re.search(pattern, q)
        if match:
            city = match.group(1).strip()
            city = re.sub(r'\s+', ' ', city)
            city = re.sub(
                r'\s+(today|tomorrow|tonight|on|in|at|this|the|before|during|exceed|above|below)$',
                '', city, flags=re.IGNORECASE
            ).strip()
            if is_valid_city(city, geocache):
                return city

    return None


# ── Detect market type ────────────────────────────────────────────
def detect_market_type(question: str) -> str:
    q = question.lower()
    if re.search(r'\b(rain|precipitation|drizzle|sleet)\b', q):
        return "RAIN"
    elif re.search(r'\b(snow|snowfall|blizzard|sleet|ice storm)\b', q):
        return "SNOW"
    elif re.search(r'\b(temperature|degrees|celsius|fahrenheit|above|below|exceed|warm|cold|hot|freeze|frost|heat wave)\b', q):
        return "TEMPERATURE"
    elif re.search(r'\b(sunny|sunshine|sun)\b', q):
        return "SUNSHINE"
    elif re.search(r'\b(wind|windy|storm|hurricane|tornado)\b', q):
        return "WIND"
    elif re.search(r'\b(fog|foggy|mist|misty)\b', q):
        return "FOG"
    elif re.search(r'\b(hail|hailstorm)\b', q):
        return "HAIL"
    elif re.search(r'\b(cloud|cloudy|overcast)\b', q):
        return "CLOUD"
    elif re.search(r'\b(frost|freeze|freezing)\b', q):
        return "FROST"
    return "WEATHER"


# ── Detect arbitrage ──────────────────────────────────────────────
def detect_arbitrage(records: list) -> list:
    """
    Detects arbitrage opportunities across all parsed records.

    Two types:
    1. Cross-market: YES(event) + YES(opposite_event) > 1.0
       e.g. "rain in London" YES=0.8 + "no rain in London" YES=0.9 = 1.7
    2. Mispricing: probability far from base rate (>0.4 deviation)
       e.g. "rain in Sahara" YES=0.8 but base rate is 0.05

    Returns list of arbitrage alert dicts.
    """
    alerts    = []
    now       = datetime.now(timezone.utc).isoformat()

    # Group by location + market_type + date
    groups = defaultdict(list)
    for r in records:
        if not r.get("closed"):  # only open markets
            key = (
                r.get("LOCATION_NAME", ""),
                r.get("MARKET_TYPE", ""),
                r.get("end_date_iso", "")[:10] if r.get("end_date_iso") else "",
            )
            groups[key].append(r)

    # Type 1: Cross-market arbitrage
    for (location, mtype, date), group in groups.items():
        if len(group) < 2:
            continue
        price_sum = sum(r.get("price", 0) for r in group)
        if price_sum > 1.0 + ARBITRAGE_THRESHOLD:
            margin   = round((price_sum - 1.0) * 100, 2)
            severity = "critical" if margin > 5 else "high"
            alerts.append({
                "location_name":    location,
                "market_type":      mtype,
                "date":             date,
                "alert_type":       "arbitrage_opportunity",
                "severity":         severity,
                "metric_value":     round(price_sum, 4),
                "arbitrage_margin": margin,
                "markets":          len(group),
                "message":          (
                    f"Cross-market arbitrage: {location} {mtype} {date} "
                    f"price_sum={price_sum:.3f} margin={margin:.2f}%"
                ),
                "detected_at":      now,
            })
            log.warning(
                f"💰 ARBITRAGE: {location} | {mtype} | "
                f"price_sum={price_sum:.3f} | margin={margin:.2f}%"
            )

    # Type 2: Mispricing arbitrage
    for r in records:
        if r.get("closed"):
            continue
        mtype     = r.get("MARKET_TYPE", "WEATHER")
        price     = r.get("price", 0.5)
        base      = BASE_RATES.get(mtype, 0.4)
        deviation = abs(price - base)

        if deviation > 0.40:
            margin   = round(deviation * 100, 2)
            severity = "critical" if deviation > 0.5 else "high"
            alerts.append({
                "location_name":    r.get("LOCATION_NAME", ""),
                "market_type":      mtype,
                "date":             r.get("end_date_iso", "")[:10] if r.get("end_date_iso") else "",
                "alert_type":       "arbitrage_opportunity",
                "severity":         severity,
                "metric_value":     price,
                "arbitrage_margin": margin,
                "markets":          1,
                "message":          (
                    f"Mispricing: {r.get('LOCATION_NAME')} {mtype} "
                    f"price={price:.3f} base_rate={base:.3f} "
                    f"deviation={margin:.2f}%"
                ),
                "detected_at":      now,
            })

    return alerts


# ── Convert timestamp ─────────────────────────────────────────────
def ms_to_iso(ms: int | None) -> str | None:
    if not ms:
        return None
    try:
        dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


# ── Parse Manifold market ─────────────────────────────────────────
def parse_market(market: dict, geocache: dict = None) -> dict | None:
    """Converts Manifold market to our standard schema."""
    question    = market.get("question", "")
    probability = market.get("probability")
    is_resolved = market.get("isResolved", False)
    resolution  = market.get("resolution")
    close_time  = market.get("closeTime")
    market_id   = market.get("id", "")
    slug        = market.get("slug", "")
    url         = market.get("url", "")

    if probability is None:
        return None
    if market.get("outcomeType") != "BINARY":
        return None

    city = extract_city(question, geocache)
    if not city:
        return None

    end_date_iso = ms_to_iso(close_time)

    if is_resolved and resolution in ["YES", "NO"]:
        actual_outcome   = 1 if resolution == "YES" else 0
        winner           = resolution == "YES"
        prediction_error = round(abs(probability - actual_outcome), 4)
        market_status    = "closed"
    else:
        actual_outcome   = None
        winner           = None
        prediction_error = None
        market_status    = "open"

    mtype     = detect_market_type(question)
    base_rate = BASE_RATES.get(mtype, 0.4)
    deviation = abs(probability - base_rate)
    is_arb    = deviation > 0.40 and not is_resolved

    return {
        # Avro schema required fields (uppercase)
        "CONDITION_ID":        market_id,
        "QUESTION":            question,
        "LOCATION_NAME":       city,
        "MARKET_TYPE":         mtype,
        "MARKET_STATUS":       market_status,
        "YES_PRICE":           round(probability, 4),
        "NO_PRICE":            round(1 - probability, 4),
        "WINNER":              winner,
        "CLOSED":              is_resolved,
        "END_DATE_ISO":        end_date_iso,
        "WEATHER_TYPE":        mtype,
        "IS_ARBITRAGE":        is_arb,
        "POLL_TIMESTAMP":      datetime.now(timezone.utc).isoformat(),
        # Extra fields for Flink jobs / downstream processing
        "condition_id":        market_id,
        "question_id":         slug,
        "question":            question,
        "market_slug":         slug,
        "end_date_iso":        end_date_iso,
        "price":               round(probability, 4),
        "winner":              winner,
        "closed":              is_resolved,
        "PRICE_SUM":           1.0,
        "BASE_RATE":           base_rate,
        "DEVIATION_FROM_BASE": round(deviation, 4),
        "IS_VALID":            True,
        "ACTUAL_OUTCOME":      actual_outcome,
        "PREDICTION_ERROR":    prediction_error,
        "NEEDS_WEATHER_CHECK": not is_resolved,
        "SOURCE":              "manifold",
        "MANIFOLD_URL":        url,
        "RESOLUTION":          resolution,
        "UNIQUE_BETTORS":      market.get("uniqueBettorCount", 0),
        "VOLUME":              market.get("volume", 0),
    }


# ── Fetch from API ────────────────────────────────────────────────
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=16),
    reraise=True
)
def fetch_markets(term: str) -> list:
    response = requests.get(
        f"{BASE_URL}/search-markets",
        params={"term": term, "limit": 100},
        timeout=10
    )
    response.raise_for_status()
    return response.json()


def fetch_all_weather_markets() -> list:
    log.info("Fetching all weather markets from Manifold...")
    seen_ids    = set()
    all_markets = []

    for term in WEATHER_TERMS:
        try:
            markets = fetch_markets(term)
            new = 0
            for m in markets:
                mid = m.get("id")
                if mid and mid not in seen_ids:
                    seen_ids.add(mid)
                    all_markets.append(m)
                    new += 1
            log.info(f"  '{term}': {len(markets)} results, {new} new")
            time.sleep(0.5)
        except Exception as e:
            log.error(f"  '{term}' failed: {e}")

    log.info(f"Total unique markets: {len(all_markets)}")
    return all_markets


# ── Save city coordinates to PostgreSQL ──────────────────────────
def upsert_city_location(conn, city: str, coords: dict) -> None:
    """Inserts or updates city lat/lon in city_locations table."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO city_locations (city_name, lat, lon, timezone, source)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (city_name) DO UPDATE SET
                    lat        = EXCLUDED.lat,
                    lon        = EXCLUDED.lon,
                    timezone   = EXCLUDED.timezone,
                    updated_at = NOW()
            """, (city, coords["lat"], coords["lon"],
                  coords.get("tz", "UTC"), coords.get("source", "nominatim")))
        conn.commit()
    except Exception as e:
        conn.rollback()
        log.warning("Failed to save city location for %s: %s", city, e)


# ── Main polling loop ─────────────────────────────────────────────
def run():
    start_metrics_server(8000, "manifold_producer")
    log.info("Starting Manifold Producer")
    log.info(f"Kafka broker:  {KAFKA_BROKER}")
    log.info(f"Topic:         {KAFKA_TOPIC}")
    log.info(f"Poll interval: {POLL_INTERVAL}s")

    schema_reg = SchemaRegistryClient(SCHEMA_REGISTRY)

    # PostgreSQL for city coordinates
    try:
        pg_conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "prediction_market"),
            user=os.getenv("POSTGRES_USER", "admin"),
            password=os.getenv("POSTGRES_PASSWORD", "changeme"),
        )
        log.info("Connected to PostgreSQL ✅")
    except Exception as e:
        log.warning("PostgreSQL unavailable, city coords won't be saved: %s", e)
        pg_conn = None

    geocache = load_cache()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: schema_reg.serialize(KAFKA_TOPIC, v),
        acks="all",
        retries=5,
        compression_type="snappy",
        max_in_flight_requests_per_connection=1,
    )
    log.info("Connected to Kafka ✅")

    while True:
        try:
            poll_start   = time.time()
            manifold_polls_total.inc()
            raw_markets  = fetch_all_weather_markets()
            manifold_markets_fetched_total.inc(len(raw_markets))
            records      = []
            sent         = 0
            skipped      = 0
            closed_count = 0
            open_count   = 0
            arb_count    = 0
            cities_found = set()
            by_type      = {}

            for market in raw_markets:
                record = parse_market(market, geocache)
                if record is None:
                    skipped += 1
                    continue

                errors = schema_reg.validate(KAFKA_TOPIC, record)
                if errors:
                    log.warning("Schema validation failed for %s: %s", record.get("CONDITION_ID"), errors)
                    skipped += 1
                    continue

                records.append(record)
                producer.send(KAFKA_TOPIC, value=record)
                sent += 1
                manifold_markets_produced_total.inc()
                kafka_messages_produced_total.labels(topic=KAFKA_TOPIC).inc()
                city = record["LOCATION_NAME"]
                cities_found.add(city)
                mtype = record["MARKET_TYPE"]
                by_type[mtype] = by_type.get(mtype, 0) + 1

                if record["MARKET_STATUS"] == "closed":
                    closed_count += 1
                else:
                    open_count += 1

                if record["IS_ARBITRAGE"]:
                    arb_count += 1
                    manifold_arbitrage_detected_total.inc()

            producer.flush()
            manifold_poll_duration_seconds.set(time.time() - poll_start)
            manifold_cities_tracked.set(len(cities_found))

            # Save city coordinates to PostgreSQL (once per unique city per poll)
            if pg_conn:
                for city in cities_found:
                    coords = geocode_city(city, geocache)
                    if coords:
                        upsert_city_location(pg_conn, city, coords)

            # Detect cross-market arbitrage
            arb_alerts = detect_arbitrage(records)
            if arb_alerts:
                log.warning(
                    f"💰 Found {len(arb_alerts)} arbitrage opportunities!"
                )

            log.info(
                f"Poll complete → "
                f"Sent={sent} Skipped={skipped} "
                f"Closed={closed_count} Open={open_count} "
                f"Arbitrage={arb_count} CrossArb={len(arb_alerts)}"
            )
            log.info(f"  Market types: {by_type}")
            log.info(
                f"  Cities ({len(cities_found)}): "
                f"{sorted(cities_found)}"
            )

            # Write pipeline health to PostgreSQL
            write_pipeline_health(
                status="healthy",
                messages=sent,
                errors=0,
                details={
                    "cities":       len(cities_found),
                    "by_type":      by_type,
                    "closed":       closed_count,
                    "open":         open_count,
                    "arbitrage":    arb_count,
                    "cross_arb":    len(arb_alerts),
                    "skipped":      skipped,
                }
            )

        except Exception as e:
            log.error(f"Error in polling loop: {e}")
            manifold_api_errors_total.inc()
            write_pipeline_health(
                status="degraded",
                messages=0,
                errors=1,
                details={"error": str(e)}
            )

        log.info(f"Sleeping {POLL_INTERVAL}s...")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()