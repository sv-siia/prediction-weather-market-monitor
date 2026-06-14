"""
weather_producer.py
--------------------
Dynamically fetches weather for ALL cities found in Kafka predictions.
No hardcoded city list — automatically discovers cities from Manifold markets.

Three weather fetch strategies:
  1. CLOSED market with known date → historical Open-Meteo archive API
  2. OPEN market → current Open-Meteo forecast API
  3. Unknown date → current snapshot

Produces records to Kafka topic: weather-actuals-raw
"""

import json
import psycopg2
import time
import os
import sys
import logging
from datetime import datetime, timezone, date
from kafka import KafkaProducer, KafkaConsumer
from tenacity import retry, stop_after_attempt, wait_exponential
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import requests

# ── Logging ───────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────
KAFKA_BROKER      = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY   = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8085")
KAFKA_TOPIC_IN  = os.getenv("KAFKA_TOPIC_PREDICTIONS", "polymarket-predictions-raw")
KAFKA_TOPIC_OUT = os.getenv("KAFKA_TOPIC_WEATHER",     "weather-actuals-raw")
POLL_INTERVAL   = int(os.getenv("WEATHER_POLL_INTERVAL", "900"))
POSTGRES_HOST   = os.getenv("POSTGRES_HOST",     "localhost")
POSTGRES_PORT   = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB     = os.getenv("POSTGRES_DB",       "prediction_market")
POSTGRES_USER   = os.getenv("POSTGRES_USER",     "admin")
POSTGRES_PASS   = os.getenv("POSTGRES_PASSWORD", "changeme")
CURRENT_API_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_API_URL = "https://archive-api.open-meteo.com/v1/archive"
GEOCACHE_FILE   = "data/geocoding_cache.json"

# ── Known city coordinates ────────────────────────────────────────
KNOWN_COORDS = {
    "London":           {"lat": 51.5074,   "lon": -0.1278,   "tz": "Europe/London"},
    "New York":         {"lat": 40.7128,   "lon": -74.0060,  "tz": "America/New_York"},
    "New York City":    {"lat": 40.7128,   "lon": -74.0060,  "tz": "America/New_York"},
    "NYC":              {"lat": 40.7128,   "lon": -74.0060,  "tz": "America/New_York"},
    "Manhattan":        {"lat": 40.7831,   "lon": -73.9712,  "tz": "America/New_York"},
    "Tokyo":            {"lat": 35.6762,   "lon": 139.6503,  "tz": "Asia/Tokyo"},
    "Seattle":          {"lat": 47.6062,   "lon": -122.3321, "tz": "America/Los_Angeles"},
    "Paris":            {"lat": 48.8566,   "lon": 2.3522,    "tz": "Europe/Paris"},
    "Berlin":           {"lat": 52.5200,   "lon": 13.4050,   "tz": "Europe/Berlin"},
    "Sydney":           {"lat": -33.8688,  "lon": 151.2093,  "tz": "Australia/Sydney"},
    "Toronto":          {"lat": 43.6532,   "lon": -79.3832,  "tz": "America/Toronto"},
    "Miami":            {"lat": 25.7617,   "lon": -80.1918,  "tz": "America/New_York"},
    "Warsaw":           {"lat": 52.2297,   "lon": 21.0122,   "tz": "Europe/Warsaw"},
    "Amsterdam":        {"lat": 52.3676,   "lon": 4.9041,    "tz": "Europe/Amsterdam"},
    "Dublin":           {"lat": 53.3498,   "lon": -6.2603,   "tz": "Europe/Dublin"},
    "Chicago":          {"lat": 41.8781,   "lon": -87.6298,  "tz": "America/Chicago"},
    "Los Angeles":      {"lat": 34.0522,   "lon": -118.2437, "tz": "America/Los_Angeles"},
    "San Francisco":    {"lat": 37.7749,   "lon": -122.4194, "tz": "America/Los_Angeles"},
    "Boston":           {"lat": 42.3601,   "lon": -71.0589,  "tz": "America/New_York"},
    "Denver":           {"lat": 39.7392,   "lon": -104.9903, "tz": "America/Denver"},
    "Atlanta":          {"lat": 33.7490,   "lon": -84.3880,  "tz": "America/New_York"},
    "Vancouver":        {"lat": 49.2827,   "lon": -123.1207, "tz": "America/Vancouver"},
    "Montreal":         {"lat": 45.5017,   "lon": -73.5673,  "tz": "America/Toronto"},
    "Calgary":          {"lat": 51.0447,   "lon": -114.0719, "tz": "America/Denver"},
    "Ottawa":           {"lat": 45.4215,   "lon": -75.6972,  "tz": "America/Toronto"},
    "Manchester":       {"lat": 53.4808,   "lon": -2.2426,   "tz": "Europe/London"},
    "Birmingham":       {"lat": 52.4862,   "lon": -1.8904,   "tz": "Europe/London"},
    "Istanbul":         {"lat": 41.0082,   "lon": 28.9784,   "tz": "Europe/Istanbul"},
    "Moscow":           {"lat": 55.7558,   "lon": 37.6173,   "tz": "Europe/Moscow"},
    "Bratislava":       {"lat": 48.1486,   "lon": 17.1077,   "tz": "Europe/Bratislava"},
    "Zurich":           {"lat": 47.3769,   "lon": 8.5417,    "tz": "Europe/Zurich"},
    "Zürich":           {"lat": 47.3769,   "lon": 8.5417,    "tz": "Europe/Zurich"},
    "Brussels":         {"lat": 50.8503,   "lon": 4.3517,    "tz": "Europe/Brussels"},
    "Vienna":           {"lat": 48.2082,   "lon": 16.3738,   "tz": "Europe/Vienna"},
    "Prague":           {"lat": 50.0755,   "lon": 14.4378,   "tz": "Europe/Prague"},
    "Szczecin":         {"lat": 53.4285,   "lon": 14.5528,   "tz": "Europe/Warsaw"},
    "Madurai":          {"lat": 9.9252,    "lon": 78.1198,   "tz": "Asia/Kolkata"},
    "Anchorage":        {"lat": 61.2181,   "lon": -149.9003, "tz": "America/Anchorage"},
    "Phoenix":          {"lat": 33.4484,   "lon": -112.0740, "tz": "America/Phoenix"},
    "Washington":       {"lat": 38.9072,   "lon": -77.0369,  "tz": "America/New_York"},
    "Durham":           {"lat": 35.9940,   "lon": -78.8986,  "tz": "America/New_York"},
    "Austin":           {"lat": 30.2672,   "lon": -97.7431,  "tz": "America/Chicago"},
    "Rockford":         {"lat": 42.2711,   "lon": -89.0940,  "tz": "America/Chicago"},
    "Edmonton":         {"lat": 53.5461,   "lon": -113.4938, "tz": "America/Edmonton"},
    "Halifax":          {"lat": 44.6488,   "lon": -63.5752,  "tz": "America/Halifax"},
    "Saskatoon":        {"lat": 52.1332,   "lon": -106.6700, "tz": "America/Regina"},
    "Thunder Bay":      {"lat": 48.3809,   "lon": -89.2477,  "tz": "America/Thunder_Bay"},
    "Prince George":    {"lat": 53.9171,   "lon": -122.7497, "tz": "America/Vancouver"},
    "Bibinje":          {"lat": 44.0667,   "lon": 15.2833,   "tz": "Europe/Zagreb"},
    "Jerusalem":        {"lat": 31.7683,   "lon": 35.2137,   "tz": "Asia/Jerusalem"},
    "Nicosia":          {"lat": 35.1856,   "lon": 33.3823,   "tz": "Asia/Nicosia"},
    "Ljubljana":        {"lat": 46.0569,   "lon": 14.5058,   "tz": "Europe/Ljubljana"},
    "Chelyabinsk":      {"lat": 55.1644,   "lon": 61.4368,   "tz": "Asia/Yekaterinburg"},
    "Oymyakon":         {"lat": 63.4608,   "lon": 142.7858,  "tz": "Asia/Vladivostok"},
    "West Hollywood":   {"lat": 34.0900,   "lon": -118.3617, "tz": "America/Los_Angeles"},
    "Berkeley":         {"lat": 37.8716,   "lon": -122.2727, "tz": "America/Los_Angeles"},
    "Orlando":          {"lat": 28.5383,   "lon": -81.3792,  "tz": "America/New_York"},
    "Westfield":        {"lat": 42.1251,   "lon": -72.7495,  "tz": "America/New_York"},
    "Hamilton":         {"lat": 43.2557,   "lon": -79.8711,  "tz": "America/Toronto"},
    "Kent":             {"lat": 51.2787,   "lon": 1.0798,    "tz": "Europe/London"},
    "Camden":           {"lat": 39.9259,   "lon": -75.1196,  "tz": "America/New_York"},
    "Barriere":         {"lat": 51.1891,   "lon": -120.1335, "tz": "America/Vancouver"},
    "Banská Štiavnica": {"lat": 48.4586,   "lon": 18.8965,   "tz": "Europe/Bratislava"},
}

# ── Import shared city validator ──────────────────────────────────
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.city_validator import is_valid_city
from utils.city_cache import load_cache as load_city_cache
from utils.schema_registry import SchemaRegistryClient
from utils.metrics import (
    start_metrics_server,
    weather_polls_total, weather_records_produced_total,
    weather_poll_duration_seconds, weather_cities_active,
    weather_api_errors_total, weather_geocoding_cache_size,
    kafka_messages_produced_total, kafka_produce_errors_total,
)


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
                "weather_producer", status,
                messages, errors,
                json.dumps(details or {}),
            ))
        conn.commit()
        conn.close()
        log.debug(f"Pipeline health recorded: {status}")
    except Exception as e:
        log.warning(f"Could not write pipeline_health: {e}")


# ── Geocoding cache ───────────────────────────────────────────────
def load_geocache() -> dict:
    if os.path.exists(GEOCACHE_FILE):
        with open(GEOCACHE_FILE, "r") as f:
            cache = json.load(f)
            log.info(f"Loaded geocache: {len(cache)} cities")
            return cache
    return {}


def save_geocache(cache: dict) -> None:
    os.makedirs("data", exist_ok=True)
    with open(GEOCACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


def get_coordinates(city: str, cache: dict) -> dict | None:
    """
    Returns {lat, lon, tz} for a city.
    Checks: KNOWN_COORDS → cache → geopy lookup
    """
    if city in KNOWN_COORDS:
        return KNOWN_COORDS[city]

    if city in cache:
        return cache[city]

    log.info(f"Geocoding new city: '{city}'")
    try:
        geolocator = Nominatim(
            user_agent="prediction_market_monitor_v1",
            timeout=10
        )
        location = geolocator.geocode(city)
        if location:
            coords = {
                "lat": location.latitude,
                "lon": location.longitude,
                "tz":  "UTC"
            }
            cache[city] = coords
            save_geocache(cache)
            log.info(
                f"  Geocoded: {city} → "
                f"({coords['lat']:.4f}, {coords['lon']:.4f})"
            )
            return coords
        else:
            log.warning(f"  Could not geocode: '{city}'")
            cache[city] = None
            save_geocache(cache)
            return None
    except (GeocoderTimedOut, GeocoderServiceError) as e:
        log.error(f"  Geocoding error for '{city}': {e}")
        return None


# ── Get all city+date combinations from Kafka ─────────────────────
def get_city_dates_from_kafka(city_cache: dict = None) -> list:
    """
    Reads all predictions from Kafka.
    Returns list of {city, date, needs_check} dicts.
    Deduplicates by city+date.
    Filters out invalid city names.
    """
    seen    = set()
    results = []

    try:
        def _deserialize(v):
            # Try JSON first (old messages), then Avro wire format (new messages)
            try:
                return json.loads(v.decode("utf-8"))
            except Exception:
                try:
                    import io, struct, fastavro
                    from pathlib import Path
                    schema_path = Path(__file__).parent.parent / "schemas" / "prediction.avsc"
                    parsed = fastavro.parse_schema(json.loads(schema_path.read_text()))
                    buf = io.BytesIO(v[5:])  # skip 5-byte Confluent header
                    return fastavro.schemaless_reader(buf, parsed)
                except Exception:
                    return None

        consumer = KafkaConsumer(
            KAFKA_TOPIC_IN,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset="earliest",
            consumer_timeout_ms=5000,
            value_deserializer=_deserialize,
            group_id=None,
        )

        for message in consumer:
            r = message.value
            if r is None:
                continue
            city     = r.get("LOCATION_NAME")
            end_date = r.get("end_date_iso", "")
            date_str = end_date[:10] if end_date else "current"

            if not city:
                continue

            if not is_valid_city(city, city_cache):
                continue

            key = f"{city}_{date_str}"
            if key not in seen:
                seen.add(key)
                results.append({
                    "city":        city,
                    "date":        date_str,
                    "needs_check": r.get("NEEDS_WEATHER_CHECK", True),
                })

        consumer.close()
        log.info(f"Found {len(results)} unique city+date combinations")

    except Exception as e:
        log.warning(f"Kafka read error: {e}")
        for city in list(KNOWN_COORDS.keys())[:10]:
            results.append({
                "city":        city,
                "date":        "current",
                "needs_check": True,
            })

    return results


# ── Fetch current weather ─────────────────────────────────────────
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8))
def fetch_current_weather(city: str, coords: dict) -> dict | None:
    params = {
        "latitude":  coords["lat"],
        "longitude": coords["lon"],
        "current":   (
            "temperature_2m,precipitation,rain,"
            "weather_code,wind_speed_10m,relative_humidity_2m"
        ),
        "timezone":  coords.get("tz", "UTC"),
    }
    response = requests.get(CURRENT_API_URL, params=params, timeout=10)
    response.raise_for_status()
    data    = response.json()
    current = data.get("current", {})
    if not current:
        return None

    temp_c = current.get("temperature_2m")
    temp_f = round(temp_c * 9/5 + 32, 1) if temp_c is not None else None

    return {
        "latitude":      data.get("latitude"),
        "longitude":     data.get("longitude"),
        "current_units": data.get("current_units", {}),
        "current": {
            "time":                 current.get("time"),
            "temperature_2m":       temp_c,
            "temperature_2m_f":     temp_f,
            "precipitation":        current.get("precipitation"),
            "rain":                 current.get("rain"),
            "weather_code":         current.get("weather_code"),
            "wind_speed_10m":       current.get("wind_speed_10m"),
            "relative_humidity_2m": current.get("relative_humidity_2m"),
        },
        "LOCATION_NAME":    city,
        "WEATHER_TYPE":     "current",
        "OBSERVATION_DATE": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "SOURCE":           "open-meteo",
        "POLL_TIMESTAMP":   datetime.now(timezone.utc).isoformat(),
    }


# ── Fetch historical weather ──────────────────────────────────────
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8))
def fetch_historical_weather(
        city: str, coords: dict, date_str: str) -> dict | None:
    params = {
        "latitude":   coords["lat"],
        "longitude":  coords["lon"],
        "start_date": date_str,
        "end_date":   date_str,
        "daily":      (
            "temperature_2m_max,temperature_2m_min,"
            "precipitation_sum,rain_sum,weathercode"
        ),
        "timezone":   coords.get("tz", "UTC"),
    }
    response = requests.get(ARCHIVE_API_URL, params=params, timeout=10)
    response.raise_for_status()
    data  = response.json()
    daily = data.get("daily", {})

    if not daily or not daily.get("temperature_2m_max"):
        return None

    temp_max_c = daily["temperature_2m_max"][0]
    temp_min_c = daily["temperature_2m_min"][0]
    temp_max_f = (
        round(temp_max_c * 9/5 + 32, 1)
        if temp_max_c is not None else None
    )
    temp_min_f = (
        round(temp_min_c * 9/5 + 32, 1)
        if temp_min_c is not None else None
    )
    precip = daily.get("precipitation_sum", [None])[0]
    rain   = daily.get("rain_sum",          [None])[0]
    wcode  = daily.get("weathercode",       [None])[0]

    return {
        "latitude":  data.get("latitude"),
        "longitude": data.get("longitude"),
        "current_units": {
            "temperature_2m_max": "°C",
            "temperature_2m_min": "°C",
            "precipitation_sum":  "mm",
        },
        "current": {
            "time":                 date_str,
            "temperature_2m":       temp_max_c,
            "temperature_2m_f":     temp_max_f,
            "temperature_2m_min":   temp_min_c,
            "temperature_2m_min_f": temp_min_f,
            "precipitation":        precip,
            "rain":                 rain,
            "weather_code":         wcode,
            "wind_speed_10m":       None,
            "relative_humidity_2m": None,
        },
        "LOCATION_NAME":    city,
        "WEATHER_TYPE":     "historical",
        "OBSERVATION_DATE": date_str,
        "SOURCE":           "open-meteo-archive",
        "POLL_TIMESTAMP":   datetime.now(timezone.utc).isoformat(),
    }


# ── Should use historical API? ────────────────────────────────────
def should_fetch_historical(date_str: str) -> bool:
    if not date_str or date_str == "current":
        return False
    try:
        return date.fromisoformat(date_str) < date.today()
    except Exception:
        return False


# ── Main polling loop ─────────────────────────────────────────────
def run():
    start_metrics_server(8001, "weather_producer")
    log.info("Starting Weather Producer (Dynamic Cities)")
    log.info(f"Kafka broker:  {KAFKA_BROKER}")
    log.info(f"Topic out:     {KAFKA_TOPIC_OUT}")
    log.info(f"Poll interval: {POLL_INTERVAL}s")

    geocache   = load_geocache()
    city_cache = load_city_cache()

    updated = 0
    for city, coords in KNOWN_COORDS.items():
        if city not in geocache:
            geocache[city] = coords
            updated += 1
    if updated:
        save_geocache(geocache)
        log.info(f"Pre-populated geocache with {updated} known cities")

    schema_reg = SchemaRegistryClient(SCHEMA_REGISTRY)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: schema_reg.serialize(KAFKA_TOPIC_OUT, v),
        acks="all",
        retries=5,
        compression_type="snappy",
        max_in_flight_requests_per_connection=1,
    )
    log.info("Connected to Kafka ✅")

    while True:
        try:
            poll_start = time.time()
            weather_polls_total.inc()
            city_dates = get_city_dates_from_kafka(city_cache)
            weather_cities_active.set(len(city_dates))
            weather_geocoding_cache_size.set(len(geocache))
            log.info(
                f"Processing {len(city_dates)} city+date combinations"
            )

            sent       = 0
            failed     = 0
            skipped    = 0
            historical = 0
            current    = 0

            for item in city_dates:
                city     = item["city"]
                date_str = item["date"]

                coords = get_coordinates(city, geocache)
                if not coords:
                    skipped += 1
                    continue

                try:
                    if should_fetch_historical(date_str):
                        record = fetch_historical_weather(
                            city, coords, date_str
                        )
                        if record:
                            errs = schema_reg.validate(KAFKA_TOPIC_OUT, record)
                            if errs:
                                log.warning("Schema validation failed %s %s: %s", city, date_str, errs)
                                skipped += 1
                            else:
                                producer.send(KAFKA_TOPIC_OUT, value=record)
                                sent += 1
                                historical += 1
                                weather_records_produced_total.labels(type="historical").inc()
                                kafka_messages_produced_total.labels(topic=KAFKA_TOPIC_OUT).inc()
                                log.info(
                                    f"HISTORICAL {city} {date_str}: "
                                    f"max={record['current']['temperature_2m']}°C "
                                    f"rain={record['current']['rain']}mm"
                                )
                    else:
                        record = fetch_current_weather(city, coords)
                        if record:
                            errs = schema_reg.validate(KAFKA_TOPIC_OUT, record)
                            if errs:
                                log.warning("Schema validation failed %s: %s", city, errs)
                                skipped += 1
                            else:
                                producer.send(KAFKA_TOPIC_OUT, value=record)
                                sent += 1
                                current += 1
                                weather_records_produced_total.labels(type="current").inc()
                                kafka_messages_produced_total.labels(topic=KAFKA_TOPIC_OUT).inc()
                            log.info(
                                f"CURRENT {city}: "
                                f"{record['current']['temperature_2m']}°C "
                                f"code={record['current']['weather_code']}"
                            )

                except Exception as e:
                    log.error(f"Failed {city} {date_str}: {e}")
                    failed += 1
                    weather_api_errors_total.inc()

                time.sleep(0.3)

            producer.flush()
            weather_poll_duration_seconds.set(time.time() - poll_start)

            # Write pipeline health to PostgreSQL
            write_pipeline_health(
                status="healthy",
                messages=sent,
                errors=failed,
                details={
                    "historical": historical,
                    "current":    current,
                    "skipped":    skipped,
                    "cities":     len(city_dates),
                }
            )

            log.info(
                f"Poll complete → "
                f"Sent={sent} Historical={historical} "
                f"Current={current} Skipped={skipped} Failed={failed}"
            )

        except Exception as e:
            log.error(f"Error in polling loop: {e}")
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