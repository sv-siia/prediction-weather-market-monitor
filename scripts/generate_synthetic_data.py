"""
generate_synthetic_data.py
---------------------------
Generates synthetic Polymarket predictions with timestamps
that match CURRENT real weather from Open-Meteo API.

game_start_time = 0-6 hours in the PAST
→ Open-Meteo already has real weather data for these times
→ Flink can immediately join predictions with real weather

Usage:
    python generate_synthetic_data.py
"""

import json
import random
import os
from datetime import datetime, timedelta, timezone

# ── Config ────────────────────────────────────────────────────────
OUTPUT_DIR       = "data"
PREDICTIONS_FILE = os.path.join(OUTPUT_DIR, "sample_polymarket_predictions.json")
NUM_RECORDS      = 1000
RANDOM_SEED      = 42

random.seed(RANDOM_SEED)

# ── Cities ────────────────────────────────────────────────────────
CITIES = [
    "London", "New York", "Tokyo", "Seattle",
    "Paris", "Berlin", "Sydney", "Toronto", "Miami", "Warsaw"
]

# ── Question templates ────────────────────────────────────────────
RAIN_QUESTIONS = [
    "Will it rain in {city} on {date}?",
    "Will there be rain in {city} on {date}?",
    "Will precipitation occur in {city} on {date}?",
]
SNOW_QUESTIONS = [
    "Will it snow in {city} on {date}?",
    "Will there be snowfall in {city} on {date}?",
]
TEMP_QUESTIONS = [
    "Will the temperature in {city} exceed {threshold}°C on {date}?",
    "Will it be above {threshold}°C in {city} on {date}?",
]

# ── Helpers ───────────────────────────────────────────────────────
def make_id():
    return "0x" + "".join(random.choices("0123456789abcdef", k=64))

def iso_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def past_datetime(hours_min=0, hours_max=6):
    """
    Returns a timestamp in the PAST (0-6 hours ago).
    Rounded to nearest 15 minutes — matches Open-Meteo interval.
    Open-Meteo already has real weather data for these times.
    """
    base          = datetime.now(timezone.utc)
    hours_ago     = random.randint(hours_min, hours_max)
    minutes_ago   = random.choice([0, 15, 30, 45])
    observation   = base - timedelta(hours=hours_ago, minutes=minutes_ago)

    # Round to nearest 15 min to match Open-Meteo intervals
    rounded_min   = (observation.minute // 15) * 15
    observation   = observation.replace(minute=rounded_min, second=0, microsecond=0)

    return observation.strftime("%Y-%m-%dT%H:%M:%SZ")

def end_datetime():
    """
    Market close date — slightly in the future (market still open).
    """
    base   = datetime.now(timezone.utc)
    offset = timedelta(hours=random.randint(1, 48))
    return (base + offset).strftime("%Y-%m-%dT%H:%M:%SZ")

# ── Generator ─────────────────────────────────────────────────────
def generate_predictions(n):
    records = []
    market_types = ["RAIN"] * 60 + ["SNOW"] * 20 + ["TEMPERATURE"] * 20

    for i in range(n):
        city       = random.choice(CITIES)
        mtype      = random.choice(market_types)

        # ← КЛЮЧОВА ЗМІНА: час події в МИНУЛОМУ (0-6 годин тому)
        # Open-Meteo вже має реальні дані для цього часу
        game_start = past_datetime(hours_min=0, hours_max=6)
        end_date   = end_datetime()
        date_str   = game_start[:10]

        closed     = random.random() < 0.3
        active     = not closed

        # Build question
        if mtype == "RAIN":
            question  = random.choice(RAIN_QUESTIONS).format(
                city=city, date=date_str)
            yes_price = round(random.uniform(0.10, 0.90), 2)

        elif mtype == "SNOW":
            question  = random.choice(SNOW_QUESTIONS).format(
                city=city, date=date_str)
            yes_price = round(random.uniform(0.05, 0.60), 2)

        else:  # TEMPERATURE
            threshold = random.randint(15, 35)
            question  = random.choice(TEMP_QUESTIONS).format(
                city=city, date=date_str, threshold=threshold)
            yes_price = round(random.uniform(0.20, 0.80), 2)

        # 5% arbitrage
        is_arbitrage = random.random() < 0.05
        if is_arbitrage:
            no_price = round(1.0 + random.uniform(0.02, 0.08) - yes_price, 2)
        else:
            no_price = round(1.0 - yes_price, 2)

        # Winner only for closed markets
        if closed:
            yes_winner = random.random() < yes_price
            no_winner  = not yes_winner
        else:
            yes_winner = None
            no_winner  = None

        record = {
            "condition_id":    make_id(),
            "question_id":     make_id(),
            "question":        question,
            "market_slug":     question.lower()
                                       .replace(" ", "-")
                                       .replace("?", "")
                                       .replace(",", "")
                                       .replace("°", "")[:60],
            "end_date_iso":    end_date,
            "game_start_time": game_start,
            "outcome":         "Yes",
            "price":           yes_price,
            "winner":          yes_winner,
            "closed":          closed,
            "active":          active,
            "tags":            ["Weather", "All"],
            "tokens": [
                {"outcome": "Yes", "price": yes_price, "winner": yes_winner},
                {"outcome": "No",  "price": no_price,  "winner": no_winner},
            ],
            "LOCATION_NAME":  city,
            "POLL_TIMESTAMP": iso_now(),
            "IS_ARBITRAGE":   is_arbitrage,
            "PRICE_SUM":      round(yes_price + no_price, 2),
            "MARKET_TYPE":    mtype,
        }
        records.append(record)

    return records

# ── Main ──────────────────────────────────────────────────────────
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"Generating {NUM_RECORDS} synthetic predictions...")
    print(f"game_start_time = past 0-6 hours (Open-Meteo has real data)")
    print()

    predictions = generate_predictions(NUM_RECORDS)

    with open(PREDICTIONS_FILE, "w", encoding="utf-8") as f:
        json.dump(predictions, f, indent=2, ensure_ascii=False)

    # Stats
    rain   = sum(1 for p in predictions if p["MARKET_TYPE"] == "RAIN")
    snow   = sum(1 for p in predictions if p["MARKET_TYPE"] == "SNOW")
    temp   = sum(1 for p in predictions if p["MARKET_TYPE"] == "TEMPERATURE")
    arb    = sum(1 for p in predictions if p["IS_ARBITRAGE"])
    closed = sum(1 for p in predictions if p["closed"])
    cities = set(p["LOCATION_NAME"] for p in predictions)

    print(f"Saved → {PREDICTIONS_FILE}")
    print(f"\nStats:")
    print(f"  RAIN={rain} | SNOW={snow} | TEMPERATURE={temp}")
    print(f"  Arbitrage opportunities: {arb}")
    print(f"  Closed markets: {closed}")
    print(f"  Cities: {sorted(cities)}")
    print(f"  File size: {os.path.getsize(PREDICTIONS_FILE)//1024} KB")
    print()

    # Show 3 examples
    now = datetime.now(timezone.utc)
    print("Sample records (with time difference from NOW):")
    for p in predictions[:3]:
        game_time = datetime.fromisoformat(
            p["game_start_time"].replace("Z", "+00:00"))
        diff = int((now - game_time).total_seconds() / 60)
        print(f"\n  Question:       {p['question']}")
        print(f"  City:           {p['LOCATION_NAME']}")
        print(f"  Type:           {p['MARKET_TYPE']}")
        print(f"  Yes price:      {p['price']}")
        print(f"  game_start:     {p['game_start_time']}  ({diff} min ago)")
        print(f"  Arbitrage:      {p['IS_ARBITRAGE']}")

    print()
    print("Timeline check:")
    print(f"  Earliest game_start: min of all records ← up to 6h ago")
    print(f"  Latest game_start:   closest to NOW")
    print(f"  Open-Meteo interval: 15 min")
    print(f"  → Flink can join ALL records with real weather")

if __name__ == "__main__":
    main()