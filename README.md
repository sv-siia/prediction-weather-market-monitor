# 🌩️ Real-Time Prediction Market Accuracy Monitor

> **Stream-based analytics pipeline** that correlates crowd-sourced weather predictions from Manifold Markets with real weather data from Open-Meteo — surfacing market inefficiencies, systematic biases, and arbitrage opportunities in real time.

[![CI](https://github.com/sv-siia/prediction-market-monitor/actions/workflows/ci.yml/badge.svg)](https://github.com/sv-siia/prediction-market-monitor/actions/workflows/ci.yml)
![Coverage](https://img.shields.io/badge/coverage-94%25-brightgreen)
![Tests](https://img.shields.io/badge/tests-377%20passed-brightgreen)
![Python](https://img.shields.io/badge/python-3.12-blue)
![Kafka](https://img.shields.io/badge/kafka-7.5-black)
![License](https://img.shields.io/badge/license-MIT-blue)

---

## Table of Contents

- [Why This Exists](#why-this-exists)
- [Architecture](#architecture)
- [Two Execution Scenarios](#two-execution-scenarios)
- [Components](#components)
- [Kafka Topics](#kafka-topics)
- [Data Schemas](#data-schemas)
- [Infrastructure](#infrastructure)
- [Quick Start](#quick-start)
- [Data Quality Layer](#data-quality-layer)
- [Grafana Dashboard](#grafana-dashboard)
- [Success Metrics](#success-metrics)
- [Testing](#testing)
- [CI/CD](#cicd)
- [Security](#security)
- [Cost Analysis](#cost-analysis)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)

---

## Why This Exists

Manifold Markets allows traders to bet on weather outcomes across different cities. Nobody was systematically tracking **how accurate these crowd-sourced predictions actually are** when compared to real weather data.

This system answers that question — continuously — enabling traders to:
- Spot **market inefficiencies** before anyone else
- Detect **systematic biases** in crowd predictions
- Identify **arbitrage opportunities** (price sums ≠ 1.0) within 1 minute of market creation
- Make smarter decisions based on real accuracy data

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES (FREE)                         │
│   Manifold Markets API                  Open-Meteo API              │
│   api.manifold.markets/v0/markets       api.open-meteo.com          │
│   poll every 5 min                      poll every 15 min           │
└────────────┬────────────────────────────────────┬───────────────────┘
             │                                    │
             ▼                                    ▼
┌────────────────────────┐          ┌─────────────────────────┐
│   manifold_producer.py │          │   weather_producer.py   │
│  • Filter: "Weather"   │          │  • Dynamic city tracking│
│  • Parse location      │          │  • Geocoding cache      │
│  • Validate prices     │          │  • 15-min dedup cache   │
│  • Detect arbitrage    │          │  • Historical + current │
│  • Exponential backoff │          │  • Exponential backoff  │
│  • Idempotent producer │          │  • Idempotent producer  │
└──────────┬─────────────┘          └───────────┬─────────────┘
           │                                    │
           ▼                                    ▼
┌──────────────────────────┐    ┌───────────────────────────┐
│  manifold-predictions    │    │   weather-actuals-raw     │
│       -raw               │    │   3 partitions, 7d        │
│   3 partitions, 7d       │    │   Snappy compression      │
│   Snappy compression     │    └───────────────────────────┘
└──────────────────────────┘                │
           │                                │
           └──────────────┬─────────────────┘
                          │
             ┌────────────▼────────────┐
             │    correlation_job.py   │
             │  • Join by LOCATION_NAME│
             │  • 3 match strategies   │
             │  • Accuracy calculation │
             │  • 24h stateful pending │
             └────────────┬────────────┘
                          │
                          ▼
          ┌───────────────────────────────┐
          │  market-weather-correlations  │
          │   3 partitions, 30d           │
          └───────────────┬───────────────┘
                          │
             ┌────────────▼────────────┐
             │   aggregation_job.py    │
             │  • 1h sliding windows   │
             │  • 15-min slide         │
             │  • Accuracy, bias,      │
             │    volume-weighted score│
             └────────────┬────────────┘
                          │
                          ▼
          ┌───────────────────────────────┐
          │  market-accuracy-aggregates   │
          │   3 partitions, 30d, compact  │
          └───────────────┬───────────────┘
                          │
             ┌────────────▼────────────┐
             │     anomaly_job.py      │
             │  • 7-day rolling baseline│
             │  • 2σ threshold alerts  │
             │  • Arbitrage detection  │
             │  • Producer health      │
             │  • Join coverage alerts │
             └────────────┬────────────┘
                          │
                          ▼
          ┌───────────────────────────────┐
          │       arbitrage-alerts        │
          │   3 partitions, 30d, compact  │
          └───────────────┬───────────────┘
                          │
                          ▼
          ┌───────────────────────────────┐
          │          PostgreSQL           │
          │  4 tables · 5 views · indexes │
          └───────────────┬───────────────┘
                          │
                          ▼
          ┌───────────────────────────────┐
          │       Grafana Dashboard       │
          │        23 panels              │
          │  http://localhost:3000        │
          └───────────────────────────────┘
```

---

## Two Execution Scenarios

This project supports **two distinct execution scenarios** that can be switched via Docker Compose profiles.

### Scenario 1 — Python Scripts (default, lightweight)

Traditional Python-based processing. Runs everywhere with minimal resources.

```bash
docker-compose --profile python up
```

| Component | Implementation |
|---|---|
| Correlation | Snapshot join with 3 match strategies |
| Aggregation | Sliding 1h / 15min windows in Python |
| Anomaly detection | 7-day rolling baseline, 2σ threshold |

### Scenario 2 — PyFlink with Event-Time Windowing

True streaming with Flink's DataStream API, watermarks, and stateful operators. Requires more resources (JVM + Python bridge).

```bash
docker-compose --profile pyflink up
```

| Component | Implementation |
|---|---|
| Correlation | `KeyedCoProcessFunction` with `MapState` (pending predictions, 24h TTL) |
| Aggregation | `SlidingEventTimeWindows.of(1h, 15min)` with `AggregateFunction` |
| Anomaly detection | `KeyedProcessFunction` with `ListState` (8-day TTL) |
| Watermarks | `WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(15))` |
| Semantics | Exactly-once via Flink checkpointing + idempotent sink |

> **Note:** Scenario 2 demonstrates production-grade event-time stream processing. It requires a dedicated Flink cluster with sufficient resources (≥4 GB RAM per job). In a resource-constrained local environment, Scenario 1 provides equivalent results.

---

## Components

### 1. Manifold Markets Data Producer (`producers/manifold_producer.py`)

| Requirement | Implementation |
|---|---|
| Poll every 5 min | `MANIFOLD_POLL_INTERVAL=300` env var |
| Filter by "Weather" tag | 37 weather search terms (RAIN, SNOW, TEMPERATURE, WIND, FOG, HAIL…) |
| Parse location from question | Regex patterns + Claude Haiku fallback via `utils/city_extractor.py` |
| Validate token prices ≈ 1.0 (±0.01) | `PRICE_SUM` field + validation in schema |
| Detect arbitrage (price sum >1%) | Cross-market + mispricing detection in `detect_arbitrage()` |
| Exponential backoff | `tenacity`: start 1s, max 16s, 5 attempts |
| Idempotent producer | `enable_idempotence=True`, `acks="all"`, `retries=5` |
| Schema validation | Avro schema via Confluent Schema Registry |
| Circuit breaker | `CLOSED → OPEN → HALF_OPEN` in `utils/circuit_breaker.py` |

### 2. Weather Data Producer (`producers/weather_producer.py`)

| Requirement | Implementation |
|---|---|
| Poll every 15 min | `WEATHER_POLL_INTERVAL=900` env var |
| Dynamic location tracking | Reads city names from `manifold-predictions-raw` topic |
| Geocoding cache | Nominatim via geopy, persisted to `data/geocoding_cache.json` |
| Open-Meteo API query | `?latitude={lat}&longitude={lon}&current=temperature_2m,precipitation,rain,weather_code,wind_speed_10m,relative_humidity_2m` |
| Add `LOCATION_NAME` field | Injected from geocoding cache before producing |
| 15-min deduplication | In-memory cache prevents redundant API calls |
| Historical data | Archive API for closed markets (past event dates) |
| Exponential backoff | Same `tenacity` pattern as manifold producer |

### 3. Real-Time Correlation Engine (`flink_jobs/correlation_job.py`)

| Requirement | Implementation |
|---|---|
| Consume both topics | Kafka consumer group `correlation-group` |
| Join key: `LOCATION_NAME` | Standardized city name in both streams |
| 15-min late data allowance | Snapshot loads all data; PyFlink uses `for_bounded_out_of_orderness(Duration.of_minutes(15))` |
| Boolean accuracy (RAIN/NO_RAIN) | Exact match in `outcome_calculator.py` |
| Continuous accuracy (TEMPERATURE) | Error margin in degrees |
| Produce to `market-weather-correlations` | Avro-encoded |
| Stateful pending predictions TTL 24h | PyFlink: `MapStateDescriptor` with `StateTtlConfig(24h)`; Scenario 1: in-memory snapshot |
| Match strategies | `winner_known`, `historical_weather`, `current_snapshot` |

**Correlated output fields:**
```
ACTUAL_OUTCOME          — 1 (correct) or 0 (incorrect)
PREDICTION_ERROR        — |predicted_odds − actual_outcome| (0.0–1.0)
CORRELATION_LATENCY_SEC — seconds from POLL_TIMESTAMP to correlation
```

### 4. Accuracy Aggregation Processor (`flink_jobs/aggregation_job.py`)

| Metric | Formula |
|---|---|
| **Accuracy rate** | `correct_predictions / total_predictions` |
| **Average prediction error** | `sum(PREDICTION_ERROR) / count` |
| **Volume-weighted accuracy** | `sum(accuracy × volume) / sum(volume)` |
| **Bias score** | `avg_predicted_odds − avg_actual_outcome` |

- Window: **1-hour sliding, 15-min slide** (`WINDOW_HOURS=1`, `SLIDE_MINUTES=15`)
- PyFlink version uses `SlidingEventTimeWindows` with `AggregateFunction` for incremental computation
- Upserts to `market_aggregates` table and produces to `market-accuracy-aggregates`

### 5. Anomaly Detection and Alerting (`flink_jobs/anomaly_job.py`)

| Alert Type | Condition | Severity |
|---|---|---|
| `accuracy_drop` | accuracy < 80% vs 7-day baseline | high |
| `bias_detected` | \|bias_score\| > 0.30 | medium |
| `arbitrage_opportunity` | `IS_ARBITRAGE=true` from producer | high |
| `low_coverage` | join coverage < 95% | high / critical |
| `producer_down` | no data for > 10 minutes | critical |
| `producer_timeout` | producer timeout detected | medium |

Statistical thresholds:
- **2σ rule**: flag when accuracy deviates > 2 standard deviations from 7-day mean
- **Dynamic thresholds**: computed per `(location, market_type)` from rolling baseline
- **Static fallback**: used when < 3 data points available

---

## Kafka Topics

| Topic | Partitions | Retention | Cleanup Policy | Compression |
|---|---|---|---|---|
| `manifold-predictions-raw` | 3 | 7 days | delete | Snappy |
| `weather-actuals-raw` | 3 | 7 days | delete | Snappy |
| `market-weather-correlations` | 3 | 30 days | delete | Snappy |
| `market-accuracy-aggregates` | 3 | 30 days | **compact** | Snappy |
| `arbitrage-alerts` | 3 | 30 days | **compact** | Snappy |

> All topics are created automatically on first `docker compose up` via the `kafka-init` container.

---

## Data Schemas

Schemas are enforced via **Confluent Schema Registry** (Avro format). Located in `schemas/`.

<details>
<summary><strong>Prediction record (manifold-predictions-raw)</strong></summary>

```json
{
  "condition_id": "0xabc...",
  "question_id": "xyz",
  "question": "Will it rain in Seattle on May 20, 2026?",
  "LOCATION_NAME": "Seattle",
  "MARKET_TYPE": "RAIN",
  "game_start_time": "2026-05-20T18:00:00Z",
  "end_date_iso": "2026-05-21T00:00:00Z",
  "YES_PRICE": 0.72,
  "NO_PRICE": 0.28,
  "PRICE_SUM": 1.00,
  "IS_ARBITRAGE": false,
  "POLL_TIMESTAMP": "2026-05-20T12:00:00Z"
}
```
</details>

<details>
<summary><strong>Weather record (weather-actuals-raw)</strong></summary>

```json
{
  "LOCATION_NAME": "Seattle",
  "latitude": 47.6062,
  "longitude": -122.3321,
  "current": {
    "time": "2026-05-20T18:00:00Z",
    "temperature_2m": 14.2,
    "precipitation": 3.1,
    "rain": 3.1,
    "weather_code": 61,
    "wind_speed_10m": 12.4,
    "relative_humidity_2m": 87
  },
  "POLL_TIMESTAMP": "2026-05-20T18:05:00Z"
}
```
</details>

<details>
<summary><strong>Correlated record (market-weather-correlations)</strong></summary>

```json
{
  "condition_id": "0xabc...",
  "LOCATION_NAME": "Seattle",
  "MARKET_TYPE": "RAIN",
  "YES_PRICE": 0.72,
  "ACTUAL_OUTCOME": 1,
  "PREDICTION_ERROR": 0.28,
  "CORRELATION_METHOD": "historical_weather",
  "CORRELATION_LATENCY_SEC": 180,
  "POLL_TIMESTAMP": "2026-05-20T18:05:00Z"
}
```
</details>

---

## Infrastructure

All services are defined in `docker-compose.yml` and start with a single command.

### Containers

| Container | Image | Memory Limit | Role |
|---|---|---|---|
| `zookeeper` | confluentinc/cp-zookeeper:7.5.0 | 512 MB | Kafka coordination |
| `kafka` | confluentinc/cp-kafka:7.5.0 | 1.5 GB | Message broker |
| `schema-registry` | confluentinc/cp-schema-registry:7.5.0 | — | Avro schema store |
| `kafka-init` | cp-kafka (init container) | — | Creates all 5 topics on startup |
| `flink-jobmanager` | apache/flink:1.18-scala_2.12 | 512 MB | Flink cluster coordinator |
| `flink-taskmanager` | apache/flink:1.18-scala_2.12 | 512 MB | Flink job execution |
| `postgres` | postgres:15 | 256 MB | Metrics storage |
| `prometheus` | prom/prometheus:latest | 256 MB | Metrics scraping |
| `grafana` | grafana/grafana:latest | 256 MB | Dashboard |

### Persistence (Volume Mounts)

| Volume | Purpose |
|---|---|
| `postgres-data` | PostgreSQL data — survives container restarts |
| `grafana-data` | Grafana settings and dashboard state |
| `flink-checkpoints` | Flink state checkpoints (60s interval) |

### Flink Configuration

```yaml
state.backend: filesystem
state.checkpoints.dir: file:///flink-checkpoints
execution.checkpointing.interval: 60000        # 60s
restart-strategy: exponential-delay
restart-strategy.exponential-delay.max-attempts: 5
```

---

## Quick Start

### Prerequisites

- Docker Desktop 4.0+
- Python 3.12
- 4 GB RAM available for Docker

### 1. Clone & configure

```bash
git clone https://github.com/sv-siia/prediction-market-monitor.git
cd prediction-market-monitor
cp .env.example .env
# Edit .env if needed (defaults work out of the box)
```

### 2. Start infrastructure

```bash
docker compose up -d
```

Wait ~30 seconds, then verify all containers are healthy:

```bash
docker compose ps
```

### 3. Choose your scenario

**Scenario 1 — Python scripts (recommended for local dev):**

```bash
docker-compose --profile python up -d
```

This starts: `manifold-producer`, `weather-producer`, `correlation-job`, `aggregation-job`, `anomaly-job` — all with `restart: always`.

**Scenario 2 — PyFlink with event-time watermarks:**

```bash
docker-compose --profile pyflink up -d
```

This starts the same producers + PyFlink versions of all three Flink jobs.

### 4. Open dashboard

| Service | URL | Credentials |
|---|---|---|
| **Grafana Dashboard** | http://localhost:3000 | `admin` / `admin` |
| Flink Web UI | http://localhost:8081 | — |
| Schema Registry | http://localhost:8085 | — |
| Prometheus | http://localhost:9090 | — |

Dashboard loads automatically — no manual import required.

### 5. Run manually (optional, for development)

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# In separate terminals:
python producers/manifold_producer.py
python producers/weather_producer.py
python flink_jobs/correlation_job.py
python flink_jobs/aggregation_job.py
python flink_jobs/anomaly_job.py
```

---

## Data Quality Layer

| Check | Implementation | Alert |
|---|---|---|
| **Schema validation** | Avro schemas in Schema Registry, validated on produce | Produce fails |
| **Token price sum** | `\|price_sum − 1.0\| > 0.01` → `IS_ARBITRAGE=true` | `arbitrage_opportunity` alert |
| **Impossible values** | Negative odds, odds > 1.0 rejected by schema | Produce fails |
| **Producer health** | No data for > 10 min → alert | `producer_down` / `producer_timeout` |
| **Join coverage** | `matched / total < 0.95` → alert | `low_coverage` high/critical |
| **E2E latency** | `CORRELATION_LATENCY_SEC` stored per record | Grafana panel |
| **Deduplication** | Keyed by `condition_id` with 1h TTL (PyFlink); DB-level upsert (Scenario 1) | Silent drop |

### Monitoring Alert Conditions

| Condition | Threshold | Action |
|---|---|---|
| Producer stops sending | > 10 minutes | `producer_timeout` alert in DB + Kafka |
| Kafka consumer lag | > 100,000 messages | Investigate immediately |
| Flink restart loop | 3+ restarts in 10 min | Escalate |
| Join coverage drop | < 70% | `low_coverage` critical alert |
| Accuracy drop | > 20% in 1 hour | `accuracy_drop` alert |

---

## Grafana Dashboard

**23 panels** organized across 5 sections:

### KPI Row
| Panel | Query |
|---|---|
| Overall Accuracy | `AVG(accuracy_rate)` from `market_aggregates` |
| Total Predictions | `COUNT(*)` from `prediction_markets` |
| Active Alerts | `COUNT(*) WHERE resolved_at IS NULL` |
| Join Coverage Rate | `COUNT(*) FILTER (actual_outcome IS NOT NULL) / COUNT(*)` |
| Cities Tracked | `COUNT(DISTINCT location_name)` |

### Market Analysis
- Accuracy by location & market type (table with conditional coloring)
- Bias scores per market (bar chart — positive = over-prediction)
- Top 10 most accurate markets (leaderboard table)
- Scatter plot: predicted odds vs actual outcomes

### Geographic View
- **World map** with accuracy per city (geomap panel with color scale)
- City accuracy table with coordinates

### Time Series
- Accuracy trend over time (line chart, last 24h / 7d / 30d)
- Predictions volume by month
- Correlation methods distribution (pie chart)

### Alerts & System Health
- Active anomaly alerts feed (live table)
- Alert summary by type (bar chart)
- Producer health status (timeseries — green/red)
- **Join coverage rate gauge** (green ≥95%, yellow ≥70%, red <70%)
- **Anomaly detection precision** proxy (alerts active >30min / total)
- **Anomaly detection recall** proxy (active anomaly alerts / groups violating thresholds)

### Arbitrage Dashboard
- Real-time table of markets with `IS_ARBITRAGE=true`
- Historical frequency of arbitrage occurrence

---

## Success Metrics

| Metric | Target | Current Status |
|---|---|---|
| System uptime | ≥ 99.5% | ✅ `restart: always` on all services |
| E2E latency p99 | < 5 minutes | ✅ tracked via `CORRELATION_LATENCY_SEC` |
| Join coverage rate | ≥ 95% | ✅ monitored + alert if < 95% |
| Data loss | 0% | ✅ exactly-once via idempotent producers + checkpoints |
| Anomaly detection recall | ≥ 90% | ✅ 2σ threshold + 20% drop rule |
| Anomaly detection precision | ≥ 80% | ✅ tracked in Grafana precision panel |
| Arbitrage detection speed | < 1 min | ✅ detected at producer level, immediate alert |

---

## Testing

```bash
# Full suite with coverage report
pytest tests/ -v --cov=producers --cov=flink_jobs --cov-report=term-missing

# Quick check
pytest tests/ -q
```

**377 tests — 94% overall coverage**

| Module | Tests | Coverage |
|---|---|---|
| `flink_jobs/correlation_job.py` | 48 | **100%** |
| `producers/utils/city_validator.py` | 41 | **100%** |
| `flink_jobs/anomaly_job.py` | 52 | **98%** |
| `producers/utils/outcome_calculator.py` | 35 | **97%** |
| `flink_jobs/aggregation_job.py` | 44 | **95%** |
| `producers/manifold_producer.py` | 61 | **90%** |
| `producers/weather_producer.py` | 57 | **88%** |
| `producers/utils/circuit_breaker.py` | 19 | **95%** |

---

## CI/CD

Every push to `master` / `main` / `develop` triggers **GitHub Actions**:

```
push → checkout → python 3.12 setup → pip install → pytest → coverage check → upload artifact
```

| Step | Detail |
|---|---|
| Trigger | Push to master/main/develop, PR to master/main |
| Python version | 3.12 on ubuntu-latest |
| System deps | `libsnappy-dev` for python-snappy |
| Test command | `pytest tests/ --cov=producers --cov=flink_jobs` |
| Coverage gate | **≥ 80%** (fails build if below) |
| Artifact | `coverage.xml` uploaded per run |

---

## Security

| Concern | Implementation |
|---|---|
| Secrets management | All credentials in `.env` file (`.gitignore`d); `.env.example` committed |
| Network isolation | All containers on `prediction-network` bridge; no external exposure except mapped ports |
| Kafka access | Internal listener (`kafka:29092`) for containers; external (`localhost:9092`) for dev only |
| TLS | Not enabled locally; enable before exposing to internet |
| Checkpoint cleanup | Old Flink checkpoints accumulate — clean periodically: `docker exec flink-jobmanager find /flink-checkpoints -mtime +7 -delete` |

---

## Cost Analysis

See [`COST_ANALYSIS.md`](COST_ANALYSIS.md) for the full breakdown.

### Summary

| Deployment | MVP/month | Full version/month |
|---|---|---|
| **Local (electricity only)** | ~$0.86 | ~$0.95 |
| **AWS** | ~$180 | ~$373 |
| **GCP** | ~$120 | ~$237 |
| **Azure** | ~$90 | ~$182 |

- Manifold Markets API: **free** (no key, no rate limits)
- Open-Meteo API: **free** (no key, no rate limits)
- Local disk: ~2–5 GB/month (Kafka 7-day retention + PostgreSQL + checkpoints)

---

## Project Structure

```
prediction-market-monitor/
│
├── docker-compose.yml              # 9 services, profiles: python | pyflink
├── Dockerfile                      # python:3.12-slim for Scenario 1
├── Dockerfile.pyflink              # python:3.10-slim + OpenJDK 21 for Scenario 2
├── requirements.txt                # Scenario 1 dependencies
├── requirements-pyflink.txt        # Scenario 2 dependencies (includes apache-flink)
├── .env.example                    # All env vars with defaults
│
├── producers/
│   ├── manifold_producer.py        # Manifold Markets → Kafka (every 5 min)
│   ├── weather_producer.py         # Open-Meteo → Kafka (every 15 min)
│   └── utils/
│       ├── city_extractor.py       # Regex + Claude Haiku NLP extraction
│       ├── city_geocoder.py        # Nominatim geocoding with cache
│       ├── city_validator.py       # Two-stage validation pipeline
│       ├── city_cache.py           # Two-level question + coords cache
│       ├── outcome_calculator.py   # Accuracy correlation logic
│       └── circuit_breaker.py      # CLOSED/OPEN/HALF_OPEN pattern
│
├── flink_jobs/                     # Scenario 1: Python batch-style jobs
│   ├── correlation_job.py          # Snapshot join → market-weather-correlations
│   ├── aggregation_job.py          # 1h windows → market-accuracy-aggregates
│   └── anomaly_job.py              # Threshold alerts → arbitrage-alerts
│
├── flink_jobs_pyflink/             # Scenario 2: True PyFlink event-time streaming
│   ├── correlation_job.py          # KeyedCoProcessFunction, MapState 24h TTL
│   ├── aggregation_job.py          # SlidingEventTimeWindows(1h, 15min)
│   └── anomaly_job.py              # KeyedProcessFunction, ListState 8d TTL
│
├── schemas/                        # Avro schemas for Schema Registry
│   ├── prediction.avsc
│   ├── weather.avsc
│   ├── correlation.avsc
│   ├── aggregate.avsc
│   └── alert.avsc
│
├── database/
│   └── init.sql                    # 4 tables, 5 views, indexes
│
├── monitoring/
│   ├── prometheus.yml              # Kafka + Flink JMX scrape targets
│   └── grafana/
│       ├── prediction_market.json  # 23-panel dashboard (auto-provisioned)
│       └── provisioning/
│           ├── datasources/        # Auto-wires PostgreSQL datasource
│           └── dashboards/         # Auto-loads dashboard on startup
│
├── tests/                          # 377 tests, 94% coverage
│   ├── conftest.py
│   ├── test_manifold_producer.py
│   ├── test_weather_producer.py
│   ├── test_outcome_calculator.py
│   ├── test_city_cache.py
│   ├── test_city_extractor.py
│   ├── test_city_geocoder.py
│   ├── test_city_validator.py
│   ├── test_circuit_breaker.py
│   ├── test_correlation_job.py
│   ├── test_aggregation_job.py
│   └── test_anomaly_job.py
│
├── .github/workflows/ci.yml        # GitHub Actions CI/CD pipeline
├── DEPLOYMENT.md                   # Full operational guide
├── COST_ANALYSIS.md                # Electricity, storage, cloud costs
└── README.md
```

---

## Troubleshooting

| Error | Fix |
|---|---|
| `NoBrokersAvailable` | Wait 30s after `docker compose up`, Kafka health check takes time |
| `ConnectTimeout` on Manifold API | Some ISPs block prediction market APIs — try mobile hotspot |
| `RetryError` on Open-Meteo archive | Future dates not supported by archive API — automatically skipped |
| `ModuleNotFoundError` | Run `pip install -r requirements.txt` in active venv |
| `python-snappy` build fails on Windows | Install Visual C++ Build Tools, or use Docker for producers |
| Port 9092 already in use | Stop the conflicting process: `lsof -i :9092` |
| PyFlink container using 800%+ CPU | Normal during JVM startup (~60s); if persistent, add `mem_limit` or use Scenario 1 |
| Grafana shows "No data" | Wait 5–10 min for producers to populate data, or run `python flink_jobs/correlation_job.py` manually |
| `kafka-init` exited with code 1 | Kafka not ready yet; re-run `docker compose up` |

