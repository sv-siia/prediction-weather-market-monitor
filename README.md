# Real-Time Prediction Market Accuracy Monitor

> **Stream-based analytics pipeline** that correlates crowd-sourced weather predictions from Manifold Markets with real weather data from Open-Meteo — surfacing market inefficiencies, systematic biases, and arbitrage opportunities in real time.

[![CI](https://github.com/sv-siia/prediction-weather-market-monitor/actions/workflows/ci.yml/badge.svg)](https://github.com/sv-siia/prediction-weather-market-monitor/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.12-blue)
![Kafka](https://img.shields.io/badge/kafka-7.5.0-black)
![Flink](https://img.shields.io/badge/flink-1.18-orange)
![PostgreSQL](https://img.shields.io/badge/postgresql-15-blue)
![Grafana](https://img.shields.io/badge/grafana-latest-orange)
![Prometheus](https://img.shields.io/badge/prometheus-latest-red)

---

## Table of Contents

- [Why This Exists](#why-this-exists)
- [Architecture](#architecture)
- [Two Execution Scenarios](#two-execution-scenarios)
- [Components](#components)
- [Kafka Topics](#kafka-topics)
- [Quick Start](#quick-start)
- [Monitoring](#monitoring)
- [Testing](#testing)
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
│   api.manifold.markets/v0               api.open-meteo.com          │
│   poll every 5 min                      poll every 15 min           │
└────────────┬────────────────────────────────────┬───────────────────┘
             │                                    │
             ▼                                    ▼
┌────────────────────────┐          ┌─────────────────────────┐
│   manifold_producer.py │          │   weather_producer.py   │
│  • Filter weather mkts │          │  • Dynamic city tracking│
│  • Parse location      │          │  • Geocoding cache      │
│  • Detect arbitrage    │          │  • Historical + current │
│  • Exponential backoff │          │  • Exponential backoff  │
└──────────┬─────────────┘          └───────────┬─────────────┘
           │                                    │
           ▼                                    ▼
┌──────────────────────────┐    ┌───────────────────────────┐
│  manifold-predictions    │    │   weather-actuals-raw     │
│       -raw               │    │   3 partitions · 7d       │
│   3 partitions · 7d      │    └───────────────────────────┘
└──────────────────────────┘                │
           │                                │
           └──────────────┬─────────────────┘
                          │
             ┌────────────▼────────────┐
             │    correlation_job.py   │
             │  • Join by LOCATION_NAME│
             │  • 3 match strategies   │
             │  • Accuracy calculation │
             └────────────┬────────────┘
                          ▼
          ┌───────────────────────────────┐
          │  market-weather-correlations  │
          │   3 partitions · 30d          │
          └───────────────┬───────────────┘
                          │
             ┌────────────▼────────────┐
             │   aggregation_job.py    │
             │  • 1h sliding windows   │
             │  • 15-min slide         │
             │  • Accuracy, bias score │
             └────────────┬────────────┘
                          ▼
          ┌───────────────────────────────┐
          │  market-accuracy-aggregates   │
          │   3 partitions · 30d · compact│
          └───────────────┬───────────────┘
                          │
             ┌────────────▼────────────┐
             │     anomaly_job.py      │
             │  • 7-day rolling baseln │
             │  • 2σ threshold alerts  │
             │  • Arbitrage detection  │
             │  • Producer health      │
             └────────────┬────────────┘
                          ▼
          ┌───────────────────────────────┐
          │  arbitrage-alerts · PostgreSQL│
          │  Grafana Dashboard (23 panels)│
          │  Prometheus metrics           │
          └───────────────────────────────┘
```

---

## Two Execution Scenarios

### Scenario 1 — Python Scripts (default, lightweight)

```bash
docker compose --profile python up -d
```

Traditional Python batch processing. Runs everywhere with minimal resources.

| Component | Implementation |
|---|---|
| Correlation | Snapshot join with 3 match strategies |
| Aggregation | Sliding 1h / 15min windows in Python |
| Anomaly detection | 7-day rolling baseline, 2σ threshold |

### Scenario 2 — PyFlink with Event-Time Windowing

```bash
docker compose --profile pyflink up -d
```

True streaming with Flink's DataStream API, watermarks, and stateful operators.

| Component | Implementation |
|---|---|
| Correlation | `KeyedCoProcessFunction` with `MapState` (24h TTL) |
| Aggregation | `SlidingEventTimeWindows.of(1h, 15min)` |
| Anomaly detection | `KeyedProcessFunction` with `ListState` (8d TTL) |
| Watermarks | `WatermarkStrategy.for_bounded_out_of_orderness(15min)` |

> Scenario 2 requires a Flink cluster with ≥4 GB RAM per job. For local development, Scenario 1 provides equivalent results.

---

## Components

### 1. Manifold Markets Producer (`producers/manifold_producer.py`)

- Polls Manifold API every 5 min using 37 weather search terms
- Extracts city name from market question via regex + NLP fallback
- Detects arbitrage: cross-market price sums > 1.01 and mispricing vs historical base rates
- Exponential backoff via `tenacity` (1s → 16s, 5 attempts)
- Schema validation via Confluent Schema Registry (Avro)
- Exposes Prometheus metrics on port 8000

### 2. Weather Producer (`producers/weather_producer.py`)

- Dynamically discovers cities from `manifold-predictions-raw` topic
- Fetches current weather (open markets) and historical data (closed markets) from Open-Meteo
- Geocoding cache via Nominatim (persisted to `data/geocoding_cache.json`)
- Exposes Prometheus metrics on port 8001

### 3. Correlation Job (`flink_jobs/correlation_job.py`)

Joins predictions with weather data by `LOCATION_NAME`. Three match strategies:

| Method | When Used |
|---|---|
| `winner_known` | Market is closed with YES/NO resolution |
| `historical_weather` | Past date with archived weather data |
| `current_snapshot` | Open market with current weather |

Output fields: `ACTUAL_OUTCOME`, `PREDICTION_ERROR`, `CORRELATION_LATENCY_SEC`

### 4. Aggregation Job (`flink_jobs/aggregation_job.py`)

Computes per `(location, market_type)` over 1h sliding / 15min slide windows:

| Metric | Formula |
|---|---|
| Accuracy rate | `correct / total` |
| Avg prediction error | `sum(|predicted − actual|) / count` |
| Volume-weighted accuracy | `sum(accuracy × volume) / sum(volume)` |
| Bias score | `avg_predicted_odds − avg_actual_outcome` |

### 5. Anomaly Detection (`flink_jobs/anomaly_job.py`)

| Alert Type | Condition | Severity |
|---|---|---|
| `accuracy_drop` | accuracy < 7-day baseline − 2σ | high |
| `bias_detected` | \|bias_score\| > 0.30 | medium |
| `arbitrage_opportunity` | `IS_ARBITRAGE=true` | high |
| `low_coverage` | join coverage < 95% | high / critical |
| `producer_timeout` | no data for > 10 min | critical |

---

## Kafka Topics

| Topic | Partitions | Retention | Policy | Compression |
|---|---|---|---|---|
| `manifold-predictions-raw` | 3 | 7 days | delete | Snappy |
| `weather-actuals-raw` | 3 | 7 days | delete | Snappy |
| `market-weather-correlations` | 3 | 30 days | delete | Snappy |
| `market-accuracy-aggregates` | 3 | 30 days | compact | Snappy |
| `arbitrage-alerts` | 3 | 30 days | compact | Snappy |

All topics are created automatically on first `docker compose up` via `kafka-init` container.

---

## Quick Start

### Prerequisites

- Docker Desktop 4.0+
- 4 GB RAM available for Docker

### 1. Clone & configure

```bash
git clone https://github.com/sv-siia/prediction-weather-market-monitor.git
cd prediction-weather-market-monitor
cp .env.example .env
```

### 2. Start infrastructure

```bash
docker compose up -d
```

Wait ~30 seconds for containers to become healthy:

```bash
docker compose ps
```

### 3. Start the pipeline

```bash
docker compose --profile python up -d
```

### 4. Open dashboards

| Service | URL | Credentials |
|---|---|---|
| Grafana Dashboard | http://localhost:3000 | `admin` / `admin` |
| Flink Web UI | http://localhost:8081 | — |
| Schema Registry | http://localhost:8085 | — |
| Prometheus | http://localhost:9090 | — |

Dashboard loads automatically — no manual import required.

### 5. Run manually (optional)

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python producers/manifold_producer.py
python producers/weather_producer.py
python flink_jobs/correlation_job.py
python flink_jobs/aggregation_job.py
python flink_jobs/anomaly_job.py
```

---

## Monitoring

### Grafana Dashboard (23 panels)

- **KPI row** — Overall accuracy, total predictions, active alerts, cities tracked
- **Market analysis** — Accuracy by location & market type, bias scores, top markets
- **Time series** — Accuracy trend, prediction volume, correlation methods
- **Alerts & health** — Active anomaly alerts, producer status, join coverage gauge
- **Arbitrage** — Real-time table of arbitrage opportunities

### Prometheus Metrics

Each service exposes a `/metrics` endpoint:

| Service | Port |
|---|---|
| manifold_producer | 8000 |
| weather_producer | 8001 |
| correlation_job | 8002 |
| aggregation_job | 8003 |
| anomaly_job | 8004 |

Key metrics: `manifold_markets_produced_total`, `weather_records_produced_total`, `correlation_join_coverage_ratio`, `aggregation_accuracy_rate`, `anomaly_alerts_fired_total`

---

## Testing

```bash
pytest tests/ -v --cov=producers --cov=flink_jobs --cov-report=term-missing
```

**84% overall coverage · 336 tests**

| Module | Coverage |
|---|---|
| `producers/utils/city_validator.py` | 100% |
| `producers/utils/outcome_calculator.py` | 97% |
| `producers/utils/city_cache.py` | 96% |
| `producers/manifold_producer.py` | 87% |
| `producers/weather_producer.py` | 84% |
| `flink_jobs/correlation_job.py` | 78% |
| `flink_jobs/aggregation_job.py` | 78% |

CI runs automatically on every push via GitHub Actions (coverage gate ≥ 80%).

---

## Project Structure

```
prediction-weather-market-monitor/
│
├── docker-compose.yml              # All services, profiles: python | pyflink
├── Dockerfile                      # Python 3.12 for Scenario 1
├── Dockerfile.pyflink              # Python 3.10 + OpenJDK 21 for Scenario 2
├── requirements.txt
├── requirements-pyflink.txt
├── .env.example
│
├── producers/
│   ├── manifold_producer.py
│   ├── weather_producer.py
│   └── utils/
│       ├── outcome_calculator.py   # Accuracy correlation logic
│       ├── city_validator.py       # Two-stage city validation
│       ├── city_cache.py           # Question + coordinates cache
│       ├── city_geocoder.py        # Nominatim geocoding
│       ├── city_extractor.py       # Regex + NLP city extraction
│       ├── schema_registry.py      # Avro serialization
│       ├── circuit_breaker.py      # CLOSED/OPEN/HALF_OPEN pattern
│       └── metrics.py              # Prometheus metrics definitions
│
├── flink_jobs/                     # Scenario 1: Python batch jobs
│   ├── correlation_job.py
│   ├── aggregation_job.py
│   └── anomaly_job.py
│
├── flink_jobs_pyflink/             # Scenario 2: PyFlink streaming
│   ├── correlation_job.py
│   ├── aggregation_job.py
│   └── anomaly_job.py
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
│   ├── prometheus.yml
│   └── grafana/
│       ├── prediction_market.json  # 23-panel dashboard
│       └── provisioning/           # Auto-wires datasource + dashboard
│
├── tests/                          # 336 tests
├── scripts/                        # Utility and diagnostic scripts
├── .github/workflows/ci.yml        # GitHub Actions CI/CD
├── DEPLOYMENT.md                   # Full operational guide
└── COST_ANALYSIS.md                # Local and cloud cost breakdown
```

---

## Troubleshooting

| Error | Fix |
|---|---|
| `NoBrokersAvailable` | Wait 30s after `docker compose up` — Kafka health check takes time |
| `ConnectTimeout` on Manifold API | Some ISPs block prediction APIs — try mobile hotspot |
| `RetryError` on Open-Meteo archive | Future dates not supported — automatically skipped |
| `ModuleNotFoundError` | Run `pip install -r requirements.txt` in active venv |
| `python-snappy` build fails on Windows | Use Docker instead of running producers locally |
| Port 9092 already in use | Find conflicting process: `lsof -i :9092` |
| PyFlink container using 800%+ CPU | Normal during JVM startup (~60s) — if persistent, switch to Scenario 1 |
| Grafana shows "No data" | Wait 5–10 min for producers to populate, or run correlation job manually |
| Flink UI shows 0 running jobs | Normal for Scenario 1 — Python jobs run as processes, not Flink cluster jobs |

---

See [`DEPLOYMENT.md`](DEPLOYMENT.md) for full operational guide and [`COST_ANALYSIS.md`](COST_ANALYSIS.md) for cost breakdown.
