# Cost Analysis — Real-Time Prediction Market Pipeline

> Estimated operational costs for running this pipeline locally and in the cloud.
> All API costs are **zero** — Polymarket (public read API) and Open-Meteo are free with no rate limits.

---

## 1. Local Infrastructure

### Container Resource Usage (measured)

| Container | CPU (avg) | Memory | Role |
|---|---|---|---|
| kafka | ~5% | 512 MB | Message broker |
| zookeeper | ~0.2% | 90 MB | Kafka coordination |
| schema-registry | ~1% | 185 MB | Avro schema store |
| postgres | ~0.5% | 54 MB | Persistent storage |
| grafana | ~1% | 165 MB | Dashboard |
| prometheus | ~2.5% | 87 MB | Metrics scraping |
| flink-jobmanager | ~1.5% | 120 MB | Flink cluster |
| flink-taskmanager | ~1.5% | 158 MB | Flink execution |
| manifold-producer | ~2% | 120 MB | Polymarket polling |
| weather-producer | ~1% | 110 MB | Open-Meteo polling |
| correlation-job | ~3% | 150 MB | Join engine |
| aggregation-job | ~3% | 150 MB | Windowed metrics |
| anomaly-job | ~3% | 150 MB | Alert detection |
| **TOTAL** | **~25% of 1 core** | **~1.85 GB** | |

### Electricity Cost (24/7 operation)

Assumptions:
- Host machine: modern laptop/desktop, ~65W total power draw under this load
- Pipeline adds ~15% load → **~10W additional draw**
- Electricity rate: **$0.12/kWh** (US average)

| Period | Calculation | Cost |
|---|---|---|
| Per hour | 10W × 1h = 0.01 kWh × $0.12 | **$0.0012** |
| Per day | 0.01 kWh × 24h × $0.12 | **$0.029** |
| Per month | 0.01 kWh × 720h × $0.12 | **$0.86** |
| Per year | 0.01 kWh × 8760h × $0.12 | **$10.51** |

> For Europe (e.g., Ukraine ~$0.07/kWh): ~$0.50/month, ~$6/year.

---

## 2. API Costs

| API | Cost | Notes |
|---|---|---|
| Polymarket / Manifold | **$0** | Public REST API, no auth required |
| Open-Meteo | **$0** | Free tier: unlimited calls, no key needed |
| **Total API cost** | **$0/month** | |

---

## 3. Storage Projections

### Kafka Retention

- **polymarket-predictions-raw**: ~50 markets × 1 record/5min = 600 records/hour → ~14,400/day
- Each record ~2 KB (Avro) → **~28 MB/day**, retained 7 days → **~196 MB**
- **weather-actuals-raw**: ~20 cities × 1 record/15min = 80 records/hour → 1,920/day
- Each record ~1 KB → **~2 MB/day**, retained 7 days → **~14 MB**
- **market-weather-correlations**: ~500 records/day × 3 KB → **~1.5 MB/day** → **~10 MB/7d**
- **market-accuracy-aggregates** + **arbitrage-alerts**: ~200 records/day combined → **~5 MB/7d**

**Total Kafka storage: ~225 MB** (7-day retention)

### PostgreSQL Data

| Table | Growth rate | 30-day size |
|---|---|---|
| prediction_markets | ~14,400 rows/day | ~430,000 rows → ~200 MB |
| market_correlations | ~500 rows/day | ~15,000 rows → ~15 MB |
| market_aggregates | ~200 rows/day | ~6,000 rows → ~5 MB |
| anomaly_alerts | ~20 rows/day | ~600 rows → ~1 MB |
| pipeline_health | ~50 rows/day | ~1,500 rows → ~1 MB |
| **Total PostgreSQL** | | **~222 MB/month** |

### Flink Checkpoints

- Checkpoint every 60 seconds, ~5 MB per checkpoint
- Keep last 3 checkpoints → **~15 MB** at any time

### Total Local Disk Usage

| Component | 30-day estimate |
|---|---|
| Kafka logs | ~1 GB (7-day rolling) |
| PostgreSQL | ~220 MB |
| Flink checkpoints | ~15 MB |
| Docker images | ~8 GB (one-time) |
| **Total** | **~9.5 GB** |

---

## 4. Total Estimated Operational Cost

### MVP (Components 1–3 + basic validation)

| Cost item | Monthly | Yearly |
|---|---|---|
| Electricity (local) | $0.86 | $10.51 |
| API costs | $0 | $0 |
| Storage (local disk) | $0 | $0 |
| **Total MVP** | **$0.86/month** | **$10.51/year** |

### Full Version (all components)

| Cost item | Monthly | Yearly |
|---|---|---|
| Electricity (local, +10% for extra jobs) | $0.95 | $11.40 |
| API costs | $0 | $0 |
| Storage (local disk, amortized) | $0 | $0 |
| **Total Full Version** | **$0.95/month** | **$11.40/year** |

> **Conclusion:** Running this pipeline locally costs under $1/month — essentially free beyond electricity.

---

## 5. Cloud Deployment Cost Estimate

If deployed to a production cloud environment (AWS / GCP / Azure), costs change significantly due to managed services.

### Architecture assumptions for cloud:
- Managed Kafka (MSK / Confluent Cloud / Event Hubs)
- Managed Flink (Kinesis Data Analytics / Dataflow / Azure Stream Analytics)
- Managed PostgreSQL (RDS / Cloud SQL / Azure Database)
- Grafana Cloud or self-hosted on a VM

---

### AWS Estimate (us-east-1)

| Service | Tier | Monthly cost |
|---|---|---|
| Amazon MSK (Kafka) | 2× kafka.t3.small brokers | ~$100 |
| Amazon Kinesis Data Analytics (Flink) | 2 KPU (1 vCPU, 4 GB each) | ~$220 |
| Amazon RDS PostgreSQL | db.t3.micro, 20 GB SSD | ~$25 |
| EC2 for producers (t3.small) | 1 instance, 2 vCPU, 2 GB | ~$15 |
| Grafana (self-hosted on t3.micro) | 1 instance | ~$8 |
| S3 (Flink checkpoints + logs) | ~10 GB | ~$0.23 |
| Data transfer | Minimal internal | ~$5 |
| **AWS Total** | | **~$373/month** |

### GCP Estimate (us-central1)

| Service | Tier | Monthly cost |
|---|---|---|
| Confluent Cloud on GCP (Kafka) | Basic cluster, 1 CKU | ~$65 |
| Google Dataflow (Flink-compatible) | 2 n1-standard-1 workers | ~$140 |
| Cloud SQL PostgreSQL | db-f1-micro, 20 GB | ~$20 |
| GCE for producers (e2-small) | 2 vCPU, 2 GB | ~$12 |
| Grafana Cloud (free tier) | Up to 10k metrics | $0 |
| Cloud Storage (checkpoints) | ~10 GB | ~$0.20 |
| **GCP Total** | | **~$237/month** |

### Azure Estimate (East US)

| Service | Tier | Monthly cost |
|---|---|---|
| Azure Event Hubs (Kafka-compatible) | Standard, 1 TU | ~$22 |
| Azure Stream Analytics | 1 streaming unit | ~$80 |
| Azure Database for PostgreSQL | Burstable B1ms, 32 GB | ~$25 |
| Azure Container Instances (producers) | 1 vCPU, 2 GB | ~$35 |
| Azure Managed Grafana | Basic tier | ~$20 |
| Azure Blob Storage (checkpoints) | ~10 GB | ~$0.18 |
| **Azure Total** | | **~$182/month** |

---

### Cloud Cost Comparison Summary

| Provider | MVP/month | Full Version/month | Yearly (Full) |
|---|---|---|---|
| **Local** | $0.86 | $0.95 | **$11.40** |
| **AWS** | ~$180 | ~$373 | **~$4,476** |
| **GCP** | ~$120 | ~$237 | **~$2,844** |
| **Azure** | ~$90 | ~$182 | **~$2,184** |

> **Azure** is the cheapest cloud option for this workload due to Event Hubs Kafka compatibility and competitive Stream Analytics pricing.
> **GCP** offers the best developer experience with Dataflow's native Apache Beam/Flink compatibility.
> **AWS** is the most expensive but offers the most mature managed Kafka (MSK) and monitoring ecosystem.

---

## 6. Cost Optimization Recommendations

| Optimization | Potential saving |
|---|---|
| Use spot/preemptible instances for Flink workers | 60–70% off compute |
| Reduce Kafka retention from 7 days to 3 days | ~50% storage saving |
| Scale producers to 1 instance (they're lightweight) | ~50% on compute |
| Use Grafana Cloud free tier instead of self-hosted | ~$8–20/month saving |
| Implement intelligent polling (skip closed markets) | Reduce API calls by ~40% |
| Compress Kafka messages with Snappy (already configured) | ~30% storage saving |

---

*Analysis based on prices as of June 2026. Cloud pricing subject to change.*
