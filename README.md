  # 🛡️ Data Platform Sentinel

**Production-ready data quality pipeline with Dagster orchestration, Pandera validation, PostgreSQL metadata logging, and Slack alerting.**

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue)](https://python.org)
[![Dagster](https://img.shields.io/badge/Dagster-1.8.5-purple)](https://dagster.io)
[![Pandera](https://img.shields.io/badge/Pandera-0.20.0-green)](https://pandera.readthedocs.io)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Key Features](#key-features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Data Contracts](#data-contracts)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Dagster UI](#dagster-ui)
- [Testing](#testing)
- [Future Optimizations](#future-optimizations)
- [Contributing](#contributing)

---

## 🎯 Overview

**Data Platform Sentinel** is a production-grade data quality platform that acts as a **gatekeeper** for your data warehouse. It validates every incoming dataset against strict schema contracts, logs validation results to PostgreSQL for auditability, and sends real-time Slack alerts when data quality issues are detected.

The pipeline is intentionally designed with **deliberately bad data** (negative order amounts) to prove that the validation layer actually works — preventing bad data from poisoning downstream analytics.

### What This Project Demonstrates

| Capability | Implementation |
|-----------|----------------|
| **Data Contract Enforcement** | Pandera schemas validate types, ranges, regex patterns |
| **Pipeline Orchestration** | Dagster assets with automatic DAG dependency resolution |
| **Incremental Processing** | Daily partitions for date-based incremental loads |
| **Parallel Execution** | Multiprocess executor for independent assets |
| **Metadata Logging** | PostgreSQL audit trail of all pass/fail events |
| **Real-time Alerting** | Slack webhook notifications on validation failures |
| **External Data Sources** | CSV ingestion with extensible API/S3/Kafka stubs |
| **Engine Flexibility** | Pandas or Polars DataFrame engine toggle |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA PLATFORM SENTINEL                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  External Data Sources                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │
│  │  CSV Files  │  │    APIs     │  │  S3 Buckets │  │    Kafka    │   │
│  │  (default)  │  │   (stub)    │  │   (stub)    │  │   (stub)    │   │
│  └──────┬──────┘  └─────────────┘  └─────────────┘  └─────────────┘   │
│         │                                                               │
│         ▼                                                               │
│  ┌─────────────────────────────────────────┐                            │
│  │     services/data_source.py             │                            │
│  │  • Configurable loader (CSV/API/S3)     │                            │
│  │  • Pandas / Polars engine toggle        │                            │
│  │  • Partition date filtering             │                            │
│  └──────────────────┬──────────────────────┘                            │
│                     │                                                   │
│         ┌───────────┴───────────┐                                       │
│         ▼                       ▼                                       │
│  ┌─────────────┐         ┌─────────────┐                                │
│  │  raw_orders │         │customers_raw│                                │
│  │  (ingest)   │         │  (ingest)   │                                │
│  └──────┬──────┘         └──────┬──────┘                                │
│         │                       │                                       │
│         ▼                       ▼                                       │
│  ┌─────────────────┐     ┌─────────────────┐                          │
│  │ validated_orders│     │validated_customers                         │
│  │  (Pandera:      │     │  (Pandera:      │                          │
│  │   OrderSchema)  │     │ CustomerSchema) │                          │
│  └────────┬────────┘     └────────┬────────┘                          │
│           │                       │                                     │
│           └───────────┬───────────┘                                     │
│                       ▼                                                 │
│              ┌─────────────────┐                                        │
│              │   metadata/db.py│                                        │
│              │  PostgreSQL logs│                                        │
│              │  • pass/fail    │                                        │
│              │  • partition_date│                                       │
│              │  • error details│                                        │
│              └─────────────────┘                                        │
│                       │                                                 │
│                       ▼                                                 │
│              ┌─────────────────┐                                        │
│              │services/alerting│                                        │
│              │  Slack webhook  │                                        │
│              │  notifications  │                                        │
│              └─────────────────┘                                        │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Asset Dependency Graph

```
┌─────────────────┐         ┌─────────────────────┐
│   raw_orders    │────────▶│  validated_orders   │
│  (CSV loader)   │         │ (Pandera validated) │
└─────────────────┘         └─────────────────────┘

┌─────────────────┐         ┌─────────────────────┐
│ customers_raw   │────────▶│ validated_customers │
│  (CSV loader)   │         │ (Pandera validated) │
└─────────────────┘         └─────────────────────┘
```

---

## ✨ Key Features

### 1. Data Contract Enforcement with Pandera
Every dataset is validated against a strict schema before reaching your warehouse:
- **Type coercion** — automatic type conversion with validation
- **Range checks** — numeric bounds (e.g., `amount >= 0`)
- **Regex validation** — email format, string patterns
- **Nullable support** — optional fields with null handling

### 2. Dagster Asset Orchestration
- **Declarative dependencies** — Dagster automatically builds the execution DAG
- **Daily partitioning** — process data incrementally by date instead of full loads
- **Parallel execution** — independent assets run simultaneously via multiprocess executor
- **Materialization tracking** — every asset output is versioned and tracked

### 3. Production Observability
- **PostgreSQL metadata store** — complete audit trail of all validation events
- **Partition-aware logging** — track which date partition had issues
- **Slack alerting** — instant notifications when validation fails
- **Dagster UI** — visual asset graph, run history, and partition browser

### 4. Extensible Data Sources
The `services/data_source.py` loader is designed for easy extension:
- **CSV** (default) — local file-based ingestion with partition filtering
- **API** (stub) — ready for REST API integration
- **S3** (stub) — ready for cloud object storage via `dagster-aws`
- **Polars support** — toggle between Pandas and Polars for 10-50x performance

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Orchestrator** | Dagster 1.8.5 | Pipeline DAG definition and execution |
| **Data Validation** | Pandera 0.20.0 | Schema contracts and DataFrame validation |
| **Metadata Store** | PostgreSQL 14 + SQLAlchemy | Audit logging and observability |
| **Alerting** | Slack Webhooks | Real-time failure notifications |
| **Data Processing** | Python 3.10+ + Pandas/Polars | Data ingestion and transformation |
| **Containerization** | Docker Compose | Local PostgreSQL development |
| **Scheduler** | Dagster Schedules | Daily cron-based pipeline execution |

---

## 📁 Project Structure

```
data-platform-sentinel/
│
├── 📂 data/                          # External data sources
│   ├── orders.csv                    # Sample orders data with ingestion_date
│   └── customers.csv                 # Sample customers data with ingestion_date
│
├── 📂 orchestrator/                  # Dagster orchestration layer
│   ├── __init__.py
│   ├── repository.py                 # Definitions: assets, jobs, schedules, executor
│   ├── partitions.py                 # DailyPartitionsDefinition (shared)
│   └── 📂 assets/
│       ├── __init__.py
│       ├── orders.py                 # raw_orders, validated_orders
│       └── customers.py              # customers_raw, validated_customers
│
├── 📂 data_contracts/                # Pandera schema definitions
│   ├── orders_schema.py              # OrderSchema: order_id, amount, customer_id
│   └── customers_schema.py           # CustomerSchema: id, name, email
│
├── 📂 metadata/                      # Observability layer
│   └── db.py                         # PostgreSQL logging utility
│
├── 📂 services/                      # Shared services
│   ├── alerting.py                   # Slack webhook integration
│   └── data_source.py                # Configurable data loader (CSV/API/S3/Polars)
│
├── 📂 scripts/                       # Utility scripts
│   ├── test_validation.py            # Standalone Pandera test (no Dagster/DB)
│   ├── run_pipeline.py               # Full pipeline materialization
│   ├── demo_orchestration.py         # Step-by-step execution demo
│   └── migrate_db.py                 # DB migration for data_quality_logs table
│
├── docker-compose.yml                # PostgreSQL 14 container
├── requirements.txt                  # Python dependencies
├── workspace.yaml                    # Dagster workspace configuration
├── setup.bat                         # Windows setup script
├── .gitignore                        # Git ignore rules
├── README.md                         # This file
├── ORCHESTRATION.md                  # Detailed orchestration guide
├── OPTIMIZATION.md                   # Future optimization roadmap
└── TODO.md                           # Implementation checklist
```

---

## 📋 Prerequisites

- **Python** 3.10 or higher
- **pip** or **conda** for package management
- **Docker Desktop** (optional, for PostgreSQL)
- **Git** for version control
- **Slack Webhook URL** (optional, for alerts)

---

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/data-platform-sentinel.git
cd data-platform-sentinel
```

### 2. Create Virtual Environment

```bash
# Windows
python -m venv .venv
.venv\Scripts\activate

# macOS/Linux
python -m venv .venv
source .venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. (Optional) Start PostgreSQL

```bash
docker compose up -d
```

### 5. (Optional) Run DB Migration

```bash
python scripts/migrate_db.py
```

---

## ⚡ Quick Start

### Option A: Standalone Validation Test (Fastest — No Dagster/DB/Docker)

Tests Pandera schema validation directly without any infrastructure:

```bash
python scripts/test_validation.py
```

**Expected Output:**
```
Raw orders data:
   order_id  amount  customer_id
0         1     100           10
1         2     -50           20
2         3     200           30

Validation FAILED (as expected): Column 'amount' failed element-wise validator number 0: greater_than_or_equal_to(0) failure cases: -50.0
```

✅ **The data contract correctly rejected the negative amount!**

---

### Option B: Demo Orchestration (Step-by-Step)

Shows each asset executing individually, then the full pipeline:

```bash
# Run for a specific date partition
set PARTITION_KEY=2025-01-15     # Windows
# export PARTITION_KEY=2025-01-15 # macOS/Linux

python scripts/demo_orchestration.py
```

**Expected Output:**
```
=== Dagster Orchestration Demo ===
Partition: 2025-01-15

1. Raw Orders & Customers...
   raw_orders: True
   order_id  amount  customer_id
0         1   100.0           10
1         2   -50.0           20
2         3   200.0           30

2. Raw Customers...
   customers_raw: True
   id   name            email
0   1  Alice  alice@email.com
1   2    Bob    bob@email.com

3. Orchestrated Validation (full pipeline)...
Pipeline success: False
Failures: Data validation failed.
  - validated_orders failed

Demo complete! Multiprocessing is configured in repository.py for Dagster UI/daemon runs.
```

---

### Option C: Full Pipeline Run

Materializes all assets in dependency order:

```bash
set PARTITION_KEY=2025-01-15     # Windows
python scripts/run_pipeline.py
```

| Asset | Expected Status |
|-------|----------------|
| `customers_raw` | ✅ SUCCESS |
| `raw_orders` | ✅ SUCCESS |
| `validated_customers` | ✅ SUCCESS |
| `validated_orders` | ❌ FAILURE (intentional — bad data caught) |

---

### Option D: Dagster Web UI

Launch the interactive Dagster UI to visualize the asset graph, browse partitions, and inspect run history:

```bash
dagster dev
```

Then open **http://localhost:3000** in your browser.

**What You'll See:**
- Asset dependency graph with all 4 assets
- Daily partitions from 2025-01-01 onwards
- Run history and materialization status
- Job `data_quality_job` with schedule `daily_schedule`

---

## 📜 Data Contracts

### OrderSchema (`data_contracts/orders_schema.py`)

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `order_id` | `int` | `> 0` | Unique order identifier |
| `amount` | `float` | `>= 0` | Order amount (must be non-negative) |
| `customer_id` | `int` | `> 0` | Reference to customer |

```python
OrderSchema = pa.DataFrameSchema({
    "order_id": pa.Column(int, checks=pa.Check.gt(0), coerce=True),
    "amount": pa.Column(float, checks=pa.Check.ge(0), coerce=True),
    "customer_id": pa.Column(int, checks=pa.Check.gt(0), coerce=True),
})
```

### CustomerSchema (`data_contracts/customers_schema.py`)

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `id` | `int` | `> 0` | Unique customer identifier |
| `name` | `string` | length 1-100 | Customer name |
| `email` | `string` | valid email regex | Contact email (nullable) |

```python
CustomerSchema = pa.DataFrameSchema({
    "id": pa.Column(int, checks=pa.Check.gt(0), coerce=True),
    "name": pa.Column(pa.String, checks=pa.Check.str_length(1, 100), coerce=True),
    "email": pa.Column(pa.String, checks=pa.Check.str_matches(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"), nullable=True, coerce=True),
})
```

---

## ⚙️ Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PARTITION_KEY` | Yesterday's date (`YYYY-MM-DD`) | Date partition to process |
| `DATA_DIR` | `./data` | Path to CSV data files |
| `DATA_ENGINE` | `pandas` | DataFrame engine: `pandas` or `polars` |
| `DB_URL` | `postgresql://postgres:postgres@localhost:5432/metadata` | PostgreSQL connection string |
| `SLACK_WEBHOOK` | *(none)* | Slack webhook URL for alerts |

### Examples

```bash
# Process a specific date with Polars engine
set PARTITION_KEY=2025-01-15
set DATA_ENGINE=polars
python scripts/run_pipeline.py

# With Slack alerting
set SLACK_WEBHOOK=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
python scripts/run_pipeline.py
```

---

## 🧪 Testing

### Test Matrix

| Test | Command | What It Tests |
|------|---------|---------------|
| Standalone Validation | `python scripts/test_validation.py` | Pandera schema without Dagster |
| Demo Orchestration | `python scripts/demo_orchestration.py` | Step-by-step asset execution |
| Full Pipeline | `python scripts/run_pipeline.py` | Complete pipeline with partitions |
| Repository Load | `dagster dev` | Dagster UI, jobs, schedules |
| DB Migration | `python scripts/migrate_db.py` | PostgreSQL table setup |

---

## 🚀 Future Optimizations

This project is designed as a **production foundation** ready to scale. See `OPTIMIZATION.md` for the complete roadmap, including:

| Phase | Optimizations |
|-------|--------------|
| **Stability** | pytest suite, CI/CD, retries, timeouts |
| **Scale** | Real APIs/S3/Kafka sources, incremental partitioning, cloud storage |
| **Production** | Quarantine pattern for bad records, Kubernetes deployment, Grafana dashboards, secrets management, RBAC |

**Quick Wins** (implement today):
- Enable multiprocessing: already configured in `repository.py`
- Add retry policies: `RetryPolicy(max_retries=3, delay=5)` on assets
- Switch to Polars: `set DATA_ENGINE=polars`

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License.

---

## 🙏 Acknowledgments

- [Dagster](https://dagster.io/) for the orchestration framework
- [Pandera](https://pandera.readthedocs.io/) for DataFrame validation
- [Pandas](https://pandas.pydata.org/) / [Polars](https://pola.rs/) for data processing

---

**⭐ Star this repository if you find it useful!**
