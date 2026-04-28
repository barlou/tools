# tools

![CI](https://github.com/barlou/tools/actions/workflows/ci.yml/badge.svg?branch=main)
![Release](https://img.shields.io/github/v/release/barlou/tools)
![Python](https://img.shields.io/badge/python-3.11+-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Security](https://img.shields.io/badge/security-gitleaks-red)

Data engineering utility packages for cloud storage, structured logging, and data archiving.

Three independent, installable Python packages designed to work together or standalone
in any pipeline — Airflow tasks, RL training loops, batch jobs, or ad-hoc scripts.

---

## Packages

| Package | Purpose | Docs |
|---|---|---|
| [`cloud_client`](cloud_client/) | Upload, download, delete and list objects on AWS S3 or OVH Object Storage | [README](cloud_client/README.md) |
| [`log_framework`](log_framework/) | Structured local logging with configurable cloud flush strategies | [README](log_framework/README.md) |
| [`archive_manager`](archive_manager/) | Partition Parquet / ORC files by year/month and bundle logs into `.zip` | [README](archive_manager/README.md) |

---

## Repository structure

```
tools/
├── cloud_client/               # Provider-agnostic cloud storage client
│   ├── config/
│   │   └── config.template.json
│   ├── src/
│   │   ├── __init__.py         # CloudClientFactory
│   │   ├── base.py             # Abstract base + RetryConfig
│   │   ├── config.py           # Credential loader (env → config.json)
│   │   └── providers/
│   │       ├── aws/s3.py       # AWS S3 + multipart upload
│   │       ├── ovh/            # OVH Object Storage (S3-compatible)
│   │       ├── azure/          # Placeholder — v2
│   │       └── gcp/            # Placeholder — v2
│   ├── pyproject.toml
│   └── requirements.txt
│
├── log_framework/              # Structured logging with cloud flush
│   ├── src/
│   │   ├── __init__.py         # DataLogger + all flush strategies
│   │   ├── record.py           # Immutable LogRecord + LogLevel
│   │   ├── logger.py           # DataLogger
│   │   └── flush.py            # EndOfPipeline / OncePerDay / Shutdown / Composite
│   ├── pyproject.toml
│   └── requirements.txt
│
├── archive_manager/            # Hive-partitioned data archiving
│   ├── src/
│   │   ├── __init__.py         # Archiver + ArchiveConfig + ArchiveResult
│   │   └── archiver.py
│   ├── pyproject.toml
│   └── requirements.txt
│
├── .github/workflows/
│   └── ci.yml                  # barlou/CICD — security gate + release
└── cicd.config.yml             # Pipeline configuration (SSM secrets map)
```

---

## Installation

Packages depend on each other in this order — install sequentially:

```bash
pip install -e cloud_client/      # no local deps
pip install -e log_framework/     # needs cloud_client
pip install -e archive_manager/   # needs cloud_client + log_framework
```

Install all three with dev dependencies for local development:

```bash
pip install -e "cloud_client/[dev]"
pip install -e "log_framework/[dev]"
pip install -e "archive_manager/[dev]"
```

**Python requirement:** `>= 3.10`

---

## Credentials

Credentials are resolved in this order — highest priority wins:

```
Environment variables  →  config.json (local dev)  →  AWS SSM (server, via deploy.sh)
```

For local development, copy the template and fill in your values:

```bash
cp cloud_client/config/config.template.json cloud_client/config/config.json
```

`config.json` is gitignored. In CI/CD, credentials are injected as environment
variables from GitHub Secrets and AWS SSM — nothing is ever committed to the repository.

---

## Quick examples

### Store a file on S3

```python
from cloud_client import CloudClientFactory

client = CloudClientFactory.s3("my-data-bucket")
client.upload("local/output.parquet", "processed/2024-01/output.parquet")
```

### Log a pipeline run and flush to S3 at the end

```python
from cloud_client import CloudClientFactory
from log_framework import DataLogger, EndOfPipelineFlush

client = CloudClientFactory.s3("my-data-bucket")
logger = DataLogger("IngestTask", flush_strategy=EndOfPipelineFlush(client))

logger.info("ingest", "Started", extra={"symbol": "BTCUSDT", "rows": 50_000})
logger.error("ingest", "Row skipped", exc=some_exception)
logger.flush()   # one S3 PUT at task end
```

### Log RL training results — one S3 upload per day

```python
from log_framework import DataLogger, OncePerDayFlush, ShutdownFlush, CompositeFlush

logger = DataLogger(
    "RL-Training",
    flush_strategy=CompositeFlush([
        OncePerDayFlush(client),   # at most one S3 PUT per UTC day
        ShutdownFlush(client),     # also flush on SIGTERM (spot VM safety)
    ]),
    log_subdir="results",
)

for episode in range(100_000):
    reward = env.step(action)
    logger.log_rl_result("train", episode=episode, reward=reward, symbol="BTCUSDT")
    logger.flush()   # no-op until UTC day changes
```

### Partition and archive a data file

```python
from archive_manager import Archiver, ArchiveConfig

archiver = Archiver(client, logger, config=ArchiveConfig(
    location_archive="crypto/archive",
    output_format="parquet",
    compression="zstd",
))

result = archiver.archive(
    source_key="crypto/in_progress/BTC/data_BTCUSDT_1h.parquet",
    symbol="BTCUSDT",
    timeframe="1h",
)
# Produces:
# crypto/archive/BTC/year=2024/month=01/data_BTCUSDT_1h_202401.parquet
# crypto/archive/BTC/year=2024/month=02/data_BTCUSDT_1h_202402.parquet
# Source file deleted after all partitions confirmed
```

---

## CI/CD

This repository uses [barlou/CICD](https://github.com/barlou/CICD) reusable workflows.

The pipeline is triggered manually via `workflow_dispatch` and runs:

1. **Security gate** — gitleaks (secret scanning) + semgrep (SAST) + pip-audit (dependency vulnerabilities)
2. **Release** — semver tag and changelog on `main`

All credentials used by the pipeline are stored as GitHub Secrets and AWS SSM
parameters — see `cicd.config.yml` for the full secrets mapping.

---

## Provider roadmap

| Provider | Status |
|---|---|
| AWS S3 | ✅ v1 |
| OVH Object Storage | ✅ v1 (S3-compatible) |
| Azure Blob Storage | 🔜 v2 |
| GCP Cloud Storage | 🔜 v2 |
