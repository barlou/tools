# archive_manager

Partitioned archive manager for Parquet and ORC data files in cloud storage.

Splits in-progress data files by year/month into a Hive-compatible partition
layout, enabling efficient predicate pushdown in Spark, AWS Athena, and DuckDB.
Also bundles `.log` and result files into `.zip` archives for long-term storage.

---

## Table of contents

- [archive\_manager](#archive_manager)
  - [Table of contents](#table-of-contents)
  - [Installation](#installation)
  - [Quick start](#quick-start)
  - [Partition layout](#partition-layout)
  - [API reference](#api-reference)
    - [Archiver](#archiver)
      - [Constructor](#constructor)
      - [`archive(source_key, symbol, timeframe) → ArchiveResult`](#archivesource_key-symbol-timeframe--archiveresult)
      - [`archive_logs(log_files, remote_prefix="Logs/Archive") → str`](#archive_logslog_files-remote_prefixlogsarchive--str)
    - [ArchiveConfig](#archiveconfig)
    - [ArchiveResult](#archiveresult)
  - [Parquet vs ORC](#parquet-vs-orc)
  - [Compression guide](#compression-guide)
  - [Log archiving](#log-archiving)
  - [Full pipeline example](#full-pipeline-example)

---

## Installation

```bash
# Install sibling packages first — in this order
pip install -e ../cloud_client/
pip install -e ../log_framework/

# Then install archive_manager
pip install -e archive_manager/

# With dev/test dependencies (pytest, moto S3 mock)
pip install -e "archive_manager/[dev]"
```

**Python requirement:** `>= 3.10`

**Additional dependencies:** `pandas >= 2.0`, `pyarrow >= 12.0`

---

## Quick start

```python
from cloud_client import CloudClientFactory
from log_framework import DataLogger
from archive_manager import Archiver, ArchiveConfig

client  = CloudClientFactory.s3("my-data-bucket")
logger  = DataLogger("Archiver")

archiver = Archiver(
    client,
    logger,
    config=ArchiveConfig(
        location_archive="crypto/archive",
        location_in_progress="crypto/in_progress",
        output_format="parquet",
        compression="zstd",
    ),
)

result = archiver.archive(
    source_key="crypto/in_progress/BTC/data_BTCUSDT_1h.parquet",
    symbol="BTCUSDT",
    timeframe="1h",
)

print(result.success)              # True
print(result.partitions_uploaded)  # ['crypto/archive/BTC/year=2024/month=01/...', ...]
print(result.source_deleted)       # True (deleted after all partitions confirmed)
print(f"{result.duration_seconds:.1f}s")
```

---

## Partition layout

Input (in-progress, one flat file):
```
crypto/in_progress/BTC/data_BTCUSDT_1h.parquet
```

Output (archived, partitioned by year and month):
```
crypto/archive/BTC/year=2024/month=01/data_BTCUSDT_1h_202401.parquet
crypto/archive/BTC/year=2024/month=02/data_BTCUSDT_1h_202402.parquet
crypto/archive/BTC/year=2024/month=03/data_BTCUSDT_1h_202403.parquet
...
```

This follows the [Hive partitioning convention](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL)
used by Spark, AWS Athena, and DuckDB. A query filtered on a specific month
reads only that folder — all other partitions are skipped entirely.

```sql
-- AWS Athena / Spark SQL — reads only month=01 partition
SELECT * FROM crypto_data
WHERE year = 2024 AND month = 1
```

---

## API reference

### Archiver

```python
from archive_manager import Archiver
```

#### Constructor

```python
Archiver(
    client,          # CloudClientBase — any provider (S3, OVH, Azure...)
    logger,          # DataLogger — structured logging of archive events
    config=None,     # ArchiveConfig — defaults to parquet + zstd
)
```

#### `archive(source_key, symbol, timeframe) → ArchiveResult`

Download, partition by year/month, and re-upload a data file.

| Parameter | Type | Description |
|---|---|---|
| `source_key` | `str` | Full remote key of the source file |
| `symbol` | `str` | Asset symbol, e.g. `"BTCUSDT"` |
| `timeframe` | `str` | Candle timeframe, e.g. `"1h"`, `"4h"`, `"1d"` |

```python
result = archiver.archive(
    source_key="crypto/in_progress/BTC/data_BTCUSDT_1h.parquet",
    symbol="BTCUSDT",
    timeframe="1h",
)
```

**Safety guarantee:** The source file is deleted only after every partition is
confirmed uploaded. If any partition upload fails, the source is preserved and
the errors are recorded in `ArchiveResult.errors`.

**Schema handling:** Supports both named OHLCV columns and the Binance API's
numeric column indices (`0, 1, 2, ...`). Numeric indices are remapped
automatically:

| Index | Column name |
|---|---|
| 0 | `open_time` |
| 1 | `open` |
| 2 | `high` |
| 3 | `low` |
| 4 | `close` |
| 5 | `volume` |
| 6 | `close_time` |
| 7 | `quote_asset_volume` |
| 8 | `number_of_trades` |
| 9 | `taker_buy_base_asset_volume` |
| 10 | `taker_buy_quote_asset_volume` |

**Timestamp handling:** `open_time` is parsed from Binance millisecond epoch
format automatically.

#### `archive_logs(log_files, remote_prefix="Logs/Archive") → str`

Bundle local log files into a single `.zip` and upload to cloud storage.

| Parameter | Type | Description |
|---|---|---|
| `log_files` | `list[Path]` | Local `.log` files to bundle |
| `remote_prefix` | `str` | Cloud prefix for the zip file |

Returns the remote key of the uploaded zip.

```python
from pathlib import Path

# Archive pipeline logs
zip_key = archiver.archive_logs(
    log_files=[logger.local_log_file],
    remote_prefix="Logs/Archive",
)
# → "Logs/Archive/logs_2024-03-15.zip"

# Archive multiple log files
zip_key = archiver.archive_logs(
    log_files=[
        Path("/tmp/data_logs/IngestTask_2024-03-15.log"),
        Path("/tmp/data_logs/results/RL-Training_2024-03-15.log"),
    ],
    remote_prefix="Logs/Archive",
)
```

---

### ArchiveConfig

```python
from archive_manager import ArchiveConfig

config = ArchiveConfig(
    location_archive="crypto/archive",        # cloud prefix for archived partitions
    location_in_progress="crypto/in_progress",# cloud prefix for in-progress data
    output_format="parquet",                  # "parquet" | "orc"
    compression="zstd",                       # compression codec
    delete_source_on_success=True,            # delete source after all partitions confirmed
)
```

| Attribute | Type | Default | Description |
|---|---|---|---|
| `location_archive` | `str` | `"archive"` | Cloud prefix for archived partitions |
| `location_in_progress` | `str` | `"in_progress"` | Cloud prefix for in-progress data |
| `output_format` | `"parquet"` \| `"orc"` | `"parquet"` | Output file format |
| `compression` | `str` | `"zstd"` | Compression codec — see [Compression guide](#compression-guide) |
| `delete_source_on_success` | `bool` | `True` | Delete source only after all partitions confirmed |

---

### ArchiveResult

Returned by `Archiver.archive()`. Inspect to verify the operation or handle errors.

```python
result = archiver.archive(source_key, symbol, timeframe)

if result.success:
    print(f"Archived {len(result.partitions_uploaded)} partitions")
    print(f"Took {result.duration_seconds:.1f}s")
    for key in result.partitions_uploaded:
        print(f"  → {key}")
else:
    print(f"Archive failed:")
    for err in result.errors:
        print(f"  {err}")
```

| Attribute | Type | Description |
|---|---|---|
| `source_key` | `str` | Remote key of the source file |
| `partitions_uploaded` | `list[str]` | Keys of all successfully uploaded partitions |
| `source_deleted` | `bool` | Whether the source was deleted after archiving |
| `duration_seconds` | `float` | Wall-clock time for the operation |
| `errors` | `list[str]` | Non-fatal errors collected during the run |
| `success` | `bool` (property) | `True` when partitions were uploaded and no errors occurred |

---

## Parquet vs ORC

Both are columnar formats designed for analytics. Choose based on your query engine:

| Factor | Parquet | ORC |
|---|---|---|
| Ecosystem | Widest — Spark, Athena, DuckDB, BigQuery, pandas | Hive, HBase, Spark |
| Compression ratio | Good | Slightly better in Hive workloads |
| `pandas.read_parquet()` | ✅ native | Needs `pyarrow.orc` |
| AWS Athena | ✅ | ✅ |
| DuckDB `read_parquet()` | ✅ | ❌ |
| Recommendation | Default choice | Only if Hive/HBase is your primary engine |

Switch format via `ArchiveConfig(output_format="orc")` — no other changes required.

---

## Compression guide

| Codec | Speed | Ratio | Use when |
|---|---|---|---|
| `zstd` | Medium | Best | **Cold archival** — data read infrequently (default) |
| `snappy` | Fastest | Good | **Warm data** — queried regularly, latency matters |
| `gzip` | Slow | Good | Maximum compatibility (e.g. tools without Snappy support) |

```python
# Cold archive (monthly historical data)
ArchiveConfig(compression="zstd")

# Warm data (last 7 days, queried daily)
ArchiveConfig(compression="snappy")
```

---

## Log archiving

Use `archive_logs()` to bundle `.log` files into `.zip` archives at the end of
a pipeline run or at the end of a training job.

```python
# At end of any pipeline
zip_key = archiver.archive_logs(
    log_files=[logger.local_log_file],
    remote_prefix="Logs/Archive",
)

# End of RL training — archive both pipeline and result logs
zip_key = archiver.archive_logs(
    log_files=[pipeline_logger.local_log_file, rl_logger.local_log_file],
    remote_prefix="Logs/Archive/RL",
)
```

The zip file is named `logs_{date}.zip` and stored at:
```
{remote_prefix}/logs_2024-03-15.zip
```

---

## Full pipeline example

End-to-end: ingest → log → archive data → archive logs.

```python
from cloud_client import CloudClientFactory
from log_framework import DataLogger, EndOfPipelineFlush
from archive_manager import Archiver, ArchiveConfig

# --- Setup ---
client = CloudClientFactory.s3("my-data-bucket")
logger = DataLogger(
    "CryptoIngestPipeline",
    flush_strategy=EndOfPipelineFlush(client),
)
archiver = Archiver(
    client,
    logger,
    config=ArchiveConfig(
        location_archive="crypto/archive",
        location_in_progress="crypto/in_progress",
        output_format="parquet",
        compression="zstd",
    ),
)

# --- Ingest ---
logger.info("ingest", "Fetching OHLCV data", extra={"symbol": "BTCUSDT"})
# ... fetch and upload in-progress file ...
client.upload("local/data_BTCUSDT_1h.parquet",
              "crypto/in_progress/BTC/data_BTCUSDT_1h.parquet")
logger.info("ingest", "In-progress file uploaded")

# --- Archive data ---
result = archiver.archive(
    source_key="crypto/in_progress/BTC/data_BTCUSDT_1h.parquet",
    symbol="BTCUSDT",
    timeframe="1h",
)

if result.success:
    logger.info(
        "archive",
        f"Archived {len(result.partitions_uploaded)} partitions",
        extra={"duration_s": round(result.duration_seconds, 2)},
    )
else:
    logger.error("archive", "Archive failed", extra={"errors": result.errors})

# --- Flush pipeline logs to S3 ---
logger.flush()

# --- Bundle and archive logs ---
archiver.archive_logs(
    log_files=[logger.local_log_file],
    remote_prefix="Logs/Archive",
)
```