# Log Framework

Structured, cloud-aware logging framework for data engineering pipelines.

Writes log records locally on every call and uploads them to cloud storage on a configurable schedule - never one s3 PUT per log line 

---

## Table of contents

- [Log Framework](#log-framework)
  - [Table of contents](#table-of-contents)
  - [Installation](#installation)
  - [Why not stdlib logging ?](#why-not-stdlib-logging-)
  - [Quick start](#quick-start)
  - [Flush strategies](#flush-strategies)
    - [EndOfPipelineFlush](#endofpipelineflush)
    - [OncePerDayFlush](#onceperdayflush)
    - [Shutdown flush](#shutdown-flush)
    - [CompositeFlush](#compositeflush)
  - [API reference](#api-reference)
    - [DataLogger](#datalogger)
      - [Constructor](#constructor)
      - [Logging methods](#logging-methods)
      - [`log_rl_result(method, episode, reward, *, symbol=None, timeframe=None, extra=None)`](#log_rl_resultmethod-episode-reward--symbolnone-timeframenone-extranone)
      - [`flush() → bool`](#flush--bool)
      - [`local_log_file → Path`](#local_log_file--path)
    - [LogLevel](#loglevel)
    - [LogRecord](#logrecord)
  - [Log line format](#log-line-format)
  - [Airflow integration](#airflow-integration)
    - [Recommended Airflow wiring](#recommended-airflow-wiring)
  - [RL training integration](#rl-training-integration)

--- 

## Installation 

```bash
# Install cloud_client first (log_framework depends on it)
pip install -e ../cloud_client/

# Then isntall log_framework
pip install -e log_framework/

# With dev/test dependencies (pytest, freezegun for date mocking)
pip install -e "log_framework/[dev]"
```

**Python requirement:** `>=3.10`

---

## Why not stdlib logging ?

Python's `logging` module is excellent for general-purpose use. This framework adds what it lacks for data engineering:

| Need | stdlib logging | log_framework |
|---|---|---|
| Structured fields (symbol, episode, reward) | ❌ | ✅ `extra={}` |
| Airflow DAG/task/run correlation | ❌ | ✅ auto from env vars |
| Cloud upload on a schedule (not per line) | ❌ | ✅ flush strategies |
| RL training results logs | ❌ | ✅ `log_rl_result()` |
| Console echo via stdlib | - | ✅ bridge to `logging` | 

`DataLogger` mirrors every record to stdout via a stdlib `logging.Logger` so existing log collectors (Datadog, CloudWatch) keep working unchanged 

---

## Quick start 

```python 
from cloud_client import CloudClientFactory 
from log_framework import DataLogger, EndOfPipelineFlush

client = CloudClientFactory.s3("my-data-bucket")

logger = DataLogger(
    "IngestTask",
    flush_strategy=EndOfPipelineFlush(client),
)

logger.info("ingest", "Started ingestion", extra={"source": "exchange", "symbole": "symbol"})
logger.warning("ingest", "Missing candle", extra={"ts" : "2024-01-15 08:00"})

try:
    risky_operation()
except Exception as exc:
    logger.error("ingest", "Row processing failed", exc=exc, extra={"row": 412})
logger.flush() # upload to s3
```

---

## Flush strategies 

S3 has no native append - every write replaces the full object. Writing on every log call would cost thousands of PUT requests per day and cause race conditions in concurrent Airflow workers. The solutin is to **buffer locally and flush on a schedule**.

### EndOfPipelineFlush

Upload once when `flush()` is explicitly called. One S3 PUT per task execution.

**Use for:** Airflow tasks, batch jobs, any pipeline with a clear end point.

```python
from log_framework import DataLogger, EndOfPipelineFlush
from cloud_client import CloudClientFactory 

client = CloudClientFactory.s3("my-bucket")
logger = DataLogger("MyTask", flush_strategy=EndOfPipelineFlush(client))

logger.info("process", "work started")

# ... do work ...
logger.flush()
```

### OncePerDayFlush

Upload at most once per UTC calendar day. Subsequent `flush()` calls on the same day are silent no-ops. State is tracked by a `.flushed` sentinel file next to the log file.

**Uses for:** Long-running RL training loops - call `flush()` every episode with zero cost, get a single upload per day 

```python
from log_framework import DataLogger, OncePerDayFlush

logger = DataLogger(
    "RL-training", 
    flush_strategy=OncePerDayFlush(client),
    log_subdir="results",
)

for episode in range(100_000):
    reward = env.step(action)
    logger.log_rl_result("train", episode=episode, reward=reward)
    logger.flush()
```

### Shutdown flush 

Registers `SIGTERM` and `atexit` handlers automatically at construction. 
Upload fires when the process exists or is killed - no extra code needed

**Use for:** Spot / preemptible VMs where the machine may be killed mid-run

```python
from log_framework import DataLogger, ShutDownFlush

logger = DataLogger("RL-training", flush_strategy = ShutDownFlush(client))
# handler registered - flush fires automaticallt on SIGTERM or process exit 
```

### CompositeFlush 

Chain multiple strategies. All are applied in order on every `flush()` call.
**Most common combinaison:** once-per-day for cost control + shutdown for safety 

```python
from log_framework import DataLogger, OncePerDayFlush, ShutdownFlush, CompositeFlush

logger = DataLogger(
    "Rl-Training",
    flush_strategy=CompositeFlush([
        OncePerDayFlush(client),
        ShutDownFlush(client),
    ]),
    log_subdirs="results",
)
```


---
 
## API reference
 
### DataLogger
 
```python
from log_framework import DataLogger
```
 
#### Constructor
 
```python
DataLogger(
    job_part,                          # str — pipeline step name
    flush_strategy=None,               # FlushStrategy | None
    log_dir="/tmp/data_logs",          # str | Path
    log_subdir="",                     # str — subfolder within log_dir
    min_level=LogLevel.DEBUG,          # minimum level to record
    echo_to_console=True,              # mirror to stdout via stdlib logging
)
```
 
| Parameter | Description |
|---|---|
| `job_part` | Module / step name. Appears in every log line. e.g. `"ArchiveWorker"`, `"RL-Training"` |
| `flush_strategy` | Controls cloud upload schedule. `None` = local only, never uploads |
| `log_dir` | Root directory for local `.log` buffer files. Default: `/tmp/data_logs/` |
| `log_subdir` | Sub-folder. Use `"results"` to keep RL result logs separate from pipeline logs |
| `min_level` | Records below this severity are discarded |
| `echo_to_console` | When `True`, also print to stdout via stdlib `logging` |
 
#### Logging methods
 
```python
logger.debug("method_name",   "message", extra={"key": "value"})
logger.info("method_name",    "message", extra={"rows": 50_000})
logger.warning("method_name", "message", extra={"missing": 3})
logger.error("method_name",   "message", exc=some_exception, extra={"file": "x.parquet"})
logger.critical("method_name","message")
```
 
All methods share the same signature:
 
| Parameter | Type | Description |
|---|---|---|
| `method` | `str` | Function or sub-operation name. e.g. `"archive"`, `"train_step"` |
| `message` | `str` | Human-readable event description |
| `extra` | `dict` | Arbitrary structured fields appended to the log line |
| `exc` | `BaseException` | *(error only)* Exception whose traceback is captured automatically |
 
#### `log_rl_result(method, episode, reward, *, symbol=None, timeframe=None, extra=None)`
 
Convenience method for reinforcement learning training logs.
 
```python
logger.log_rl_result(
    "train",
    episode=42,
    reward=0.871,
    symbol="BTCUSDT",
    timeframe="1h",
    extra={"loss": 0.031, "epsilon": 0.12, "steps": 1024},
)
```
 
Produces:
```
[2024-03-15 14:22:01 UTC] [INFO    ] [RL-Training] [train] -- Episode 42 complete {episode=42, reward=0.871, symbol=BTCUSDT, timeframe=1h, loss=0.031, epsilon=0.12, steps=1024}
```
 
#### `flush() → bool`
 
Upload local log buffer to cloud according to the active flush strategy.
 
```python
success = logger.flush()
```
 
Returns `True` on success or when there is nothing to upload. Returns `False`
and prints to stderr when the upload fails — never raises.
 
#### `local_log_file → Path`
 
Path to the current local `.log` buffer file. Pass this to
`archive_manager.Archiver.archive_logs()` to bundle logs into a `.zip`.
 
```python
print(logger.local_log_file)
# /tmp/data_logs/IngestTask_2024-03-15.log
```
 
---
 
### LogLevel
 
```python
from log_framework import LogLevel
 
LogLevel.DEBUG    # 10
LogLevel.INFO     # 20
LogLevel.WARNING  # 30
LogLevel.ERROR    # 40
LogLevel.CRITICAL # 50
 
# Construct from string
level = LogLevel.from_str("warning")   # → LogLevel.WARNING
```
 
---
 
### LogRecord
 
Immutable frozen dataclass. Created internally by `DataLogger._log()`.
Useful when building custom sinks (JSON lines, databases, dashboards).
 
```python
from log_framework import make_record, LogLevel
 
record = make_record(
    level="info",
    job_part="MyTask",
    method="process",
    message="Row processed",
    extra={"symbol": "BTCUSDT", "rows": 100},
    exc_info=None,
)
 
print(record.to_line())   # human-readable string
print(record.to_dict())   # dict — JSON-serialisable
```
 
---
 
## Log line format
 
```
[YYYY-MM-DD HH:MM:SS UTC] [LEVEL   ] [job_part] [method] [dag=...] [task=...] -- message {key=value, ...}
```
 
Examples:
 
```
[2024-03-15 14:22:01 UTC] [INFO    ] [IngestTask] [ingest] -- Started ingestion {source=binance, symbol=BTCUSDT}
[2024-03-15 14:22:03 UTC] [WARNING ] [IngestTask] [ingest] -- Missing candle {ts=2024-01-15 08:00}
[2024-03-15 14:22:05 UTC] [ERROR   ] [IngestTask] [ingest] [dag=crypto_pipeline] [task=ingest_btc] [run=scheduled_2024-03-15] -- Row processing failed {row=412}
Traceback (most recent call last):
  File "...", line 42, in risky_operation
    ...
[2024-03-15 14:22:08 UTC] [INFO    ] [RL-Training] [train] -- Episode 42 complete {episode=42, reward=0.871, symbol=BTCUSDT}
```
 
Airflow context fields (`dag=`, `task=`, `run=`) are included automatically when
the logger runs inside an Airflow task — omitted otherwise.
 
---
 
## Airflow integration
 
`DataLogger` reads Airflow's standard context env vars automatically:
 
| Env var | Field in log line |
|---|---|
| `AIRFLOW_CTX_DAG_ID` | `[dag=...]` |
| `AIRFLOW_CTX_TASK_ID` | `[task=...]` |
| `AIRFLOW_CTX_DAG_RUN_ID` | `[run=...]` |
 
Airflow injects these automatically for every task — no manual wiring needed.
 
### Recommended Airflow wiring
 
```python
from cloud_client import CloudClientFactory
from log_framework import DataLogger, EndOfPipelineFlush
 
def make_logger(context) -> DataLogger:
    client = CloudClientFactory.s3("my-bucket")
    return DataLogger(
        context["task_instance"].task_id,
        flush_strategy=EndOfPipelineFlush(client),
    )
 
def on_success(context):
    logger = make_logger(context)
    logger.info("callback", "Task succeeded")
    logger.flush()
 
def on_failure(context):
    logger = make_logger(context)
    logger.error("callback", "Task failed", exc=context.get("exception"))
    logger.flush()
 
# Attach to any operator
PythonOperator(
    task_id="ingest_btc",
    python_callable=my_ingest_fn,
    on_success_callback=on_success,
    on_failure_callback=on_failure,
)
```
 
Remote key written to S3 when Airflow context is present:
```
Logs/{dag_id}/{task_id}/{job_part}_{date}.log
```
 
Without Airflow context:
```
Logs/{job_part}_{date}.log
```
 
---
 
## RL training integration
 
```python
from cloud_client import CloudClientFactory
from log_framework import DataLogger, OncePerDayFlush, ShutdownFlush, CompositeFlush
 
client = CloudClientFactory.s3("my-bucket")
 
logger = DataLogger(
    "RL-BTCUSDT-1h",
    flush_strategy=CompositeFlush([
        OncePerDayFlush(client),   # one S3 PUT per day
        ShutdownFlush(client),     # also flush on SIGTERM (spot VM safety)
    ]),
    log_subdir="results",          # → /tmp/data_logs/results/
)
 
for episode in range(100_000):
    state = env.reset()
    total_reward = 0.0
 
    while True:
        action = agent.act(state)
        state, reward, done, info = env.step(action)
        total_reward += reward
        if done:
            break
 
    logger.log_rl_result(
        "train",
        episode=episode,
        reward=total_reward,
        symbol="BTCUSDT",
        timeframe="1h",
        extra={"epsilon": agent.epsilon, "loss": agent.last_loss},
    )
 
    logger.flush()  # no-op most of the time — fires once per UTC day
 
# Log file available at logger.local_log_file for archiving
```
 