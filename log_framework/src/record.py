"""
record.py
=========
Immutable log record and log level enum.

Why a custom record instead of stdlib logging.LogRecord?
---------------------------------------------------------
Python's logging.LogRecord is excellent for general-purpose use but does
not support:
  - Structured domain fields (symbol, timeframe, RL episode, reward)
  - Airflow correlation IDs (dag_id, task_id, run_id) as first-class fields
  - Cloud-flush strategies tied to the record lifecycle
  - A standard serialisation format for later parsing / dashboarding

This module provides an immutable, frozen dataclass that is serialised to a
single log line or a dict (for JSON lines sinks in the future).

Log line format
---------------
[2024-03-15 14:22:01 UTC] [INFO    ] [ArchiveWorker] [archive] -- File archived. {symbol=BTCUSDT, year=2024, month=1}
[2024-03-15 14:22:02 UTC] [ERROR   ] [RL-Training] [train] [dag=crypto] [task=train_step] -- Episode failed. {episode=42}
<traceback here if exc_info was provided>

References
----------
- Python logging: https://docs.python.org/3/library/logging.html
- structlog why:  https://www.structlog.org/en/stable/why.html
"""

from __future__ import annotations

import traceback as tb_module
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import IntEnum
from typing import Any, Optional


class LogLevel(IntEnum):
    """Severity levels, mirroring stdlib logging for easy bridge integration."""

    DEBUG    = 10
    INFO     = 20
    WARNING  = 30
    ERROR    = 40
    CRITICAL = 50

    @classmethod
    def from_str(cls, value: str) -> "LogLevel":
        mapping = {
            "debug":    cls.DEBUG,
            "info":     cls.INFO,
            "warning":  cls.WARNING,
            "warn":     cls.WARNING,
            "error":    cls.ERROR,
            "critical": cls.CRITICAL,
        }
        try:
            return mapping[value.lower()]
        except KeyError:
            raise ValueError(
                f"Unknown log level: {value!r}. Valid values: {list(mapping)}"
            )


@dataclass(frozen=True)
class LogRecord:
    """A single structured log event.  Immutable after creation.

    Attributes
    ----------
    level : LogLevel
        Severity.
    job_part : str
        Pipeline step / module name, e.g. ``"ArchiveWorker"``, ``"RL-Training"``.
    method : str
        Function or sub-operation, e.g. ``"archive"``, ``"train_episode"``.
    message : str
        Human-readable event description.
    timestamp : datetime
        UTC timestamp. Auto-set to now(UTC) when not provided.
    airflow_dag_id : str, optional
        Airflow DAG identifier — auto-captured from AIRFLOW_CTX_DAG_ID env var.
    airflow_task_id : str, optional
        Airflow task identifier — auto-captured from AIRFLOW_CTX_TASK_ID env var.
    airflow_run_id : str, optional
        Airflow run identifier — auto-captured from AIRFLOW_CTX_DAG_RUN_ID env var.
    extra : dict
        Arbitrary domain fields: symbol, timeframe, episode, reward, rows, etc.
    traceback_str : str, optional
        Formatted exception traceback, captured from exc_info.
    """

    level: LogLevel
    job_part: str
    method: str
    message: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    airflow_dag_id:  Optional[str] = None
    airflow_task_id: Optional[str] = None
    airflow_run_id:  Optional[str] = None
    extra: dict[str, Any] = field(default_factory=dict)
    traceback_str: Optional[str] = None

    def to_line(self) -> str:
        """Render as a single human-readable, machine-parseable log line."""
        ts = self.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
        level_str = self.level.name.ljust(8)

        parts = [f"[{ts}]", f"[{level_str}]", f"[{self.job_part}]", f"[{self.method}]"]

        # Airflow context — omitted when not running inside Airflow
        if self.airflow_dag_id:
            parts.append(f"[dag={self.airflow_dag_id}]")
        if self.airflow_task_id:
            parts.append(f"[task={self.airflow_task_id}]")
        if self.airflow_run_id:
            parts.append(f"[run={self.airflow_run_id}]")

        parts.append(f"-- {self.message}")

        if self.extra:
            kv = ", ".join(f"{k}={v}" for k, v in self.extra.items())
            parts.append(f"{{{kv}}}")

        line = " ".join(parts)

        if self.traceback_str:
            line += f"\n{self.traceback_str.rstrip()}"

        return line

    def to_dict(self) -> dict[str, Any]:
        """Render as a dictionary — suitable for JSON lines or structured sinks."""
        return {
            "timestamp":       self.timestamp.isoformat(),
            "level":           self.level.name,
            "job_part":        self.job_part,
            "method":          self.method,
            "message":         self.message,
            "airflow_dag_id":  self.airflow_dag_id,
            "airflow_task_id": self.airflow_task_id,
            "airflow_run_id":  self.airflow_run_id,
            "extra":           self.extra,
            "traceback":       self.traceback_str,
        }


def make_record(
    level: str | LogLevel,
    job_part: str,
    method: str,
    message: str,
    *,
    extra: Optional[dict[str, Any]] = None,
    exc_info: Optional[BaseException] = None,
    airflow_dag_id:  Optional[str] = None,
    airflow_task_id: Optional[str] = None,
    airflow_run_id:  Optional[str] = None,
) -> LogRecord:
    """Convenience constructor for LogRecord.

    Parameters
    ----------
    exc_info : BaseException, optional
        When provided, the full exception traceback is captured automatically.
    """
    if isinstance(level, str):
        level = LogLevel.from_str(level)

    traceback_str: Optional[str] = None
    if exc_info is not None:
        traceback_str = "".join(
            tb_module.format_exception(type(exc_info), exc_info, exc_info.__traceback__)
        )

    return LogRecord(
        level=level,
        job_part=job_part,
        method=method,
        message=message,
        extra=extra or {},
        traceback_str=traceback_str,
        airflow_dag_id=airflow_dag_id,
        airflow_task_id=airflow_task_id,
        airflow_run_id=airflow_run_id,
    )