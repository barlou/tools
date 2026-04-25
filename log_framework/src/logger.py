"""
logger.py
=========
DataLogger — the single entry point for the log framework.

Replaces both LogClass and ResultLogClass from the original codebase.
Key improvements over the original:
  - One class, no duplication
  - Local-first: never writes directly to S3 on each log call
  - Pluggable flush strategies (see flush.py)
  - Airflow context auto-detected from env vars — no manual wiring
  - Structured extra fields for RL training (episode, reward, symbol, timeframe)
  - Compatible with stdlib logging for console output (bridge pattern)

References
----------
- Airflow template vars: https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
- Python logging:        https://docs.python.org/3/library/logging.html
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, TYPE_CHECKING

from record import LogLevel, LogRecord, make_record

if TYPE_CHECKING:
    from flush import FlushStrategy


class DataLogger:
    """Structured, cloud-aware data engineering logger.

    Parameters
    ----------
    job_part : str
        Pipeline step / module name. Appears in every log line.
        Examples: ``"ArchiveWorker"``, ``"RL-Training"``, ``"Ingestor"``.
    flush_strategy : FlushStrategy, optional
        Controls when and how logs are uploaded to cloud.
        When None, flush() is a no-op (local-only mode).
    log_dir : str | Path
        Directory for local .log buffer files. Default: /tmp/data_logs/.
    log_subdir : str
        Sub-folder within log_dir.  Use ``"results"`` to keep RL training
        result logs separate from general pipeline logs.
    min_level : LogLevel
        Records below this level are discarded. Default: DEBUG (keep all).
    echo_to_console : bool
        When True (default), mirror records to stdout via stdlib logging.

    Examples
    --------
    # Airflow task
    >>> from log_framework import DataLogger
    >>> from log_framework import EndOfPipelineFlush
    >>> from cloud_client import CloudClientFactory
    >>>
    >>> client = CloudClientFactory.s3("my-bucket")
    >>> logger = DataLogger("IngestTask", flush_strategy=EndOfPipelineFlush(client))
    >>> logger.info("ingest", "Started", extra={"rows": 50_000})
    >>> logger.flush()   # called at task end / in Airflow callbacks

    # RL training
    >>> from log_framework import DataLogger, OncePerDayFlush, ShutdownFlush, CompositeFlush
    >>>
    >>> logger = DataLogger(
    ...     "RL-Training",
    ...     flush_strategy=CompositeFlush([
    ...         OncePerDayFlush(client),
    ...         ShutdownFlush(client),
    ...     ]),
    ...     log_subdir="results",
    ... )
    >>> logger.log_rl_result("train", episode=42, reward=0.87, symbol="BTCUSDT")
    >>> logger.flush()   # no-op until UTC day changes or SIGTERM fires
    """

    def __init__(
        self,
        job_part: str,
        flush_strategy: Optional["FlushStrategy"] = None,
        log_dir: str | Path = "/tmp/data_logs",
        log_subdir: str = "",
        min_level: LogLevel = LogLevel.DEBUG,
        echo_to_console: bool = True,
    ) -> None:
        self.job_part = job_part
        self._strategy = flush_strategy
        self._min_level = min_level

        # Resolve local log file path
        base_dir = Path(log_dir).expanduser()
        if log_subdir:
            base_dir = base_dir / log_subdir
        base_dir.mkdir(parents=True, exist_ok=True)

        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        safe_name = job_part.replace(" ", "_").replace("/", "-")
        self._local_file: Path = base_dir / f"{safe_name}_{date_str}.log"

        # Airflow context — injected automatically by Airflow as env vars
        # https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
        self._dag_id  = os.environ.get("AIRFLOW_CTX_DAG_ID")
        self._task_id = os.environ.get("AIRFLOW_CTX_TASK_ID")
        self._run_id  = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID")

        # stdlib logging bridge (console echo)
        self._std_logger = logging.getLogger(f"data_tools.{job_part}")
        if echo_to_console and not self._std_logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )
            self._std_logger.addHandler(handler)
            self._std_logger.setLevel(logging.DEBUG)
            self._std_logger.propagate = False

        # Register ShutdownFlush handlers immediately if present
        if flush_strategy is not None:
            self._register_shutdown(flush_strategy)

    # ------------------------------------------------------------------
    # Convenience log methods
    # ------------------------------------------------------------------

    def debug(self, method: str, message: str, **kwargs) -> None:
        """Log at DEBUG level."""
        self._log(LogLevel.DEBUG, method, message, **kwargs)

    def info(self, method: str, message: str, **kwargs) -> None:
        """Log at INFO level."""
        self._log(LogLevel.INFO, method, message, **kwargs)

    def warning(self, method: str, message: str, **kwargs) -> None:
        """Log at WARNING level."""
        self._log(LogLevel.WARNING, method, message, **kwargs)

    def error(
        self,
        method: str,
        message: str,
        *,
        exc: Optional[BaseException] = None,
        **kwargs,
    ) -> None:
        """Log at ERROR level with optional exception traceback.

        Parameters
        ----------
        exc : BaseException, optional
            When provided, the full traceback is appended to the log line.

        Example
        -------
        >>> try:
        ...     risky_operation()
        ... except Exception as e:
        ...     logger.error("process", "Failed", exc=e, extra={"file": "data.parquet"})
        """
        self._log(LogLevel.ERROR, method, message, exc=exc, **kwargs)

    def critical(self, method: str, message: str, **kwargs) -> None:
        """Log at CRITICAL level."""
        self._log(LogLevel.CRITICAL, method, message, **kwargs)

    # ------------------------------------------------------------------
    # RL-specific convenience method
    # ------------------------------------------------------------------

    def log_rl_result(
        self,
        method: str,
        episode: int,
        reward: float,
        *,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        extra: Optional[dict[str, Any]] = None,
    ) -> None:
        """Log a reinforcement learning training result.

        Replaces LoggingResult.log_message() from the original codebase.
        Structured fields make it easy to parse results later for plotting.

        Parameters
        ----------
        episode : int
            Training episode index.
        reward : float
            Episode reward (or loss, accuracy, or any scalar metric).
        symbol : str, optional
            Asset symbol, e.g. ``"BTCUSDT"``.
        timeframe : str, optional
            Candle timeframe, e.g. ``"1h"``.
        extra : dict, optional
            Any additional domain fields to include in the log line.

        Example
        -------
        >>> logger.log_rl_result(
        ...     "train", episode=42, reward=0.87,
        ...     symbol="BTCUSDT", timeframe="1h",
        ...     extra={"loss": 0.031, "epsilon": 0.12},
        ... )
        """
        combined: dict[str, Any] = {
            "episode": episode,
            "reward": round(reward, 6),
        }
        if symbol:
            combined["symbol"] = symbol
        if timeframe:
            combined["timeframe"] = timeframe
        if extra:
            combined.update(extra)

        self._log(
            LogLevel.INFO,
            method,
            f"Episode {episode} complete",
            extra=combined,
        )

    # ------------------------------------------------------------------
    # Cloud flush
    # ------------------------------------------------------------------

    def flush(self) -> bool:
        """Upload local log buffer to cloud according to the flush strategy.

        Returns
        -------
        bool
            True on success or when nothing needs flushing.
            False when upload failed — logged to stderr, never raises.
        """
        if self._strategy is None:
            return True

        remote_key = self._build_remote_key()
        try:
            return self._strategy.flush(self._local_file, remote_key)
        except Exception as exc:
            print(
                f"[DataLogger] Flush failed for {remote_key}: {exc}",
                file=sys.stderr,
            )
            return False

    @property
    def local_log_file(self) -> Path:
        """Path to the current local log buffer file."""
        return self._local_file

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _log(
        self,
        level: LogLevel,
        method: str,
        message: str,
        *,
        extra: Optional[dict[str, Any]] = None,
        exc: Optional[BaseException] = None,
    ) -> None:
        if level < self._min_level:
            return

        record = make_record(
            level=level,
            job_part=self.job_part,
            method=method,
            message=message,
            extra=extra,
            exc_info=exc,
            airflow_dag_id=self._dag_id,
            airflow_task_id=self._task_id,
            airflow_run_id=self._run_id,
        )

        # Write to local buffer file
        with self._local_file.open("a", encoding="utf-8") as fh:
            fh.write(record.to_line() + "\n")

        # Mirror to console via stdlib logging
        self._std_logger.log(int(level), "[%s] %s", method, message)
        if record.traceback_str:
            self._std_logger.debug(record.traceback_str)

    def _build_remote_key(self) -> str:
        """Build the S3 key for this logger's log file."""
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        safe_name = self.job_part.replace(" ", "_").replace("/", "-")

        if self._dag_id and self._task_id:
            return f"Logs/{self._dag_id}/{self._task_id}/{safe_name}_{date_str}.log"
        return f"Logs/{safe_name}_{date_str}.log"

    def _register_shutdown(self, strategy) -> None:
        """If strategy is or contains ShutdownFlush, register it now."""
        from flush import ShutdownFlush, CompositeFlush

        if isinstance(strategy, ShutdownFlush):
            strategy.register(self._local_file, self._build_remote_key())
        elif isinstance(strategy, CompositeFlush):
            for s in strategy._strategies:
                if isinstance(s, ShutdownFlush):
                    s.register(self._local_file, self._build_remote_key())