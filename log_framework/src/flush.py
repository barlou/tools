"""
flush.py
========
Cloud flush strategies for the log framework.

The flush problem
-----------------
Writing every log line directly to S3 is expensive:
  - S3 PUT requests cost money and are rate-limited (3,500 req/s per prefix)
  - S3 has no native append — every write replaces the full object
  - Concurrent Airflow workers writing the same key cause race conditions

Solution: buffer locally, flush to cloud on a schedule.

Strategies
----------
EndOfPipelineFlush
    Upload once when flush() is explicitly called.
    Use in Airflow tasks: call at task end or in on_success/on_failure callbacks.
    One S3 PUT per task execution.

OncePerDayFlush
    Upload at most once per UTC calendar day.
    Use in long-running RL training loops: call flush() every episode;
    only one real upload happens per day regardless of episode count.
    State is tracked by a sentinel file (*.flushed) next to the log file.

ShutdownFlush
    Registers SIGTERM + atexit handlers at instantiation.
    Upload fires automatically when the process exits or is killed.
    Use on spot/preemptible VMs where the machine may be killed at any time.

CompositeFlush
    Combine any of the above. All strategies run in order on flush().

Shared append logic
-------------------
All strategies share the same upload logic:
  1. Read local buffer file.
  2. Download existing remote file if present.
  3. Write combined content back to the remote key.
  4. Clean up temp file.

This ensures logs from multiple runs accumulate in the same S3 file
without overwriting each other.

References
----------
- Python signal: https://docs.python.org/3/library/signal.html
- Python atexit:  https://docs.python.org/3/library/atexit.html
- S3 pricing:     https://aws.amazon.com/s3/pricing/
"""

from __future__ import annotations

import atexit
import os
import signal
import tempfile
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from cloud_client import CloudClientBase


class FlushStrategy(ABC):
    """Abstract base for all cloud flush strategies."""

    def __init__(
        self,
        client: "CloudClientBase",
        remote_prefix: str = "Logs",
    ) -> None:
        self._client = client
        self._remote_prefix = remote_prefix.rstrip("/")

    @abstractmethod
    def flush(self, local_log_file: Path, remote_key: str) -> bool:
        """Upload local_log_file to remote_key, appending to existing content.

        Returns True on success or when there is nothing to flush.
        """

    def _append_upload(self, local_log_file: Path, remote_key: str) -> bool:
        """Core append logic shared by all strategies.

        Steps
        -----
        1. Read local buffer — skip if empty.
        2. Download existing remote content (best-effort).
        3. Combine and re-upload.
        4. Remove temp file.
        """
        if not local_log_file.exists() or local_log_file.stat().st_size == 0:
            return True  # nothing to flush

        local_content = local_log_file.read_text(encoding="utf-8")

        # Download existing remote content into a temp file
        existing_content = ""
        tmp_fd, tmp_name = tempfile.mkstemp(suffix=".log")
        os.close(tmp_fd)
        tmp_path = Path(tmp_name)

        try:
            if self._client.exists(remote_key):
                self._client.download(remote_key, tmp_path)
                existing_content = tmp_path.read_text(encoding="utf-8")
        except Exception:
            pass  # first upload or non-critical read failure — continue

        combined = existing_content + local_content
        tmp_path.write_text(combined, encoding="utf-8")

        try:
            success = self._client.upload(tmp_path, remote_key)
        finally:
            tmp_path.unlink(missing_ok=True)

        return success


# ---------------------------------------------------------------------------
# Strategy 1 — End of pipeline (explicit call)
# ---------------------------------------------------------------------------

class EndOfPipelineFlush(FlushStrategy):
    """Flush when flush() is explicitly called.

    Designed for Airflow tasks. Call logger.flush() at the end of your task,
    or wire it into on_success_callback / on_failure_callback.

    Example
    -------
    >>> strategy = EndOfPipelineFlush(client)
    >>> logger = DataLogger("MyTask", flush_strategy=strategy)
    >>> # ... work ...
    >>> logger.flush()   # one S3 PUT
    """

    def flush(self, local_log_file: Path, remote_key: str) -> bool:
        print(f"[EndOfPipelineFlush] → {remote_key}")
        return self._append_upload(local_log_file, remote_key)


# ---------------------------------------------------------------------------
# Strategy 2 — Once per UTC day (RL training mode)
# ---------------------------------------------------------------------------

class OncePerDayFlush(FlushStrategy):
    """Upload at most once per UTC calendar day.

    A sentinel file ({log_file}.flushed) records the last flush date.
    Calls to flush() before that date changes are no-ops, making this
    safe to call every training episode without hammering S3.

    Example
    -------
    >>> strategy = OncePerDayFlush(client)
    >>> logger = DataLogger("RL-Training", flush_strategy=strategy)
    >>> for episode in range(100_000):
    ...     logger.log_rl_result("train", episode, reward)
    ...     logger.flush()   # only uploads once per UTC day
    """

    def flush(self, local_log_file: Path, remote_key: str) -> bool:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        sentinel = local_log_file.with_suffix(".flushed")

        if sentinel.exists() and sentinel.read_text(encoding="utf-8").strip() == today:
            return True  # already flushed today

        print(f"[OncePerDayFlush] Daily flush → {remote_key}")
        success = self._append_upload(local_log_file, remote_key)
        if success:
            sentinel.write_text(today, encoding="utf-8")
        return success


# ---------------------------------------------------------------------------
# Strategy 3 — On shutdown (SIGTERM / atexit)
# ---------------------------------------------------------------------------

class ShutdownFlush(FlushStrategy):
    """Flush on SIGTERM, SIGINT, or normal process exit.

    Registers handlers at construction time.  Safe to use on spot VMs
    and preemptible instances where the machine may be killed mid-run.

    Example
    -------
    >>> strategy = ShutdownFlush(client)
    >>> logger = DataLogger("RL-Training", flush_strategy=strategy)
    >>> # Automatically flushes on SIGTERM / process exit — no extra code needed
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._pairs: list[tuple[Path, str]] = []

    def register(self, local_log_file: Path, remote_key: str) -> None:
        """Register a (local_file, remote_key) pair for auto-flush on shutdown.

        Called automatically by DataLogger when ShutdownFlush is the active strategy.
        """
        pair = (local_log_file, remote_key)
        if pair not in self._pairs:
            self._pairs.append(pair)

        atexit.register(self._on_exit)

        try:
            signal.signal(signal.SIGTERM, self._on_signal)
        except (OSError, ValueError):
            pass  # not main thread or restricted environment

    def flush(self, local_log_file: Path, remote_key: str) -> bool:
        """Explicit flush — also works as a normal immediate upload."""
        print(f"[ShutdownFlush] Explicit flush → {remote_key}")
        return self._append_upload(local_log_file, remote_key)

    def _on_exit(self) -> None:
        for local_path, remote_key in self._pairs:
            try:
                print(f"[ShutdownFlush] Process exit — flushing → {remote_key}")
                self._append_upload(local_path, remote_key)
            except Exception as exc:
                print(f"[ShutdownFlush] Flush failed for {remote_key}: {exc}")

    def _on_signal(self, signum: int, frame) -> None:
        self._on_exit()
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        os.kill(os.getpid(), signal.SIGTERM)


# ---------------------------------------------------------------------------
# Composite — combine any strategies
# ---------------------------------------------------------------------------

class CompositeFlush(FlushStrategy):
    """Apply multiple flush strategies in sequence.

    Example
    -------
    >>> strategy = CompositeFlush([
    ...     OncePerDayFlush(client),
    ...     ShutdownFlush(client),
    ... ])
    """

    def __init__(self, strategies: list[FlushStrategy]) -> None:
        self._strategies = strategies

    def flush(self, local_log_file: Path, remote_key: str) -> bool:
        results = []
        for strategy in self._strategies:
            try:
                results.append(strategy.flush(local_log_file, remote_key))
            except Exception as exc:
                print(
                    f"[CompositeFlush] {strategy.__class__.__name__} failed: {exc}"
                )
                results.append(False)
        return all(results)