"""
archiver.py
===========
Partitioned archive manager for Parquet/ORC data files in cloud storage.

What this solves
----------------
The original ArchiveClass.py archived data using boto3 directly, was tightly
coupled to LogClass, and deleted the source file before confirming all
partitions were uploaded.  This rewrite:

1. Uses CloudClientBase — any provider, not just S3.
2. Produces Hive-partitioned output: year=YYYY/month=MM/ layout.
3. Deletes source only after ALL partitions are confirmed uploaded.
4. Handles both numeric and named OHLCV column schemas (Binance API quirk).
5. Bundles .log and result files into .zip archives for long-term storage.
6. Uses DataLogger for structured logging of all archive operations.

Parquet vs ORC
--------------
Both are columnar formats suited to analytics workloads:
  - Parquet: widest ecosystem (pandas, Spark, Athena, DuckDB, BigQuery)
  - ORC:     better Hive/HBase support, slightly better compression in some cases

This archiver supports both transparently via ArchiveConfig.output_format.
Compression defaults to "zstd" — best ratio for cold archival storage.
Use "snappy" for warm data requiring faster reads.

Hive partitioning
-----------------
Output key format:
    {location_archive}/{symbol}/year={year}/month={month:02d}/data_{symbol}_{tf}_{year}{month:02d}.parquet

This layout enables partition pruning in Spark, Athena, and DuckDB:
    WHERE year=2024 AND month=01  →  reads only that folder, skips the rest.

References
----------
- PyArrow Parquet:      https://arrow.apache.org/docs/python/parquet.html
- PyArrow ORC:          https://arrow.apache.org/docs/python/orc.html
- Hive partitioning:    https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL
- Athena partitions:    https://docs.aws.amazon.com/athena/latest/ug/partitions.html
- Python zipfile:       https://docs.python.org/3/library/zipfile.html
"""

from __future__ import annotations

import io
import time
import tempfile
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Optional, TYPE_CHECKING

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from .cloud_client import CloudClientBase
    from .log_framework import DataLogger


# ---------------------------------------------------------------------------
# OHLCV column schema (Binance candle API returns numeric indices)
# ---------------------------------------------------------------------------

_OHLCV_COLUMNS: dict[int, str] = {
    0:  "open_time",
    1:  "open",
    2:  "high",
    3:  "low",
    4:  "close",
    5:  "volume",
    6:  "close_time",
    7:  "quote_asset_volume",
    8:  "number_of_trades",
    9:  "taker_buy_base_asset_volume",
    10: "taker_buy_quote_asset_volume",
    11: "ignore",
}


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class ArchiveConfig:
    """Configuration for archive operations.

    Attributes
    ----------
    location_archive : str
        Cloud prefix for archived partitions.
        Example: ``"crypto/archive"``
    location_in_progress : str
        Cloud prefix for live / in-progress data.
        Example: ``"crypto/in_progress"``
    output_format : "parquet" | "orc"
        Serialisation format for partitioned output files.
    compression : str
        Compression codec. ``"zstd"`` for cold archival (best ratio).
        ``"snappy"`` for warm data needing faster reads.
        ``"gzip"`` for widest compatibility.
    delete_source_on_success : bool
        When True (default), the in-progress source file is deleted only
        after every partition is confirmed uploaded — never before.
    """

    location_archive:       str = "archive"
    location_in_progress:   str = "in_progress"
    output_format:          Literal["parquet", "orc"] = "parquet"
    compression:            str = "zstd"
    delete_source_on_success: bool = True


# ---------------------------------------------------------------------------
# Result value object
# ---------------------------------------------------------------------------

@dataclass
class ArchiveResult:
    """Summary returned by Archiver.archive().

    Attributes
    ----------
    source_key : str
        Cloud key of the source file that was archived.
    partitions_uploaded : list[str]
        Remote keys of every successfully uploaded partition file.
    source_deleted : bool
        Whether the source file was deleted after archiving.
    duration_seconds : float
        Wall-clock time for the complete operation.
    errors : list[str]
        Non-fatal error messages collected during the run.
    """

    source_key:           str
    partitions_uploaded:  list[str] = field(default_factory=list)
    source_deleted:       bool = False
    duration_seconds:     float = 0.0
    errors:               list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.partitions_uploaded) > 0 and not self.errors


# ---------------------------------------------------------------------------
# Archiver
# ---------------------------------------------------------------------------

class Archiver:
    """Archive partitioned data files from in-progress to long-term storage.

    Parameters
    ----------
    client : CloudClientBase
        Cloud storage client (S3, OVH, Azure, GCP...).
    logger : DataLogger
        Structured logger for recording archive events.
    config : ArchiveConfig, optional
        Archive behaviour. Defaults to parquet + zstd.

    Examples
    --------
    >>> from cloud_client import CloudClientFactory
    >>> from log_framework import DataLogger
    >>> from archive_manager import Archiver, ArchiveConfig
    >>>
    >>> client = CloudClientFactory.s3("my-bucket")
    >>> logger = DataLogger("Archiver")
    >>> archiver = Archiver(client, logger, ArchiveConfig(
    ...     location_archive="crypto/archive",
    ...     output_format="parquet",
    ...     compression="zstd",
    ... ))
    >>>
    >>> result = archiver.archive(
    ...     source_key="crypto/in_progress/BTC/data_BTCUSDT_1h.parquet",
    ...     symbol="BTCUSDT",
    ...     timeframe="1h",
    ... )
    >>> print(result.partitions_uploaded)
    ['crypto/archive/BTC/year=2024/month=01/data_BTCUSDT_1h_202401.parquet', ...]
    """

    def __init__(
        self,
        client: "CloudClientBase",
        logger: "DataLogger",
        config: Optional[ArchiveConfig] = None,
    ) -> None:
        self._client = client
        self._logger = logger
        self._config = config or ArchiveConfig()

    def archive(
        self,
        source_key: str,
        symbol: str,
        timeframe: str,
    ) -> ArchiveResult:
        """Download, partition by year/month, and re-upload a data file.

        Parameters
        ----------
        source_key : str
            Full remote key of the file to archive, e.g.
            ``"crypto/in_progress/BTC/data_BTCUSDT_1h.parquet"``.
        symbol : str
            Asset symbol, e.g. ``"BTCUSDT"``.
        timeframe : str
            Candle timeframe, e.g. ``"1h"``, ``"4h"``, ``"1d"``.

        Returns
        -------
        ArchiveResult
        """
        start = time.perf_counter()
        result = ArchiveResult(source_key=source_key)
        symbol_base = symbol.replace("USDT", "").replace("BUSD", "")

        self._logger.info(
            "archive",
            f"Archiving {source_key}",
            extra={"symbol": symbol, "timeframe": timeframe},
        )

        try:
            # 1. Download source file into a temp local buffer
            df = self._download_as_dataframe(source_key)

            # 2. Normalise column schema (Binance numeric → named)
            df = self._normalise_schema(df)

            # 3. Parse timestamps (Binance returns milliseconds since epoch)
            df["open_time"] = pd.to_datetime(
                df["open_time"] / 1000, unit="s", origin="unix", utc=True
            )

            # 4. Partition by year and month — upload each partition
            grouped = df.groupby(
                [df["open_time"].dt.year, df["open_time"].dt.month]
            )
            for (year, month), group in grouped:
                partition_key = self._partition_key(
                    symbol_base, symbol, timeframe, int(year), int(month)
                )
                buffer = self._serialise(group.copy())
                self._client.upload_bytes(
                    buffer,
                    partition_key,
                    content_type=self._content_type(),
                )
                result.partitions_uploaded.append(partition_key)
                self._logger.info(
                    "archive",
                    f"Partition uploaded",
                    extra={
                        "key":   partition_key,
                        "year":  year,
                        "month": month,
                        "rows":  len(group),
                    },
                )

            # 5. Delete source only after all partitions are confirmed
            if self._config.delete_source_on_success and result.partitions_uploaded:
                self._client.delete(source_key)
                result.source_deleted = True
                self._logger.info("archive", f"Source deleted: {source_key}")

        except Exception as exc:
            msg = f"Archive failed for {source_key}: {exc}"
            result.errors.append(msg)
            self._logger.error("archive", msg, exc=exc)

        finally:
            result.duration_seconds = time.perf_counter() - start
            self._logger.info(
                "archive",
                "Archive complete",
                extra={
                    "partitions":  len(result.partitions_uploaded),
                    "deleted":     result.source_deleted,
                    "success":     result.success,
                    "duration_s":  round(result.duration_seconds, 2),
                },
            )

        return result

    def archive_logs(
        self,
        log_files: list[Path],
        remote_prefix: str = "Logs/Archive",
    ) -> str:
        """Bundle local log files into a single .zip and upload.

        Parameters
        ----------
        log_files : list[Path]
            Local .log files to bundle.
        remote_prefix : str
            Cloud prefix for the zip file.

        Returns
        -------
        str
            Remote key of the uploaded zip archive.

        Example
        -------
        >>> zip_key = archiver.archive_logs(
        ...     [logger.local_log_file],
        ...     remote_prefix="Logs/Archive",
        ... )
        """
        from datetime import datetime, timezone

        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        zip_key = f"{remote_prefix}/logs_{date_str}.zip"

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for log_path in log_files:
                if log_path.exists():
                    zf.write(log_path, arcname=log_path.name)

        self._client.upload_bytes(
            buf.getvalue(), zip_key, content_type="application/zip"
        )
        self._logger.info(
            "archive_logs",
            "Log bundle uploaded",
            extra={
                "key":   zip_key,
                "files": [f.name for f in log_files if f.exists()],
            },
        )
        return zip_key

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _download_as_dataframe(self, source_key: str) -> pd.DataFrame:
        ext = self._config.output_format
        with tempfile.NamedTemporaryFile(suffix=f".{ext}", delete=False) as tmp:
            tmp_path = Path(tmp.name)

        try:
            self._client.download(source_key, tmp_path)
            if ext == "parquet":
                return pd.read_parquet(tmp_path)
            else:
                import pyarrow.orc as orc
                return orc.read_table(str(tmp_path)).to_pandas()
        finally:
            tmp_path.unlink(missing_ok=True)

    @staticmethod
    def _normalise_schema(df: pd.DataFrame) -> pd.DataFrame:
        """Rename numeric column indices to OHLCV column names if needed."""
        if len(df.columns) > 0 and isinstance(df.columns[0], int):
            return df.rename(columns=_OHLCV_COLUMNS)
        return df

    def _partition_key(
        self,
        symbol_base: str,
        symbol: str,
        timeframe: str,
        year: int,
        month: int,
    ) -> str:
        ext = self._config.output_format
        filename = f"data_{symbol}_{timeframe}_{year}{month:02d}.{ext}"
        return (
            f"{self._config.location_archive}/{symbol_base}"
            f"/year={year}/month={month:02d}/{filename}"
        )

    def _serialise(self, df: pd.DataFrame) -> bytes:
        """Serialise a DataFrame to bytes in the configured format."""
        buf = io.BytesIO()
        if self._config.output_format == "parquet":
            table = pa.Table.from_pandas(df)
            pq.write_table(
                table,
                buf,
                compression=self._config.compression,
                write_statistics=True,  # enables predicate pushdown in Athena/Spark
            )
        else:
            import pyarrow.orc as orc
            table = pa.Table.from_pandas(df)
            orc.write_table(table, buf)
        return buf.getvalue()

    def _content_type(self) -> str:
        return (
            "application/vnd.apache.parquet"
            if self._config.output_format == "parquet"
            else "application/vnd.apache.orc"
        )