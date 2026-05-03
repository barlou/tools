# log_framework/src/__init__.py
from .logger import DataLogger
from .flush import (
    FlushStrategy,
    EndOfPipelineFlush,
    OncePerDayFlush,
    ShutdownFlush,
    CompositeFlush,
)
from .record import LogLevel, LogRecord

__all__ = [
    "DataLogger",
    "FlushStrategy",
    "EndOfPipelineFlush",
    "OncePerDayFlush",
    "ShutdownFlush",
    "CompositeFlush",
    "LogLevel",
    "LogRecord"
]