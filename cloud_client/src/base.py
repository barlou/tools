"""
base.py 
=======
Abstract base class for all cloud storage providers 

Design
------
Every provider (AWS, Azure, GCP, OVH) inherits from CloudClientBase ad 
implements five core operations: upload, download, delete, exists, list

The base class handles:
    - Exponential backoff with jitter on retryable errors
    - A consistent public API regardless of provider
    - Domain exceptions that callers catch without knowing provider internals 
    
Pattern used: Strategy - swap providers without changing caller code.
"""

from __future__ import annotations

import random, time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

# ---------------------------------------------------------------------------
# Retry configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RetryConfig:
    """Immutable retry policy for cloud operations.
    
    Attributes 
    ----------
    max_attempts: int 
        Total number of attempts (1 = no retry)
    base_delay: float
        Initial wait in seconds before the first try 
    max_delay: float 
        Upper cap on per-attempt wait 
    jitter_factor: float 
        Fraction of delay added as random noise (0.0-1.0)
        Spreading retries across workers avoids thundering-herd bursts
    
    Example:
    >>> cfg = RetryConfig(max_attempts=3, base_delay=0.5)
    >>> cfg.compute_delay(0)
    >>> cfg.compute_delay(1)
    """
    
    max_attempts: int = 5
    base_delay: float = 1.0
    max_delay: float = 32.0
    jitter_factor: float = 0.25
    
    def compute_delay(self, attempt: int) -> float:
        """Return wait time (seconds) for a given attempt index (0-based)"""
        raw = min(self.base_delay * (2 ** attempt), self.max_delay)
        jitter = random.uniform(0, self.jitter_factor * raw)
        return raw + jitter
    
# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class CloudClientBase(ABC):
    """Abstract cloud storage client 
    
    Subclasses implement:
        _upload_impl, _download_impl, _delete_impl, _exists_impl, _list_impl
        
    Public methods (upload, download, delete, exists, list) wrap those implementations
    with the retry engine defined in this class
    
    Parameters
    ----------
    provider_name: str
        Human-readable lable used in log/error messages
    retry_config: RetryConfig, optional
        Custom retry policy. Defaults to library defaults 
    """
    
    # Tuple of exception types that are safe to retry 
    # Each provider overrides this with its own transient error types
    RETRYABLE_ERRORS: tuple[type[Exception], ...] = ()
    
    def __init__(
        self,
        provider_name: str,
        retry_config: Optional[RetryConfig] = None,
    ) -> None:
        self.provider_name = provider_name
        self.retry = retry_config or RetryConfig()
        
    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    
    def upload(
        self, 
        local_path: str | Path,
        remote_key: str,
        *,
        content_type: Optional[str] = None,
        metadata: Optional[dict[str, str]] = None,
    ) -> bool:
        """Upload a local file to cloud storage
        
        Parameters
        ----------
        local_path: str | Path
            Absolute or relative path to the source file
        remote_key: str
            Destination object key / blob name in the bucket 
        content_type: str, optional
            MIME type. Inferred from file extension when omitted 
        metadata: dict[str, str], optional
            Provider-stored key-value pairs (e.g, pipeline run ID)
            
        Returns
        -------
        bool
            True on success
        
        Raises
        ------
        FileNotFoundError
            When local_path doesn't exist on disk
        CloudUploadError
            When all retry attempts are exhausted 
        """
        local_path = Path(local_path)
        if not local_path.exists():
            raise FileNotFoundError(f"Upload source not found: {local_path}")
        return self._with_retry(
            "upload",
            self._upload_impl,
            local_path,
            remote_key,
            content_type=content_type,
            metadata=metadata or {},
        )
        
    def download(self, remote_key: str, local_path: str | Path) -> bool:
        """Download an object from cloud storage to local_path
        
        The destination directory is created automatically if it doesn't exist.
        """
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        return self._with_retry("download", self._download_impl, remote_key, local_path)
    
    def delete(self, remote_key: str) -> bool:
        """Delete an object. Idempotent - no error if the key is absent"""
        return self._with_retry("delete", self._delete_impl, remote_key)

    def exists(self, remote_key: str) -> bool:
        """Return True when the object exists, False otherwise"""
        return self._with_retry("exists", self._exist_impl, remote_key)

    def list(self, prefix: str = "", *, page_size: int = 1000) -> Iterator[str]:
        """Yield all object keys under prefix (lazy, paginated)
        
        Parameters
        ----------
        prefix: str
            key prefix filter. Empty string lists the whole bucket
        page_size: int
            Number of keys fetched per API page
        """
        yield from self._list_impl(prefix, page_size=page_size)
        
    # ------------------------------------------------------------------
    # Abstract hooks — providers implement these
    # ------------------------------------------------------------------
 
    @abstractmethod
    def _upload_impl(
        self,
        local_path: str,
        remote_key: str,
        *,
        content_type: Optional[str],
        metadata: dict[str, str],
    ) -> bool: ...
    
    @abstractmethod
    def _download_impl(self, remote_key: str, local_path: str | Path) -> bool: ...
    
    @abstractmethod
    def _delete_impl(self, remote_key: str) -> bool: ...
    
    @abstractmethod
    def _exist_impl(self, remote_key: str) -> bool: ...
    
    @abstractmethod
    def _list_impl(self, prefix: str, *, page_size: int) -> Iterator[str]: ...
    
    # ------------------------------------------------------------------
    # Retry engine
    # ------------------------------------------------------------------

    def _with_retry(self, operation: str, fn, *args, **kwargs):
        last_exc: Optional[Exception] = None
        
        for attempt in range(self.retry.max_attempts):
            try:
                return fn(*args, **kwargs)
            except self.RETRYABLE_ERRORS as exc:
                last_exc = exc
                if attempt < self.retry.max_attempts - 1:
                    delay = self.retry.compute_delay(attempt)
                    print(
                        f"[{self.provider_name}] {operation} transient error "
                        f"(attempt {attempt + 1}/{self.retry.max_attempts}), "
                        f"retry in {delay:.2f}s - {exec}"
                    )
                    time.sleep(delay)
            except Exception:
                raise
            
        raise CloudUploadError(
            f"[{self.provider_name}] {operation} failed after "
            f"{self.retry.max_attempts} attempts. Last error: {last_exc}"
        )
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(provider={self.provider_name})"
    
# ---------------------------------------------------------------------------
# Domain exceptions
# ---------------------------------------------------------------------------

class CloudCLientError(Exception):
    """Base for all cloud client errors."""

class CloudUploadError(CloudCLientError):
    """Raises when an upload fails after all retries"""

class CloudDownloadError(CloudCLientError):
    """Raised when a download fails after all retries"""

class CloudConfigError(CloudCLientError):
    """Raised when credentials or config are missing or invalid"""