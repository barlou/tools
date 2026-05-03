"""
providers/aws/s3.py
===================
AWS S3 implementation of CloudClientBase

Features:
---------
- Full CRUD: upload, download, delete, exists, list
- upload_bytes()        - upload in-memory buffer without temp file
                            (ideal for df.to_parquet() / dt.to_orc() buffers)
- Multipart upload      - files >8 MB use parallel parts automatically
- Content-type map      - .parquet, .orc, .avro, .log, .zip detected correctly 
- Paginated listing     - yields keys, lazily, handles any bucket size
- Retry on throttling   - SlowDown / TooManyRequests retried via base class 

OVN note
--------
OVH Object Storage exposes an s3-compatible API. Instantiate with endpoint_url-"https://s3.gra.cloud.ovh.net"
to point at OVH - all other behaviour is identical. The dedicated OVH provider (providers/ovh/)
delegates here and adds any OVH-specific auth logic on top 
"""

from __future__ import annotations

import mimetypes
from pathlib import Path
from typing import Iterator, Optional

import boto3
from botocore.config import Config as BotocoreConfig
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig

from cloud_client.base import (
    CloudClientBase,
    CloudDownloadError,
    CloudUploadError,
    RetryConfig,
)
from cloud_client.config import ConfigLoader

# ---------------------------------------------------------------------------
# Content-type overrides for data engineering formats
# ---------------------------------------------------------------------------

_MIME_MAP: dict[str, str] = {
    ".parquet": "application/vnd.apache.parquet",
    ".orc":     "application/vnd.apache.orc",
    ".avro":    "application/vnd.apache.avro",
    ".jsonl":   "application/x-ndjson",
    ".ndjson":  "application/x-ndjson",
    ".log":     "text/plain",
    ".zip":     "application/zip",
}

# Multipart threshold
# AWS recommended multipart for object >100 MB; 8 MB is a safe conservative default
_MULTIPART_THRESHOLD = 8 * 1024 * 1024
_MULTIPART_CHUNK      = 8 * 1024 * 1024 

# S3 error codes that are transient and worth retrying 
_RETRYABLE_CODES = frozenset({
    "SlowDown",
    "TooManyRequests",
    "RequestTimeout",
    "ServiceUnavailable",
})

class S3Client(CloudClientBase):
    """AWS S3 cloud storage client
    
    Parameters
    ----------
    bucket_name: str
        Target s3 bucket. Must exist and be accessible with provided creds.
    config_loader: ConfigLoader, Optional
        Credential source. Auto-discovered when omitted 
    retry_config: RetryConfig, optional
        Custom retry policy
    endpoint_url: str, optional
        Override the s3 endpoint. Used by OVHProvider and MinIO
        
    Examples:
    ---------
    >>> client = S3Client("my-bucket")
    >>> client.upload("local/data.parquet", "processed/2024-01/data.parquet)
    >>> client.upload_bytes(df.to_parquet(), "processed/2024-01/data.parquet)
    >>> for key in client.list("processed/2024-01/"):
    ...     print(key)
    """
    
    def __init__(
        self,
        bucket_name: str,
        config_loader: Optional[ConfigLoader] = None,
        retry_config: Optional[RetryConfig] = None,
        endpoint_url: Optional[str] = None,
    ) -> None:
        super().__init__(provider_name="aws", retry_config=retry_config)
        self.bucket_name = bucket_name
        self._loader = config_loader or ConfigLoader()
        self._endpoint_url = endpoint_url
        
        creds = self._loader.get_aws_credentials()
        self._s3 = self._build_client(creds)
        self._transfer_config = TransferConfig(
            multipart_threshold = _MULTIPART_THRESHOLD,
            multipart_chunksize = _MULTIPART_CHUNK,
            use_threads = True,
            max_concurrency = 4,
        )
        
    # ------------------------------------------------------------------
    # CloudClientBase implementation
    # ------------------------------------------------------------------
    
    def _upload_impl(
        self,
        local_path: str,
        remote_key: str,
        *,
        content_type: Optional[str],
        metadata:    dict[str, str],
    ) -> bool:
        ct = content_type or _infer_content_type(local_path)
        extra: dict = {"ContentType": ct}
        if metadata:
            extra["Metadata"] = metadata
        
        try:
            self._s3.upload_file(
                Filename=str(local_path),
                Bucket=self.bucket_name,
                Key=remote_key,
                ExtraArgs=extra,
                Config=self._transfer_config,
            )
        except ClientError as exc:
            self._handle_client_error(exc, "upload")
        return True
    
    def _download_impl(self, remote_key: str, local_path: Path) -> bool:
        try:
            self._s3.download_file(
                Bucket=self.bucket_name,
                Key=remote_key,
                Filename=str(local_path),
                Config=self._transfer_config,
            )
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("404", "NoSuchKey"):
                raise CloudDownloadError(
                    f"Key not found in '{self.bucket_name}': '{remote_key}"
                ) from exc
            self._handle_client_error(exc, "download")
        return True
    
    def _delete_impl(self, remote_key: str) -> bool:
        try:
            self._s3.delete_object(Bucket=self.bucket_name, Key=remote_key)
        except ClientError as exc:
            self._handle_client_error(exc, "delete")
        return True

    def _exists_impl(self, remote_key: str) -> bool:
        try:
            self._s3.head_object(Bucket=self.bucket_name, Key=remote_key)
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False
            self._handle_client_error(exc, "exists")
    
    def _list_impl(self, prefix: str, *, page_size: int) -> Iterator[str]:
        paginator = self._s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=self.bucket_name,
            Prefix=prefix,
            PaginatorConfig={"PageSize": page_size},
        )
        for page in pages:
            for obj in page.get("Contents", []):
                yield obj["Key"]

    # ------------------------------------------------------------------
    # S3-specific extras
    # ------------------------------------------------------------------
 
    def upload_bytes(
        self,
        data: bytes, 
        remote_key: str,
        *,
        content_type: str = "application/octet-stream",
        metadata: Optional[dict[str, str]] = None,
    ) -> bool:
        """Upload raw bytes directly without writing a local file.
        This is the preferred method when you have an in-memory Parquet or ORC buffer
        produced by pandas / PyArrow - avoids a temp file entirely 
        
        Example
        -------
        >>> buf = df.to_parquet()
        >>> client.upload_bytes(buf, "results/2024-01/data.parquet",
        ...                     content_type="application/vnd.apache.parquet")
        """
        try:
            self._s3.put_object(
                Bucket=self.bucket_name,
                Key=remote_key,
                Body=data,
                ContentType=content_type,
                Metadata=metadata or {},
            )
        except ClientError as exc:
            self._handle_client_error(exc, "upload_bytes")
        return True
    
    def get_presigned_url(self, remote_key: str, expiry_seconds: int = 3600) -> str:
        """Generate a pre-signed GET URL for remote_key
        
        Useful for sharing objects with downstream consumers without making the bucket public
        """
        return self._s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket_name, "Key": remote_key},
            ExpiresIn=expiry_seconds,
        )
        
 
    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
 
    def _build_client(self, creds: dict):
        kwargs: dict = {
            "aws_access_key_id":     creds["aws_access_key_id"],
            "aws_secret_access_key": creds["aws_secret_access_key"],
            "config": BotocoreConfig(
                retries={"mode": "standard"},
                signature_version="s3v4",
            ),
        }
        if creds.get("region_name"):
            kwargs["region_name"] = creds["region_name"]
        if self._endpoint_url:
            kwargs["endpoint_url"] = self._endpoint_url
        return boto3.client("s3", **kwargs)
 
    def _handle_client_error(self, exc: ClientError, operation: str) -> None:
        code = exc.response["Error"]["Code"]
        if code in _RETRYABLE_CODES:
            # Raise a plain Exception so the base retry engine catches it
            raise Exception(f"S3 transient [{code}] during {operation}") from exc
        raise CloudUploadError(
            f"S3 non-retryable [{code}] during {operation}: "
            f"{exc.response['Error'].get('Message', '')}"
        ) from exc


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _infer_content_type(path: Path) -> str:
    ext = path.suffix.lower()
    if ext in _MIME_MAP:
        return _MIME_MAP[ext]
    mime, _ = mimetypes.guess_type(str(path))
    return mime or "application/octet-stream"