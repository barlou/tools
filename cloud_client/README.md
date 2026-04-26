1 # cloud_client

Multi-provider cloud storage client for data engineering workloads.

Abstract AWS S3, OVH Object Storage (and future Azure / GCP) behind a single interface so your pipeline code never needs to know which cloud it's running on.

---

## Table of contents 

- [Installation](#installation)
- [Configuration](#configuration)
- [Quick start](#quick-start)
- [API references](#api-references)
  - [CloudClientFactory](#core-operations)
  - [Core operations](#core-operation)
  - [S3-specific extras](#s3-specific-extras)
  - [RetryConfig](#retryconfig)
  - [Exceptions](#exceptions)
- [Providers](#providers)
  - [AWS S3] (#aws-s3)
  - [OVH Object Storage](#ovh-object-storage)
  - [Azure / GCP (v2)](#azure--gcp-v2)
- [Adding a new provider](#adding-a-new-provider)

---

## Installation

```bash
# From the repository root - install in editable mode (local dev)
pip install -e cloud_client/

# With optional Azure extras (when azure provider is implemented)
pip install -e "cloud_client/[azure]"

# with dev/tests dependencies (pytest, moto s3 mock)
pip install -e "cloud_client/[dev]"
```

**Python requirement:** `>=3.10`

---

## Configuration 

Credentials are resolved in this priority order - highest wins:

| Priority | Source | When to use |
|---|---|---|
| 1 | Environment variable | CI/CD (Github actions secrets), Docker, Airflow connections |
| 2 | `config/config.json` | Local dev, server (rendered from SSM by `deploy.sh`) |

### Environment variables 

| Variable | Description |
|---|---|
| `AWS_ACCESS_KEY_ID` | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key |
| `AWS_DEFAULT_REGION` | AWS region (e.g, `eu-west-3`) |

### config.json (local dev only - never commit)

Copy to template and fill in your values:

```bash
cp cloud_client/config/config.template.json cloud_client/config/config.json
```

```json
{
    "storage": {
        "aws_access_key_id":     "YOUR_KEY",
        "aws_secret_access_key": "YOUR_SECRET",
        "aws_region":            "eu-west-3",
        "bucket_name":           "my-data-bucket"
    }
}
```

> **`config.json` is gitignored.** The template (`config.template.json`) uses
> `{{ PLACEHOLDER }}` syntax - real values are injected from AWS SSM at deploy 
> time by `deploy.sh` (barlou/CICD). Never commit credentials.

### Server (barlou/CICD)

`deploy.sh` exports `MODULE_NAME` and renders `config.json` from SSM parameters
defined in `cicd.config.yml`, `ConfigLoader` finds it automatically at:

```
~/deployments/{MODULE_NAME}/config/config.json
```

---

## Quick start

```python
from cloud_client import CloudClientFactory 

# --- AWS S3 ---
client = CloudClientFactory.s3("my-data-bucket")

# --- OVH Object Storage ---
client = CloudClientFactory.ovh(
    "my-container",
    endpoint_url="https://s3.gra.cloud.ovh.net",
)

# Upload a local file 
client.upload("local/output.parquet", "processed/2024-01/output.parquet")

# Upload an in-memory buffer (no temp file needed)
buf = df.to_parquet()
client.upload_bytes(buf, "processed/2024-01/output.parquet",
                    content_type="application/vnd.apache.parquet")

# Download 
client.download("processed/2024-01/output.parquet", "local/output.parquet")

# Check existence before an expensive operation
if client.exists("processed/2024-01/output.parquet"):
    client.delete("processed/2024-01/output/parquet")

# List all keys under a prefix (lazy - no fill scan in memory)
for key in client.list("processed/2024-01/"):
    print(key)
```

---

## Api reference

### CloudClientFactory 

Entry point. Returns a fully configured client for the requested provider.

```python
from cloud_client import CloudClientFactory
```

#### `CloudClientFactory.s3(bucket_name, config_path=None, retry_config=None) -> S3Client`

| Parameter | Type | Default | Description | 
|---|---|---|---|
| `bucket_name` | `str` | required | Target s3 bucket |
| `config_path` | `str` | `None` | Explicit path to `config.json`. Auto-discovered when omitted |
| `retry-config` | `RetryConfig` | `None` | Custom retry policy |

#### `CloudClientFactory.ovh(bucket_name, endpoint_url, config_path=None, retry_config=None) -> OVHClient`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `bucket_name`| `str` | required | OVH container name |
| `endpoint_url`| `str` | required | OVH S3 endpoint, e.g, `"https://s3.gra.cloud.ovh.net"` |
| `config_path` | `str` | `None` | Explicit path to `config.json` |
| `retry_config` | `retryConfig`| `None` | Custom retry policy |

---

### Core operations 

All providers expose the same five methods inherited from `CloudClientBase`.

#### `upload(local_path, remote_key, *, content_type=None, metadata=None) -> bool`

Upload a local file to cloud storage 

```python
# Basic upload
client.upload("data/output.parquet", "processed/2024-01/output.parquet")

# With metadata (stored alongside the object)
client.upload(
    "data/output.parquet",
    "processed/2024-01/output.parquet",
    content_type = "application/vnd.apache.parquet",
    metadata={"pipeline_run": "2024-01-15", "dag_id": "data_ingestion"},
)
```

Content-type is inferred from the file extension when omitted:

| Extension | Detected MIME type |
|---|---|
| `.parquet`| `application/vnd.apache.parquet` |
| `.orc`| `application/vnd.apache.orc` |
| `.avro`| `application/vnd.apache.avro` |
| `.jsonl` / `.ndjson` | `application/x-ndjson` |
| `.log`| `text/plain` |
| `.zip`| `application/zip` |

Files larger than **8 MB** are automatically split into parallel multipart parts - no configuration required 

#### `download(remote_key, local_path) -> bool`

```python
client.download("processed/2024-01/output.parquet", "local/output.parquet")
```

The destination directory is created automatically if it doesn't exist.

#### `delete(remote_key) -> bool`

```python
client.delete("processed/2024-01/output.parquet")
```

Idempotent - no error if the key doesn't exist.

#### `exists(remote_key) -> bool`

```python
if client.exists("processed/2024-01/output.parquet"):
        # ...
```

Uses a lightweight `HEAD` request - no data is transferred.

#### `list(prefix="", *, page_size=1000) -> Iterator[str]`

```python
# All keys under a prefix
for key in client.list("processed/2024-01/"):
    print(key)

# All keys in the bucket 
for key in client.list():
    print(key)
```

Lazy and paginated - safe on buckets with millions of objects.

---

### S3-specific extras

These methods are available on `S3Client`and `OVHClient` (which extends `S3Client`).

#### `upload_bytes(data, remote_key, *, content_type=..., metadata=None) -> bool`

Upload raw bytes without writing a local file first.
Preferred when you have an in-memory Parquet/ORC buffer from pandas / PyArrow

```python
import pandas as pd 

df = pd.read_csv("data.csv")

# Parquet buffer -> s3
buf = df.to_parquet()
client.upload_bytes(
    buf,
    "processed/2024-01/output.parquet",
    content_type="application/vnd.apache.parquet",
)

# ORC buffer -> s3
import pyarrow as pa, pyarrow.src as orc, io
table = pa.Table.from_pandas(df)
buf = io.BytesIO()
orc.write_table(table, buf)
client.upload_bytes(buf.getvalue(), "processed/2024-01/output.orc")
```

#### `get_presigned_url(remote_key, expiry_seconds=3600) -> str`

Generate a pre-signed GET URL - share objects without making the bucket public.

```python
url = client.get_presigned_url("processed/2024-01/output.parquet", expiry_seconds=86400)
# share url with downstream consumers - valid for 24h
```

---

### RetryConfig 

Controls exponential-backoff retry behaviour. Applies to all operations.

```python
from cloud_client import RetryConfig, ConfigClientFactory

retry = RetryConfig(
    max_attempts=3,
    base_delay=0.5,
    max_delay=16.0,
    jitter_factory=0.25,
)
client = CloudClientFactory.s3("my-bucket", retry_config=retry)
```

Default values: `max_attempts=5`, `base_delay=1.0s`, `max_delay=32.0s`, `jitter_factor=0.25`.

S3-specific transient errors that triggers a retry: `SlowDown`, `TooManyRequests`, `RequestTimeout`, `ServiceUnavailable`.

---

### Exceptions 

```python
from cloud_client import CloudClientError, CloudUploadError, CloudDownloadError, CloudConfigError

try:
    client.upload("missing_file.parquet", "remote/key.parquet")
except FileNotFoundError:
    # local_path doesn't exist on disk
    pass
except CloudUploadError:
    # all retry attempts exhausted
    pass
except CloudConfigError:
    # Credentials missing or config.json invalid
    pass
```

| Exception | Raised when |
|---|---|
| `CloudConfigError` | Credentials missing from env and config.json |
| `CloudUploadError` | Upload fails after all retry attempts |
| `CloudDownloadError` | Download fails - key not found or network error |
| `CloudClientError` | Base class - catch-all for any cloud client error |

---

## Providers

### AWS S3

```python
client = CloudClientFactory.s3("my-bucket")
```

Credentials are picked up from env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) or `config.json`. The underlying boto3 client uses `signature_version=s3v4` and `retries.mode=standard` (botocore's own low-level retry layer on top of ours).

### OVH Object Storage

OVG exposes an S3-compatible API. the only difference is the endpoint URL.

```python
client = CloudClientFactory.ovh(
    "my-container",
    endpoint_url="https://s3.gra.cloud.ovh.net",
)
```

OVH credentials (`access_key` / `secret_key`) are loaded from the same env vars and `config.json` as AWS - the format is identical 

### Azure / GCP (v2)
Provider folders are scaffolded (`providers/azure/`, `providers/gcp/`) with a `NotImplementedError` and an implementation guide. Use S3 or OVH for now.

---

## Addinng a new provider

1. Create `src/providers/{name}/__init__.py`
2. Define a class that extends `CloudClientBase`
3. Implement the five abstract methods: `_upload_impl`,`_download_impl`, `_delete_impl`, `_exists_impl`, `_list_impl`
4. Set `RETRYABLE_ERRORS` to the provider's transient exception types 
5. Add a factory method to `CloudClientFactory` is `src/__init__.py`

```python
# src/providers/azure/__init__.py
from base import CloudClientBase

class AzureClient(CloudClientBase):
    RETRYABLE_ERRORS = (ResourceNotFoundErrors, ServiceRequestError)

    def __init__(self, container_name, ...):
        super().__init__(provider_name="Azure Blob Storage")
        # ...
    
    def __upload_impl(self, local_path, remote_key, *, content_type, metadata):
        # blob_client.upload_blob(...)
        ...
```