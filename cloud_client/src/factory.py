# cloud_client/src/factory.py

"""
factory.py
==========
CloudClientFactory — instantiate the correct cloud storage client from config. 

Usage
-----
    # Auto-detect provider from config.json / env vars
    client = CloudClientFactory.s3("my-bucket")
    client = CloudClientFactory.s3("my-bucket", config_loader=loader)
    client = CloudClientFactory.s3("my-bucket", config_path="/path/to/config.json")
 
    # Generic factory (provider read from config)
    client = CloudClientFactory.create("my-bucket")
    client = CloudClientFactory.create("my-bucket", provider="gcp")
"""

from __future__ import annotations

from typing import Optional 

from .config import ConfigLoader
from .base import CloudClientBase, CloudConfigError, RetryConfig

class CloudClientFactory:
    """
    Factory for cloud storage clients.
 
    All methods return a CloudClientBase instance — callers never import
    provider-specific classes directly.
    """
    @staticmethod
    def s3(
        bucket_name: str,
        config_loader: Optional[ConfigLoader] = None,
        config_path: Optional[str] = None,
        retry_config: Optional[RetryConfig] = None,
        endpoint_url: Optional[str] = None,
    ) -> CloudClientBase:
        """
        Instantiate an AWS S3 client.
 
        Parameters
        ----------
        bucket_name: str
            Target S3 bucket name.
        config_loader: ConfigLoader, optional
            Pre-built credential loader. Auto-discovered when omitted.
        config_path: str, optional
            Explicit path to config.json. Used to build a ConfigLoader
            when config_loader is not provided.
        retry_config: RetryConfig, optional
            Custom retry policy. Defaults to library defaults.
        endpoint_url: str, optional
            Override the S3 endpoint URL (e.g. for OVH or MinIO).
 
        Returns
        -------
        CloudClientBase
            Configured S3Client instance.
        """
        from .providers.aws.s3 import S3Client
        
        loader = config_loader or ConfigLoader(config_path=config_path)
        return S3Client(
            bucket_name = bucket_name,
            config_loader = config_loader,
            retry_config = retry_config,
            endpoint_url = endpoint_url,
        )
        
    @staticmethod
    def create(
        bucket_name: str,
        provider: Optional[str] | None = None,
        config_path: Optional[str] | None = None, 
        config_loader: Optional[ConfigLoader] = None,
        retry_config: Optional[RetryConfig] = None,
    ) -> CloudClientBase:
        """
        Instantiate the correct cloud storage client based on provider.
 
        Provider is resolved in this order:
            1. `provider` argument
            2. `provider` key in config.json
            3. `CLOUD_PROVIDER` environment variable
            4. Defaults to "aws"
 
        Parameters
        ----------
        bucket_name: str
            Target bucket / container name.
        provider: str, optional
            Cloud provider: aws | gcp | azure | ovh
        config_loader: ConfigLoader, optional
            Pre-built credential loader.
        config_path: str, optional
            Explicit path to config.json.
        retry_config: RetryConfig, optional
            Custom retry policy.
 
        Returns
        -------
        CloudClientBase
        """
        loader = config_loader or ConfigLoader(config_path=config_path)
        provider = (
            provider 
            or loader.get("provider")
            or "aws"
        ).lower()
        
        if provider == "aws":
            return CloudClientFactory.s3(
                bucket_name = bucket_name,
                config_loader = loader,
                retry_config = retry_config,
            )
        elif provider == "gcp":
            from .providers.gcp import GCSClient
            return GCSClient(bucker_name = bucket_name, config_loader = loader)
        elif provider == "azure":
            from .providers.azure import AzureBlobClient
            return AzureBlobClient(bucket_name = bucket_name, config_loader = loader)
        elif provider == "ovh":
            return CloudClientFactory.s3(
                bucket_name = bucket_name,
                config_loader = config_loader,
                retry_config = retry_config,
                endpoint_url = loader.get("ovh_endpoint", "https://s3.gra.cloud.ovh.net"),
            )
        else:
            raise CloudConfigError(
                f"Unkown provider: '{provider}'. "
                f"Supported: aws, gcp, azure, ovh"
            )