""" 
exchange_client/src/factory.py

Exchange factory - instantiates the correct connector based on 
the EXCHANGE field in config / environment.

Follows the same pattern as CloudClientFactory in cloud_client. 
No dependency on rl_trading - standalone tool

Usage
-----
    # From a ConfigLoader (ingestion pipeline):
    from exchange_client import ExchangeFactory 
    connector = ExchangeFactory.from_config(config_loader)
    
    # From environment variables directly:
    connector = ExchangeFactory.from_env()
    
    # Explicit:
    connector = ExchangeFactory.create("kraken", api_key="...", secret_key="...")

Switching exchange 
------------------
Change ONE SSM parameter: /ingestion/prod/exchange/active -> "kraken" or "binance"
No code changes anywhere
"""

from __future__ import annotations

import os 
from typing import Optional 

from .base import BaseExchangeConnector

_SUPPORTED = ("kraken", "binance")

class ExchangeFactory:
    """  
    Factory for exchange connectors.
    
    All three class methods return a BaseExchangeConnector instance
    The caller never imports a specific connector directly
    """
    
    @classmethod
    def create(
        cls,
        exchange: str,
        api_key: str,
        secret_key: str,
        bucket_name: Optional[str] = None
    ) -> BaseExchangeConnector:
        """  
        Instantiate a connector by name.
        
        Args:
            exchange:    'kraken' or 'binance' (case-insensitive)
            api_key:     exchange API key
            secret_key:  exchange secret / private key
            bucket_name: S3 bucket for DataLogger (optional - falls back to 
                         BUCKET_NAME env var, then no logging)
        
        Returns:
            Ready-to-use BaseExchangeConnector instance
        
        Raises:
            ValueError: if exchange name is not supported
        """
        name = exchange.strip().lower()
        
        if name == "kraken":
            from .kraken_connector import KrakenConnector
            return KrakenConnector(
                api_key = api_key,
                secret_key = secret_key,
                bucket_name = bucket_name,
            )
        
        if name == "binance":
            from .binance_connector import BinanceConnector
            return BinanceConnector(
                api_key = api_key,
                secret_key = secret_key,
                bucket_name = bucket_name
            )
        
        raise ValueError(
            f"Unsupported exchange '{exchange}'. "
            f"Supported values: {_SUPPORTED}. "
            f"Check the EXCHANGE field in your config / SSM."
        )
    
    @classmethod
    def from_config(cls, config_loader) -> BaseExchangeConnector:
        """  
        Build a connector from a ConfigLoader instance. 
        
        Reads three keys from config (rendered from SSM at deploy time):
            EXCHANGE            -> 'kraken' or 'binance'
            EXCHANGE_ACCESS_KEY -> API key for the active exchange 
            EXCHANGE_SECRET_KEY -> Secret key for the active exchange 
            
        Used by the ingestion pipeline (CollectData_method.py)
        
        Args:
            config_loader: cloud_client.config.ConfigLoader instance 
            
        Returns:
            Ready-to-use BaseExchangeConnector instance 
        """
        exchange = (config_loader.get("EXCHANGE") or "").strip().lower()
        if not exchange:
            raise ValueError(
                "EXCHANGE is not set in config."
                "Add it to config.template.json and set the SSM parameter."
            )
        
        api_key = config_loader._resolve(
            env_key = "EXCHANGE_ACCESS_KEY",
            json_keys = ["EXCHANGE_ACCESS_KEY"],
            required = True,
        )
        secret_key = config_loader._resolve(
            env_key = "EXCHANGE_SECRET_KEY",
            json_keys = ["EXCHANGE_SECRET_KEY"],
            required = True
        )
        bucket_name = config_loader.get("bucket_name") or os.environ.get("BUCKET_NAME")
        
        return cls.create(
            exchange = exchange,
            api_key = api_key,
            secret_key = secret_key,
            bucket_name = bucket_name
        )

    @classmethod
    def from_env(cls) -> BaseExchangeConnector:
        """  
        Build a connector purely from env variables.
        
        Reads:
            EXCHANGE
            EXCHANGE_ACCESS_KEY
            EXCHANGE_SECRET_KEY
            BUCKET_NAME
        
        Used when no ConfigLoader is available (scripts, tests).
        """
        exchange = os.environ.get("EXCHANGE", "").strip().lower()
        api_key = os.environ.get("EXCHANGE_ACCESS_KEY", "")
        secret_key = os.environ.get("EXCHANGE_SECRET_KEY", "")
        bucket_name = os.environ.get("BUCKET_NAME", "")
        
        if not exchange:
            raise ValueError("EXCHANGE environment variable is not set")
        if not api_key: 
            raise ValueError("EXCHANGE_ACCESS_KEY environment variable is not set.")
        if not secret_key: 
            raise ValueError("EXCHANGE_SECRET_KEY environment variable is not set.")
        if not bucket_name: 
            raise ValueError("BUCKET_NAME environment variable is not set.")
        
        return cls.create(
            exchange = exchange,
            api_key = api_key,
            secret_key = secret_key,
            bucket_name = bucket_name
        )