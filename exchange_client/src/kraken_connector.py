"""  
exchange_client/src/kraken_connector.py

Kraken exchange connector
No dependency on rl_trading - standalone tool.

bucket_name is optional - if not provided, logging is disabled gracefully.
"""

from __future__ import annotations

from typing import Optional
import pandas as pd
import krakenex 

from .base import BaseExchangeConnector

_TIMEFRAME_MAP: dict[str, int] = {
    "1h": 60,
    "4h": 240,
    "1d": 1440,
    "1w": 10080,
}

# Symbol normalisation: unified name -> Kraken pair name
_SYMBOL_MAP: dict[str, str] = {
    "BTCUSDT":   "XBTUSD",
    "ETHUSDT":   "ETHUSD",
    "LTCUSDT":   "LTCUSD",
    "ADAUSDT":   "ADAUSD",
    "SOLUSDT":   "SOLUSD",
    "XRPUSDT":   "XRPUSD",
    "DOTUSDT":   "DOTUSD",
    "LINKUSDT":  "LINKUSD",
    "MATICUSDT": "MATICUSD",
    "AVAXUSDT":  "AVAXUSD",
}

# Assets counted as USD balance
_USD_ASSETS = {"USD", "ZUSD", "USDT", "USDC"}

class KrakenConnector(BaseExchangeConnector):
    """Kraken Rest API connector"""
    def __init__(self, api_key: str, secret_key: str, bucket_name: Optional[str] = None) -> None:
        
        self._api = krakenex.API(key=api_key, secret=secret_key)
        self._log = self._build_logger(bucket_name)
        
        # Verify connectivity at init
        try:
            response = self._api.query_public("Time")
            self._raise_on_error(response, "init")
            self._log.info(
                "init",
                "Kraken connected",
                extra={"server_time": response["result"]["unixtime"]}
            )
        except Exception as e:
            self._log.error("init", "Kraken connection failed", exc=e)
            raise
    
    def _build_logger(self, bucket_name: Optional[str]):
        """Build logger if log_framework and cloud_client are available."""
        try:
            from log_framework import DataLogger, CompositeFlush, OncePerDayFlush, ShutdownFlush
            from cloud_client import CloudClientFactory
            if not bucket_name:
                raise ValueError("no bucket")
            client = CloudClientFactory.s3(bucket_name)
            return DataLogger(
                job_part="Exchange/Kraken",
                flush_strategy=CompositeFlush([
                    OncePerDayFlush(client),
                    ShutdownFlush(client),
                ]),
                log_subdir="results",
            )
        except Exception:
            import logging 
            return logging.getLogger("exchange_client.kraken")
    # ------------------------------------------------------------------
    # OHLCV data
    # ------------------------------------------------------------------

    def get_ohlcv(self, symbol: str, timeframe: str, limit: int = 100) -> pd.DataFrame:
        """Fetch recent OHLCV candles from Kraken"""
        interval = _TIMEFRAME_MAP.get(timeframe)
        kraken_pair = self._normalize_symbol(symbol)
        
        if interval is None:
            raise ValueError(
                f"Unsupported timeframe '{timeframe}'. "
                f"Valid: {list(_TIMEFRAME_MAP.keys())}"
            )
        
        try:
            response = self._api.query_public("OHLC", {
                "pair": kraken_pair,
                "interval": interval,
            })
            self._raise_on_error(response, "get_ohlcv")
        except Exception as e:
            self._log.error(
                "get_ohlcv",
                f"Failed to fetch OHLCV for {symbol}/{timeframe}",
                exc=e,
            )
            raise
        
        # Kraken OHLC format:
        # [time, open, high, low, close, vwap, volume, count]
        result_key = list(response["result"].keys())[0]
        raw = response["result"][result_key]
        
        df = pd.DataFrame(raw, columns=[
            "time", "open", "high", "low", "close", "vwap", "volume", "count"
        ])
        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
        df = df.set_index("time")
        
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
        
        # Kraken includes the current (incomplete) candle last — drop it 
        df = df.iloc[:-1]
        
        # Kraken returns all history — slice to requested limit
        df = df.tail(limit)
        
        return df[["open", "high", "low", "close", "volume"]]
    
    def get_historical_date(self, symbol: str, timeframe: str, since: int | None = None,) -> pd.DataFrame:
        """Fetch historical OHCLV data"""
        interval = _TIMEFRAME_MAP.get(timeframe)
        kraken_pair = self._normalize_symbol(symbol)
        
        params: dict = {"pair": kraken_pair, "interval": interval}
        if since:
            params["since"] = since
        
        response = self._api.query_public("OHLC", params)
        self._raise_on_error(response, "get_historical_date")
        
        result_key = list(response["result"].keys())[0]
        raw = response["result"][result_key]
        
        df = pd.DataFrame(raw, columns=[
            "time", "open", "high", "low", "close", "vwap", "volume", "count"
        ])
        df["time"] = pd.to_datetime(df['time'], unit="s", utc=True)
        df = df.set_index("time")
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
        
        self._log.info(
            "get_historical_date",
            f"Fetched {len(df)} historical candles for {symbol}/{timeframe}",
            extra={"symbol": symbol, "rows": len(df)},
        )
        return df[["open", "high", "low", "close", "volume"]]

    # ------------------------------------------------------------------
    # Balance
    # ------------------------------------------------------------------

    def get_balance_usd(self) -> float:
        """Return total USD-equivalent balance (USD + ZUSD + USDT + USDC)"""
        try:
            response = self._api.query_private("Balance")
            self._raise_on_error(response, "get_balance_usd")
        except Exception as e :
            self._log.error("get_balance_usd", "Failed to fetch balance", exc=e)
            raise
        
        balances = response.get("result", {})
        total = sum(
            float(v) for k, v in balances.items()
            if k.upper() in _USD_ASSETS
        )
        self._log.info(
            "get_balance_usd",
            f"USD balance: {total:.2f}",
            extra={"balance_usd": total},
        )
        return total 
    
    def get_asset_balance(self, asset: str) -> float:
        """Return balance for a specific asset."""
        _asset_map = {'BTC': "XXBT", "ETH":"XETH", "SOL":"SOL"}
        kraken_asset = _asset_map.get(asset.upper(), asset)
        
        try:
            response = self._api.query_private("Balance")
            self._raise_on_error(response, "get_asset_balance")
            balances = response.get("result", {})
            return float(balances.get(kraken_asset, 0.0))
        except Exception as e:
            self._log.error(
                "get_asset_balance",
                f"Failed to fetch balance for {asset}",
                exc=e,
            )
            return 0.0

    # ------------------------------------------------------------------
    # Order execution
    # ------------------------------------------------------------------

    def place_order(self, symbol: str, side: str, quantity: float) -> dict | None:
        """Place a market order on Kraken"""
        kraken_pair = self._normalize_symbol(symbol)
        kraken_side = "buy" if side.upper() == "BUY" else "sell"
        
        data = {
            "pair":      kraken_pair,
            "type":      kraken_side,
            "ordertype": "market",
            "volume":    str(round(quantity, 6)),
        }
        
        try:
            response = self._api.query_private("AddOrder", data)
            self._raise_on_error(response, "place_order")
            
            result = response.get("result", {})
            txids = result.get("txid", [])
            
            self._log.info(
                "place_order",
                f"Order placed — {side} {quantity:.6f} {symbol} "
                f"txid={txids}",
                extra={
                    "symbol": symbol, "side": side, 
                    "quantity": quantity, "txid": txids,
                },
            )
            self._log.flush()
            return result 
        except Exception as e :
            self._log.error(
                "place_order",
                f"Order failed — {side} {quantity:.6f} {symbol} ",
                exc = e,
                extra={"symbol": symbol, "side": side, "quantity": quantity},
            )
            return None

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def get_current_price(self, symbol: str) -> float:
        """Return the latest ask price for a symbol"""
        kraken_pair = self._normalize_symbol(symbol)
        response    = self._api.query_public("Ticker", {"pair": kraken_pair})
        self._raise_on_error(response, "get_current_price")
        result_key = list(response["result"].keys())[0]
        return float(response["result"][result_key]["a"][0])
    
    def _normalize_symbol(self, symbol: str) -> str:
        """Convert unified symbol format to Kraken pair name."""
        return _SYMBOL_MAP.get(symbol.upper(), symbol)
    
    def _raise_on_error(self, response: dict, method: str) -> None:
        """Raise an exception if the kraken response contains errors."""
        errors = response.get("error", [])
        if errors:
            error_str = ", ".join(errors)
            self._log.error(
                method,
                f"Kraken API error: {error_str}",
                extra={"errors": errors},
            )
            raise RuntimeError(f"Kraken API error in {method}: {error_str}")