"""
rl_trading/production/exchanges/binance_connector.py

Binance exchange connector.
No dependency on rl_trading - standalone tool.

bucket_name is optional - if not provided, logging is disabled gracefully.
"""

from __future__ import annotations 

from typing import Optional 
import pandas as pd
from binance.client import Client 
from binance.exceptions import BinanceAPIException, BinanceOrderException

from .base import BaseExchangeConnector

# Unified timeframe string -> Binance kline interval constant 
_TIMEFRAME_MAP: dict[str, str] = {
    "1h": Client.KLINE_INTERVAL_1HOUR,
    "4h": Client.KLINE_INTERVAL_4HOUR,
    "1d": Client.KLINE_INTERVAL_1DAY,
    "1w": Client.KLINE_INTERVAL_1WEEK,
}

# Stablecoin assets that count toward USD balance
_STABLECOINS = {"USDT", "BUSD", "USDC"}

class BinanceConnector(BaseExchangeConnector):
    """Binance spot API connector"""
    def __init__(self, api_key: str, secret_key: str, bucket_name: Optional[str] = None, testnet: bool = False) -> None:
        self._client = Client(api_key = api_key, api_secret = secret_key, testnet = testnet)
        self._log = self._build_logger(bucket_name)
        
        # Verify connectivity at init, fail fast if keys are wrong 
        try:
            self._client.ping()
            server_time = self._client.get_server_time()
            self._log.info(
                "init",
                f"Binance connected {'(testnet)' if testnet else '(mainnnet)'}",
                extra={"server_time": server_time["serverTime"], "testnet": testnet},
            )
        except BinanceAPIException as e :
            self._log.error("init", "Binance connection failed", exc=e)
            raise 
    
    def _build_logger(self, bucket_name: Optional[str] = None):
        try:
            from log_framework import DataLogger, CompositeFlush, OncePerDayFlush, ShutdownFlush
            from cloud_client import CloudClientFactory
            
            if not bucket_name:
                raise ValueError("no bucket")
            client = CloudClientFactory.s3(bucket_name)
            return DataLogger(
                job_part="Exchange/Binance",
                flush_strategy=CompositeFlush([
                    OncePerDayFlush(client),
                    ShutdownFlush(client)
                ]),
                log_sudbirs="results",
            )
        except Exception as e:
            import logging 
            return logging.getLogger("exchange_client.binance")
        
    # ------------------------------------------------------------------
    # OHLCV data
    # ------------------------------------------------------------------

    def get_ohlcv(self, symbol: str, timeframe: str, limit: int = 100) -> pd.DataFrame:
        """Fetch recent OHLCV candles from Binance"""
        interval = _TIMEFRAME_MAP.get(timeframe)
        if interval is None:
            raise ValueError(
                f"Unsupported timeframe '{timeframe}'. "
                f"Valid options: {list(_TIMEFRAME_MAP.keys())}"
            )
        
        try:
            klines = self._client.get_klines(
                symbol = symbol,
                interval = interval,
                limit = limit,
            )
        except BinanceAPIException as e:
            self._log.error(
                "get_ohlcv",
                f"Failed to fetch klines for {symbol}/{timeframe}",
                exc=e,
            )
            raise
        
        df = pd.DataFrame(klines, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "num_trades",
            "taker_buy_base", "taker_buy_quote", "ignore",       
        ])
        
        # Convert type
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df = df.set_index("open_time")
        
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
        
        # Drop the last row, it's the currently forming candle (incomplete)
        df = df.iloc[:-1]
        
        return df[["open", "high", "low", "close", "volume"]]
    
    def get_historical_data(
        self,
        symbol: str,
        timeframe: str,
        start_date: str = "1 Jan, 2015",
        end_date: str | None = None
    ) -> pd.DataFrame:
        """Fetch historical candles for backtesting or fine-tuning data prep"""
        interval = _TIMEFRAME_MAP.get(timeframe)
        if interval is None:
            raise ValueError(f"Unsupported timeframe: {timeframe}")
        
        try:
            klines = self._client.get_historical_klines(
                symbol = symbol,
                interval = interval,
                start_str = start_date,
                end_str = end_date
            )
        except BinanceAPIException as e:
            self._log.error(
                "get_historical_data",
                f"Failed to fetch historical klines for {symbol}",
                exc=e,
            )
            raise
        
        df = pd.DataFrame(klines, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "num_trades",
            "taker_buy_base", "taker_buy_quote", "ignore",
        ])
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df = df.set_index("open_time")
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
        
        self._log.info(
            "get_historical_data",
            f"Fetched {len(df)} historical candles for {symbol}/{timeframe}",
            extra={"symbol": symbol, "timeframe": timeframe, "rows": len(df)},
        )
        return df[["open", "high", "low", "close", "volume"]]

    # ------------------------------------------------------------------
    # Balance
    # ------------------------------------------------------------------

    def get_balance_usd(self) -> float:
        """Return total stablecoin balance (USDT + BUSB + USDC) in USD"""
        try:
            account = self._client.get_account()
        except BinanceAPIException as e:
            self._log.error("get_balance_usd", "Failed to fetch account", exc=e)
            raise
        
        total = sum(
            float(a["free"]) for a in account["balances"]
            if a["asset"] in _STABLECOINS
        )
        
        self._log.info(
            "get_balance_usd",
            f"USD balance: {total:.2f}",
            extra={"balance_usd": total},
        )
        return total
    
    def get_asset_balance(self, asset: str) -> float:
        """Return free balance for a specific asset (e.g. 'BTC')"""
        try:
            balance = self._client.get_asset_balance(asset=asset)
            return float(balance["free"]) if balance else 0.0
        except BinanceAPIException as e:
            self._log.error(
                "get_asset_balance",
                f"Failed to fetch balance for {asset}",
                exc=e,
            )
            return 0.0

    # ------------------------------------------------------------------
    # Order execution
    # ------------------------------------------------------------------

    def place_order(self, symbol: str, side: str, quantity: float,) -> dict | None:
        try:
            order = self._client.create_order(
                symbol = symbol,
                side = side.upper(),
                type = Client.ORDER_TYPE_MARKET,
                quantity = round(quantity, 0)
            )
            
            executed_qty = float(order.get("executeQty", 0))
            self._log.info(
                "place_order",
                f"Order placed - {side} {executed_qty:.6f} {symbol} "
                f"orderId={order.get('orderId')} status={order.get('status')}",
                extra = {
                    "symbol": symbol,
                    "side": side,
                    "quantity": quantity,
                    "executed_qty": executed_qty,
                    "order_id": order.get("orderId"),
                    "status": order.get("status"),
                },
            )
            self._log.flush()
            return order
        
        except BinanceOrderException as e:
            self._log.error(
                "place_order",
                f"Order reject by Binance - {side} {quantity:.6f} {symbol}",
                exc=e,
                extra={"symbol": symbol, "side": side, "quantity": quantity},
            )
            return None
        
        except BinanceAPIException as e:
            self._log.error(
                "place_order",
                f"Binance API error during order — {side} {quantity:.6f} {symbol}",
                exc=e,
            )
            return None

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def get_current_price(self, symbol: str) -> float:
        """Return the latest price for a symbol"""
        try:
            ticker = self._client.get_symbol_ticker(symbol=symbol)
            return float(ticker['price'])
        except BinanceAPIException as e:
            self._log.error("get_current_price", f"Failed to get price for {symbol}", exc=e)
            raise
    
    def get_symbol_info(self, symbol: str) -> dict:
        """Return exchange info for a symbol"""
        return self._client.get_symbol_info(symbol) or {}