
"""
exchange_client/src/base.py
 
Abstract exchange connector interface.
No dependency on rl_trading — standalone tool.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
import pandas as pd 

class BaseExchangeConnector(ABC):
    """  
    Abstract base for all exchange connectors.
    
    Subclasses must implement the three methods below 
    Constructor must accept api_key and secret_key
    """
    
    @abstractmethod
    def get_ohlcv(
        self,
        symbol: str, 
        timeframe: str,
        limit: str = 100
    ) -> pd.DataFrame:
        """ 
        Fetch recent OHLCV candles 
        
        Returns Df[open, high, low, close, volume]
        DatetimeIndex UTC, newest row last
        """
        
    @abstractmethod
    def get_balance_usd(self) -> float:
        """ 
        Fetch total portfolio value in USD
        
        Binance: sum of USDT + BUSD balances
        Kraken: sum of USD + ZUSD balances
        """
    
    @abstractmethod
    def place_order(
        self,
        symbol: str,
        side: str,
        quantity: str
    ) -> dict | None:
        """  
        Place a market order 
        
        Args: 
            symbol:   trading pair (e.g, 'BTCUSDT')
            side:     'BUY' or 'SELL'
            quantity: base asset quantity to trade 
        
        Returns:
            Exchange order confirmation dict, or None on failure 
        """