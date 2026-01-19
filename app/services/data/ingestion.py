import pandas as pd
import yfinance as yf
from alpaca_trade_api.rest import REST, TimeFrame
from datetime import datetime, date
from typing import Optional, Union
import logging
import os
logger = logging.getLogger(__name__)
class DataIngestion:
    def __init__(self):
        self.alpaca = None
        # Directly use os.getenv instead of src.config
        api_key = os.getenv("ALPACA_API_KEY")
        secret_key = os.getenv("ALPACA_API_SECRET") # Note: typically SECRET_KEY or API_SECRET
        endpoint = os.getenv("ALPACA_ENDPOINT", "https://paper-api.alpaca.markets")
        
        if api_key and secret_key:
            try:
                self.alpaca = REST(
                    api_key,
                    secret_key,
                    base_url=endpoint
                )
            except Exception as e:
                logger.warning(f"Failed to initialize Alpaca API: {e}")
        else:
            logger.warning("Alpaca credentials not found. Alpaca fetching will be disabled.")
    def fetch_data(
        self, 
        ticker: str, 
        start_date: Union[str, date, datetime], 
        end_date: Union[str, date, datetime], 
        source: str = "alpaca",
        force_fresh: bool = False
    ) -> pd.DataFrame:
        """
        Fetch market data for a ticker.
        """
        # Ensure dates are strings YYYY-MM-DD
        start_str = self._format_date(start_date)
        end_str = self._format_date(end_date)
        
        # Note: In compute service, we might skip local caching to /tmp 
        # or use a temp file if needed. For now, we fetch fresh every time
        # to simplify stateless Execution.
        
        if source == "alpaca":
            df = self._fetch_alpaca(ticker, start_str, end_str)
        elif source == "yahoo":
            df = self._fetch_yahoo(ticker, start_str, end_str)
        else:
            raise ValueError(f"Unknown source: {source}")
            
        return df
    def _fetch_alpaca(self, ticker: str, start: str, end: str) -> pd.DataFrame:
        if not self.alpaca:
            logger.error("Alpaca API not initialized.")
            return pd.DataFrame()
            
        logger.info(f"Fetching {ticker} from Alpaca ({start} to {end})...")
        try:
            bars = self.alpaca.get_bars(
                ticker, 
                TimeFrame.Day, 
                start=start, 
                end=end, 
                adjustment='raw',
                feed='iex' 
            ).df
            
            if bars.empty:
                logger.warning(f"No data returned from Alpaca for {ticker}")
                return bars
            # Normalize columns
            bars = bars.reset_index() # timestamp is index
            bars.rename(columns={'timestamp': 'date'}, inplace=True)
            
            if 'date' in bars.columns:
                 bars['date'] = pd.to_datetime(bars['date']).dt.tz_convert(None)
            return bars
        except Exception as e:
            logger.error(f"Error fetching from Alpaca: {e}")
            return pd.DataFrame()
    def _fetch_yahoo(self, ticker: str, start: str, end: str) -> pd.DataFrame:
        logger.info(f"Fetching {ticker} from Yahoo Finance ({start} to {end})...")
        try:
            df = yf.download(ticker, start=start, end=end, progress=False)
            
            if df.empty:
                logger.warning(f"No data returned from Yahoo for {ticker}")
                return df
                
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df.reset_index(inplace=True)
            df.columns = [c.lower() for c in df.columns] 
            
            rename_map = {'adj close': 'adj_close'}
            df.rename(columns=rename_map, inplace=True)
             
            expected_cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'adj_close']
            df = df[[c for c in expected_cols if c in df.columns]]
            
            return df
        except Exception as e:
            logger.error(f"Error fetching from Yahoo: {e}")
            return pd.DataFrame()
    def _format_date(self, d: Union[str, date, datetime]) -> str:
        if isinstance(d, datetime) or isinstance(d, date):
            return d.strftime("%Y-%m-%d")
        return d