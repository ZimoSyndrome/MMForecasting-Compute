import pandas as pd
import numpy as np
import logging
logger = logging.getLogger(__name__)
class DataProcessor:
    def __init__(self):
        pass
    def clean_and_process(self, df: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """
        Takes raw OHLCV and produces canonical dataset with Log Returns & Vol
        """
        if df.empty:
            logger.warning(f"Empty dataframe for {ticker}")
            return df
        # 1. Basic Cleaning
        df = df.sort_values('date').drop_duplicates(subset=['date'])
        df = df[df['close'] > 0].copy() 
        
        # 2. Date Alignment (Business Daily)
        df.set_index('date', inplace=True)
        df.index = pd.to_datetime(df.index)
        
        full_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq='B')
        df = df.reindex(full_range)
        df.ffill(limit=5, inplace=True)
        df.dropna(inplace=True)
        # 3. Base Features
        price_col = 'adj_close' if 'adj_close' in df.columns else 'close'
        df['log_return'] = np.log(df[price_col] / df[price_col].shift(1))
        
        # Realized Volatility (Annualized)
        df['realized_vol'] = df['log_return'].rolling(window=21).std() * np.sqrt(252)
        # 4. Final Cleanup
        df.dropna(subset=['log_return', 'realized_vol'], inplace=True)
        
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'date'}, inplace=True)
        return df