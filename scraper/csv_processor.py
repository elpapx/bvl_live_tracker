import pandas as pd
from datetime import datetime
import os
from functools import lru_cache

@lru_cache(maxsize=1)
def _load_full_csv(self):
    return pd.read_csv(self.csv_path, parse_dates=['timestamp'])


class CSVDataProcessor:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self._validate_csv()

    def _validate_csv(self):
        """Verifica que el CSV exista y tenga las columnas necesarias"""
        if not os.path.exists(self.csv_path):
            raise FileNotFoundError(f"Archivo CSV no encontrado: {self.csv_path}")

        # Lectura rápida solo de cabeceras
        df_sample = pd.read_csv(self.csv_path, nrows=1)
        required_columns = {'currentPrice', 'previousClose', 'open', 'dayLow', 'dayHigh', 'regularMarketPreviousClose',
                            'regularMarketOpen', 'dividendRate', 'dividendYield', 'exDividendDate',
                            'volume', 'shortName', 'timestamp'}
        if not required_columns.issubset(df_sample.columns):
            raise ValueError(f"CSV debe contener las columnas: {required_columns}")

    def get_realtime_data(self, company: str):
        """Obtiene el último registro para una empresa"""
        df = pd.read_csv(self.csv_path)
        df = df[df['shortName'] == company]
        return df.sort_values('timestamp', ascending=False).iloc[0].to_dict()

    def get_historical_data(self, company: str, days: int = 30):
        """Obtiene datos históricos"""
        df = pd.read_csv(self.csv_path, parse_dates=['timestamp'])
        df = df[df['shortName'] == company]
        cutoff_date = datetime.now() - pd.Timedelta(days=days)
        return df[df['timestamp'] >= cutoff_date].to_dict('records')