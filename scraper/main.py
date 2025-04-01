import pandas as pd
from pathlib import Path
import os


class StockPriceExtractor:
    def __init__(self, symbols=None):
        self.data_dir = self._get_data_directory()
        self.csv_files = self.get_csv_files()
        self.symbols = symbols if symbols else ["ILF", "BAP", "BRK-B"]

    def _get_data_directory(self):
        current_dir = Path(__file__).resolve().parent
        data_dir = current_dir / "data"
        if not data_dir.exists():
            raise FileNotFoundError(f"No se encontró la carpeta 'data' en: {data_dir}")
        return data_dir

    def get_csv_files(self):
        return list(self.data_dir.glob("*.csv"))

    def get_latest_price(self):
        latest_prices = {}
        print(f"🔍 Buscando archivos en: {self.data_dir}")
        for file in self.csv_files:
            print(f"📂 Procesando: {file.name}")
            try:
                df = pd.read_csv(file, usecols=['symbol', 'timestamp', 'currentPrice'], parse_dates=['timestamp'])
                print(f"  ✅ Columnas: {df.columns.tolist()}")
                df = df[df['symbol'].isin(self.symbols)]
                if df.empty:
                    print(f"  ⚠️ No hay datos para {self.symbols} en {file.name}")
                else:
                    latest_prices.update(df[['currentPrice']].to_dict()['currentPrice'])
            except Exception as e:
                print(f"  ❌ Error: {str(e)}")
        return latest_prices


if __name__ == "__main__":
    print("\n🔹 Iniciando extracción de precios...")
    extractor = StockPriceExtractor(symbols=["ILF", "BAP", "BRK-B"])
    latest_prices = extractor.get_latest_price()

    if latest_prices:
        print("\n✅ Resultados:")
        for symbol, price in latest_prices.items():
            print(f"{symbol}: ${price:.2f}")
    else:
        print("\n❌ No se encontraron datos. Verifica:")
        print("- Archivos CSV en 'scraper/data'")
        print("- Columnas requeridas: 'symbol', 'timestamp', 'currentPrice'")