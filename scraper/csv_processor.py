import os
import pandas as pd
from pathlib import Path


class StockPriceExtractor:
    """Extrae el último precio disponible para una lista de empresas"""

    def __init__(self, symbols=None):
        """Inicializa la clase y busca la carpeta de datos automáticamente"""
        self.data_dir = self._get_data_directory()
        self.csv_files = self.get_csv_files()
        self.symbols = symbols if symbols else ["ILF", "BAP", "BRK-B"]

    def _get_data_directory(self):
        """Encuentra la carpeta donde están los CSVs"""
        current_dir = Path(__file__).resolve().parent
        data_dir = current_dir / "data"

        if not data_dir.exists():
            raise FileNotFoundError(f"No se encontró la carpeta de datos en: {data_dir}")

        return data_dir

    def get_csv_files(self):
        """Obtiene la lista de archivos CSV dentro del directorio de datos"""
        return list(self.data_dir.glob("*.csv"))

    def get_latest_price(self):
        """Obtiene el último precio registrado para las empresas seleccionadas"""
        latest_prices = {}

        for file in self.csv_files:
            try:
                df = pd.read_csv(file, usecols=['symbol', 'timestamp', 'currentPrice'], parse_dates=['timestamp'])
                df = df[df['symbol'].isin(self.symbols)]
                df = df.sort_values('timestamp', ascending=False).groupby('symbol').first()
                latest_prices.update(df[['currentPrice']].to_dict()['currentPrice'])
            except Exception as e:
                print(f"❌ Error procesando {file.name}: {e}")

        return latest_prices


class StockPriceColumnExtractor:
    """Extrae todas las filas de una columna específica en los archivos CSV"""

    def __init__(self):
        """Inicializa la clase y busca la carpeta de datos automáticamente"""
        self.data_dir = self._get_data_directory()
        self.csv_files = self.get_csv_files()

    def _get_data_directory(self):
        """Encuentra la carpeta donde están los CSVs"""
        current_dir = Path(__file__).resolve().parent
        data_dir = current_dir / "data"

        if not data_dir.exists():
            raise FileNotFoundError(f"No se encontró la carpeta de datos en: {data_dir}")

        return data_dir

    def get_csv_files(self):
        """Obtiene la lista de archivos CSV dentro del directorio de datos"""
        return list(self.data_dir.glob("*.csv"))

    def get_all_column_data(self, column_name: str):
        """Obtiene todas las filas de la columna específica"""
        all_data = []

        for file in self.csv_files:
            try:
                df = pd.read_csv(file, usecols=['symbol', 'timestamp', column_name], parse_dates=['timestamp'])
                all_data.append(df)
            except ValueError:
                print(f"⚠️ Advertencia: {file.name} no contiene la columna '{column_name}'")
            except Exception as e:
                print(f"❌ Error procesando {file.name}: {e}")

        if all_data:
            result_df = pd.concat(all_data, ignore_index=True)
            return result_df
        else:
            return pd.DataFrame(columns=['symbol', 'timestamp', column_name])


# 🔹 Ejemplo de uso
if __name__ == "__main__":
    # Obtener el último precio de ILF, BAP y BRK-B
    extractor = StockPriceExtractor()
    latest_prices = extractor.get_latest_price()
    print("Últimos precios:", latest_prices)

    # Obtener todas las filas de la columna 'currentPrice'
    column_extractor = StockPriceColumnExtractor()
    all_prices_df = column_extractor.get_all_column_data("volume")
    print("Datos completos del Volumen:")
    print(all_prices_df)

