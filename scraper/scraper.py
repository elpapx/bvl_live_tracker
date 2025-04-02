import yfinance as yf
import pandas as pd
import time
import os
import logging
from typing import Dict, List
from pathlib import Path
from datetime import datetime


# Configuración centralizada
class ScraperConfig:
    # Configuración de rutas
    SCRIPT_DIR = Path(__file__).parent
    PROJECT_ROOT = SCRIPT_DIR.parent
    DATA_DIR = PROJECT_ROOT / "data"
    LOGS_DIR = PROJECT_ROOT / "logs"

    # Configuración de tiempo
    UPDATE_INTERVAL_MINUTES = 1
    TOTAL_RUNTIME_HOURS = 7
    TOTAL_ITERATIONS = (TOTAL_RUNTIME_HOURS * 60) // UPDATE_INTERVAL_MINUTES

    # Configuración de logging
    LOG_LEVEL = logging.INFO
    LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

    # Instrumentos a rastrear
    INSTRUMENTS = {
        "BAP": {
            "columns": [
                "currentPrice", "previousClose", "open", "dayLow", "dayHigh",
                "bid", "dividendYield", "earningsGrowth", "revenueGrowth",
                "grossMargins", "ebitdaMargins", "operatingMargins", "financialCurrency",
                "returnOnAssets", "returnOnEquity", "bookValue", "priceToBook", 'volume'
            ],
            "filename": "bap_stock_data.csv"
        },
        "BRK-B": {
            "columns": [
                "currentPrice", "previousClose", "open", "dayLow", "dayHigh",
                "bid", "dividendYield", "earningsGrowth", "revenueGrowth",
                "grossMargins", "ebitdaMargins", "operatingMargins", "financialCurrency",
                "returnOnAssets", "returnOnEquity", "bookValue", "priceToBook", 'volume'
            ],
            "filename": "brk-b_stock_data.csv"
        },
        "ILF": {
            "columns": [
                "previousClose", "open", "dayLow", "dayHigh",
                "volume", "regularMarketVolume", "averageVolume", "averageVolume10days",
                "bid", "ask", "dividendYield", 'volume'
            ],
            "filename": "ilf_etf_data.csv",
            "is_etf": True
        }
    }


class DataScraper:
    def __init__(self):
        self._setup_directories()
        self._setup_logging()
        self.logger = logging.getLogger(__name__)

    def _setup_directories(self):
        """Crea los directorios necesarios si no existen"""
        os.makedirs(ScraperConfig.DATA_DIR, exist_ok=True)
        os.makedirs(ScraperConfig.LOGS_DIR, exist_ok=True)

    def _setup_logging(self):
        """Configura el sistema de logging"""
        log_filename = ScraperConfig.LOGS_DIR / f"scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

        logging.basicConfig(
            level=ScraperConfig.LOG_LEVEL,
            format=ScraperConfig.LOG_FORMAT,
            handlers=[
                logging.FileHandler(log_filename),
                logging.StreamHandler()
            ]
        )

    def fetch_instrument_data(self, symbol: str, config: Dict) -> Dict:
        """Obtiene los datos del instrumento financiero"""
        try:
            self.logger.info(f"Iniciando scraping para {symbol}")
            ticker = yf.Ticker(symbol)
            data = {}

            if config.get("is_etf", False):
                self.logger.debug(f"{symbol} es un ETF, usando metodo especial")
                history = ticker.history(period="1d")
                if not history.empty:
                    data["currentPrice"] = history["Close"].iloc[-1]
                    self.logger.debug(f"Precio actual obtenido: {data['currentPrice']}")

                info = ticker.info
                for key in config["columns"]:
                    data[key] = info.get(key, None)
                    self.logger.debug(f"{key}: {data[key]}")
            else:
                self.logger.debug(f"{symbol} es una accion, usando metodo estandar")
                info = ticker.info
                for key in config["columns"]:
                    data[key] = info.get(key, None)
                    self.logger.debug(f"{key}: {data[key]}")

            data["symbol"] = symbol
            data["timestamp"] = pd.Timestamp.now()

            self.logger.info(f"Datos obtenidos exitosamente para {symbol}")
            return data

        except Exception as e:
            self.logger.error(f"Error al obtener datos de {symbol}: {str(e)}", exc_info=True)
            return None

    def save_to_csv(self, data: Dict, filename: str) -> None:
        """Guarda los datos en CSV"""
        if data is None:
            self.logger.warning("No hay datos para guardar")
            return

        try:
            filepath = ScraperConfig.DATA_DIR / filename
            df = pd.DataFrame([data])

            if filepath.exists():
                df.to_csv(filepath, mode='a', header=False, index=False)
                self.logger.info(f"Datos añadidos a {filename}")
            else:
                df.to_csv(filepath, mode='w', header=True, index=False)
                self.logger.info(f"Archivo {filename} creado exitosamente")

            self.logger.debug(f"Ultimo registro: {data['timestamp']}")

        except Exception as e:
            self.logger.error(f"Error al guardar datos en {filename}: {str(e)}", exc_info=True)

    def run_scraping_cycle(self) -> None:
        """Ejecuta un ciclo completo de scraping"""
        self.logger.info("Iniciando ciclo de scraping")
        for symbol, config in ScraperConfig.INSTRUMENTS.items():
            data = self.fetch_instrument_data(symbol, config)
            if data:
                self.save_to_csv(data, config["filename"])
        self.logger.info("Ciclo de scraping completado")

    def run(self) -> None:
        """Ejecuta el scraping en intervalos regulares"""
        self.logger.info("Iniciando proceso de scraping")
        self.logger.info(f"Instrumentos monitoreados: {', '.join(ScraperConfig.INSTRUMENTS.keys())}")
        self.logger.info(f"Intervalo de actualizacion: {ScraperConfig.UPDATE_INTERVAL_MINUTES} minutos")
        self.logger.info(f"Duracion total del proceso: {ScraperConfig.TOTAL_RUNTIME_HOURS} horas")
        self.logger.info(f"Ubicacion de archivos de datos: {ScraperConfig.DATA_DIR}")
        self.logger.info(f"Ubicacion de archivos de log: {ScraperConfig.LOGS_DIR}")

        try:
            for i in range(ScraperConfig.TOTAL_ITERATIONS):
                self.logger.info(f"Ciclo {i + 1}/{ScraperConfig.TOTAL_ITERATIONS}")
                self.run_scraping_cycle()

                if i < ScraperConfig.TOTAL_ITERATIONS - 1:
                    self.logger.info(
                        f"Esperando {ScraperConfig.UPDATE_INTERVAL_MINUTES} minutos para siguiente ciclo...")
                    time.sleep(ScraperConfig.UPDATE_INTERVAL_MINUTES * 60)

            self.logger.info("Proceso completado exitosamente")

        except KeyboardInterrupt:
            self.logger.warning("Proceso interrumpido por el usuario")
        except Exception as e:
            self.logger.critical(f"Error critico en el proceso: {str(e)}", exc_info=True)
            raise


if __name__ == "__main__":
    scraper = DataScraper()
    scraper.run()