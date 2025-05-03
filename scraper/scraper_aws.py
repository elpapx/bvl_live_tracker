import yfinance as yf
import pandas as pd
import time
import os
import logging
import boto3
import io
from typing import Dict, List
from pathlib import Path
from datetime import datetime


# Configuración centralizada
class ScraperConfig:
    # Configuración de rutas
    SCRIPT_DIR = Path(__file__).parent
    PROJECT_ROOT = SCRIPT_DIR.parent
    LOCAL_DATA_DIR = PROJECT_ROOT / "data"  # Mantener directorio local como respaldo
    LOGS_DIR = PROJECT_ROOT / "logs"

    # Configuración de tiempo
    UPDATE_INTERVAL_MINUTES = 1
    TOTAL_RUNTIME_HOURS = 7
    TOTAL_ITERATIONS = (TOTAL_RUNTIME_HOURS * 60) // UPDATE_INTERVAL_MINUTES

    # Configuración de logging
    LOG_LEVEL = logging.INFO
    LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

    # Configuración de Amazon S3
    S3_BUCKET = "nombre-de-tu-bucket"
    S3_PREFIX = "financial-data/"  # Prefijo para organizar los archivos en S3

    # Instrumentos a rastrear
    INSTRUMENTS = {
        "BAP": {
            "columns": [
                "currentPrice", "previousClose", "open", "dayLow", "dayHigh",
                "bid", "dividendYield", "earningsGrowth", "revenueGrowth",
                "grossMargins", "ebitdaMargins", "operatingMargins", "financialCurrency",
                "returnOnAssets", "returnOnEquity", "bookValue", "priceToBook", 'volume',
                'fiftyTwoWeekRange', 'marketCap', 'trailingPE'
            ],
            "filename": "bap_stock_data.csv"
        },
        "BRK-B": {
            "columns": [
                "currentPrice", "previousClose", "open", "dayLow", "dayHigh",
                "bid", "dividendYield", "earningsGrowth", "revenueGrowth",
                "grossMargins", "ebitdaMargins", "operatingMargins", "financialCurrency",
                "returnOnAssets", "returnOnEquity", "bookValue", "priceToBook", 'volume',
                'fiftyTwoWeekRange', 'marketCap', 'trailingPE'
            ],
            "filename": "brk-b_stock_data.csv"
        },
        "ILF": {
            "columns": [
                "currentPrice",
                "previousClose",
                "open",
                "dayLow",
                "dayHigh",
                "volumen",  # Nombre personalizado para el CSV
                "regularMarketVolume",
                "averageVolume",
                "averageVolume10days",
                'fiftyTwoWeekRange',
                "bid",
                "ask",
                "dividendYield",
                'marketCap',
                'trailingPE'
            ],
            "filename": "ilf_etf_data.csv",
            "is_etf": True,
            "field_mapping": {"volume": "volumen"}  # Mapeo de Yahoo Finance -> CSV
        }
    }


class S3DataScraper:
    def __init__(self):
        self._setup_directories()
        self._setup_logging()
        self.logger = logging.getLogger(__name__)
        self._setup_s3_client()

    def _setup_directories(self):
        """Crea los directorios necesarios si no existen"""
        os.makedirs(ScraperConfig.LOCAL_DATA_DIR, exist_ok=True)
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

    def _setup_s3_client(self):
        """Inicializa el cliente de AWS S3"""
        try:
            self.s3_client = boto3.client('s3')
            self.logger.info("Cliente de S3 inicializado correctamente")
        except Exception as e:
            self.logger.error(f"Error al inicializar cliente S3: {str(e)}", exc_info=True)
            raise

    def fetch_instrument_data(self, symbol: str, config: Dict) -> Dict:
        """Obtiene los datos del instrumento financiero"""
        try:
            self.logger.info(f"Iniciando scraping para {symbol}")
            ticker = yf.Ticker(symbol)
            data = {}
            field_mapping = config.get("field_mapping", {})

            if config.get("is_etf", False):
                self.logger.debug(f"{symbol} es un ETF, usando metodo especial")
                history = ticker.history(period="1d")

                # 1. Obtener currentPrice desde el historial
                data["currentPrice"] = history["Close"].iloc[-1] if not history.empty else None
                self.logger.debug(f"Precio actual obtenido: {data['currentPrice']}")

                # 2. Procesar el volumen desde el historial (si está configurado)
                if "volumen" in config["columns"] or "volume" in config["columns"]:
                    data["volumen"] = history["Volume"].iloc[-1] if not history.empty else None
                    self.logger.debug(f"Volumen obtenido: {data['volumen']}")

                # 3. Procesar el resto de campos desde ticker.info
                info = ticker.info
                for key in config["columns"]:
                    if key in ["currentPrice", "volumen"]:  # Campos ya asignados
                        continue

                    # Mapeo CORRECTO: Yahoo Finance → CSV
                    csv_key = key  # Ej: "dividendYield"
                    yahoo_key = field_mapping.get(key, key)  # Ej: "volume" → "volumen"

                    data[csv_key] = info.get(yahoo_key, None)
                    self.logger.debug(f"{csv_key} (desde Yahoo '{yahoo_key}'): {data[csv_key]}")

            else:  # Para acciones (no-ETF)
                self.logger.debug(f"{symbol} es una accion, usando metodo estandar")
                info = ticker.info
                for key in config["columns"]:
                    data[key] = info.get(key, None)
                    self.logger.debug(f"{key}: {data[key]}")

            # Campos comunes
            data["symbol"] = symbol
            data["timestamp"] = pd.Timestamp.now()
            return data

        except Exception as e:
            self.logger.error(f"Error al obtener datos de {symbol}: {str(e)}", exc_info=True)
            return None

    def _read_existing_from_s3(self, s3_key):
        """Lee un archivo CSV existente desde S3"""
        try:
            response = self.s3_client.get_object(Bucket=ScraperConfig.S3_BUCKET, Key=s3_key)
            return pd.read_csv(io.BytesIO(response['Body'].read()))
        except self.s3_client.exceptions.NoSuchKey:
            self.logger.info(f"Archivo no encontrado en S3: {s3_key}. Se creará uno nuevo.")
            return None
        except Exception as e:
            self.logger.error(f"Error al leer archivo de S3 {s3_key}: {str(e)}", exc_info=True)
            return None

    def save_to_s3(self, data: Dict, filename: str) -> None:
        """Guarda los datos en S3 y opcionalmente en el sistema de archivos local"""
        if data is None:
            self.logger.warning("No hay datos para guardar")
            return

        try:
            # Crear DataFrame con los nuevos datos
            new_data_df = pd.DataFrame([data])

            # Ruta S3 completa
            s3_key = f"{ScraperConfig.S3_PREFIX}{filename}"

            # Verificar si el archivo ya existe en S3
            existing_df = self._read_existing_from_s3(s3_key)

            if existing_df is not None:
                # Concatenar datos existentes con nuevos
                combined_df = pd.concat([existing_df, new_data_df], ignore_index=True)
                self.logger.info(f"Añadiendo datos a archivo existente en S3: {s3_key}")
            else:
                # Usar solo los nuevos datos
                combined_df = new_data_df
                self.logger.info(f"Creando nuevo archivo en S3: {s3_key}")

            # Convertir DataFrame a CSV en memoria
            csv_buffer = io.StringIO()
            combined_df.to_csv(csv_buffer, index=False)

            # Subir a S3
            self.s3_client.put_object(
                Bucket=ScraperConfig.S3_BUCKET,
                Key=s3_key,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            self.logger.info(f"Datos guardados exitosamente en S3: {s3_key}")

            # También guardar localmente como respaldo
            local_filepath = ScraperConfig.LOCAL_DATA_DIR / filename
            combined_df.to_csv(local_filepath, index=False)
            self.logger.debug(f"Respaldo local guardado en: {local_filepath}")

            self.logger.debug(f"Último registro: {data['timestamp']}")

        except Exception as e:
            self.logger.error(f"Error al guardar datos en S3 para {filename}: {str(e)}", exc_info=True)

            # Intentar guardar localmente en caso de error con S3
            try:
                filepath = ScraperConfig.LOCAL_DATA_DIR / filename
                df = pd.DataFrame([data])

                if filepath.exists():
                    df.to_csv(filepath, mode='a', header=False, index=False)
                else:
                    df.to_csv(filepath, mode='w', header=True, index=False)

                self.logger.info(f"Datos guardados localmente como respaldo en: {filepath}")
            except Exception as local_err:
                self.logger.error(f"Error también al guardar localmente: {str(local_err)}", exc_info=True)

    def run_scraping_cycle(self) -> None:
        """Ejecuta un ciclo completo de scraping"""
        self.logger.info("Iniciando ciclo de scraping")
        for symbol, config in ScraperConfig.INSTRUMENTS.items():
            data = self.fetch_instrument_data(symbol, config)
            if data:
                self.save_to_s3(data, config["filename"])
        self.logger.info("Ciclo de scraping completado")

    def run(self) -> None:
        """Ejecuta el scraping en intervalos regulares"""
        self.logger.info("Iniciando proceso de scraping con almacenamiento en S3")
        self.logger.info(f"Bucket S3: {ScraperConfig.S3_BUCKET}")
        self.logger.info(f"Prefijo S3: {ScraperConfig.S3_PREFIX}")
        self.logger.info(f"Instrumentos monitoreados: {', '.join(ScraperConfig.INSTRUMENTS.keys())}")
        self.logger.info(f"Intervalo de actualización: {ScraperConfig.UPDATE_INTERVAL_MINUTES} minutos")
        self.logger.info(f"Duración total del proceso: {ScraperConfig.TOTAL_RUNTIME_HOURS} horas")
        self.logger.info(f"Ubicación de respaldos locales: {ScraperConfig.LOCAL_DATA_DIR}")
        self.logger.info(f"Ubicación de archivos de log: {ScraperConfig.LOGS_DIR}")

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
            self.logger.critical(f"Error crítico en el proceso: {str(e)}", exc_info=True)
            raise


if __name__ == "__main__":
    scraper = S3DataScraper()
    scraper.run()