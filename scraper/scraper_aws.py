import yfinance as yf
import pandas as pd
import time
import os
import logging
import boto3
from io import StringIO
from typing import Dict, List
from pathlib import Path
from datetime import datetime


# Configuración centralizada
class ScraperConfig:
    # Configuración de AWS
    AWS_REGION = "us-east-1"  # Cambia a tu región preferida
    S3_BUCKET_NAME = "bvl-monitor-data"  # Cambia al nombre de tu bucket

    # Configuración de rutas
    SCRIPT_DIR = Path(__file__).parent
    PROJECT_ROOT = SCRIPT_DIR.parent
    DATA_DIR = "data"  # Ahora será una carpeta en S3
    LOGS_DIR = PROJECT_ROOT / "logs"  # Logs se mantienen locales

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
                # Cambiar "volume" por "volumen" 👇
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
            # Añadir mapeo de campos especiales 👇
            "field_mapping": {"volume": "volumen"}  # Mapeo de Yahoo Finance -> CSV
        }
    }


class DataScraper:
    def __init__(self):
        self._setup_directories()
        self._setup_logging()
        self.logger = logging.getLogger(__name__)

        # Inicializar cliente S3
        self.s3_client = boto3.client(
            's3',
            region_name=ScraperConfig.AWS_REGION
        )

        # Verificar existencia del bucket
        self._verify_s3_bucket()

    def _setup_directories(self):
        """Crea los directorios necesarios si no existen"""
        # Solo creamos directorios locales para logs
        os.makedirs(ScraperConfig.LOGS_DIR, exist_ok=True)

    def _verify_s3_bucket(self):
        """Verifica que el bucket de S3 exista, lo crea si no existe"""
        try:
            self.s3_client.head_bucket(Bucket=ScraperConfig.S3_BUCKET_NAME)
            self.logger.info(f"Bucket {ScraperConfig.S3_BUCKET_NAME} verificado correctamente")
        except Exception as e:
            if "404" in str(e):
                self.logger.info(f"Bucket {ScraperConfig.S3_BUCKET_NAME} no existe, creándolo...")
                try:
                    self.s3_client.create_bucket(
                        Bucket=ScraperConfig.S3_BUCKET_NAME,
                        CreateBucketConfiguration={'LocationConstraint': ScraperConfig.AWS_REGION}
                    )
                    self.logger.info(f"Bucket {ScraperConfig.S3_BUCKET_NAME} creado exitosamente")
                except Exception as create_err:
                    self.logger.error(f"Error al crear bucket: {str(create_err)}")
                    raise
            else:
                self.logger.error(f"Error al verificar bucket: {str(e)}")
                raise

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

    def _check_file_exists_in_s3(self, filepath):
        """Verifica si un archivo existe en S3"""
        try:
            self.s3_client.head_object(
                Bucket=ScraperConfig.S3_BUCKET_NAME,
                Key=filepath
            )
            return True
        except Exception:
            return False

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

    def save_to_csv(self, data: Dict, filename: str) -> None:
        """Guarda los datos en CSV en S3"""
        if data is None:
            self.logger.warning("No hay datos para guardar")
            return

        try:
            # Ruta del archivo en S3
            s3_key = f"{ScraperConfig.DATA_DIR}/{filename}"

            # Crear dataframe con los nuevos datos
            new_df = pd.DataFrame([data])

            # Verificar si el archivo ya existe en S3
            if self._check_file_exists_in_s3(s3_key):
                # Leer el archivo existente de S3
                response = self.s3_client.get_object(
                    Bucket=ScraperConfig.S3_BUCKET_NAME,
                    Key=s3_key
                )
                existing_df = pd.read_csv(response['Body'])

                # Concatenar con los nuevos datos
                df = pd.concat([existing_df, new_df], ignore_index=True)
                self.logger.info(f"Datos añadidos a {filename} en S3")
            else:
                # Crear nuevo archivo
                df = new_df
                self.logger.info(f"Archivo {filename} creado exitosamente en S3")

            # Convertir el dataframe a un buffer CSV en memoria
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)

            # Subir el buffer a S3
            self.s3_client.put_object(
                Bucket=ScraperConfig.S3_BUCKET_NAME,
                Key=s3_key,
                Body=csv_buffer.getvalue()
            )

            self.logger.debug(f"Último registro: {data['timestamp']}")

        except Exception as e:
            self.logger.error(f"Error al guardar datos en {filename} en S3: {str(e)}", exc_info=True)

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
        self.logger.info(f"Intervalo de actualización: {ScraperConfig.UPDATE_INTERVAL_MINUTES} minutos")
        self.logger.info(f"Duración total del proceso: {ScraperConfig.TOTAL_RUNTIME_HOURS} horas")
        self.logger.info(f"Bucket S3: {ScraperConfig.S3_BUCKET_NAME}")
        self.logger.info(f"Carpeta S3: {ScraperConfig.DATA_DIR}")
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
    scraper = DataScraper()
    scraper.run()