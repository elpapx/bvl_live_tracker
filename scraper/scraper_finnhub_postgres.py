import finnhub
import pandas as pd
import numpy as np
import time
import os
import logging
import psycopg2
from psycopg2 import sql
from typing import Dict, List
from pathlib import Path
from datetime import datetime, timedelta


# Configuración centralizada
class ScraperConfig:
    # Configuración de PostgreSQL
    PG_HOST = "98.85.189.191"
    PG_PORT = 5432
    PG_USER = "bvl_user"
    PG_PASSWORD = "179fae82"
    PG_DATABASE = "bvl_monitor"

    # Configuración de Finnhub
    FINNHUB_API_KEY = "d0d1es9r01qm2sk7bvc0d0d1es9r01qm2sk7bvcg"

    # Configuración de rutas
    SCRIPT_DIR = Path(__file__).parent
    PROJECT_ROOT = SCRIPT_DIR.parent
    LOGS_DIR = PROJECT_ROOT / "logs"

    # Configuración de tiempo
    UPDATE_INTERVAL_MINUTES = 3
    TOTAL_RUNTIME_HOURS = 7
    TOTAL_ITERATIONS = (TOTAL_RUNTIME_HOURS * 60) // UPDATE_INTERVAL_MINUTES

    # Configuración de logging
    LOG_LEVEL = logging.INFO
    LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

    # Instrumentos a rastrear
    INSTRUMENTS = {
        "BAP": {
            "finnhub_fields": [
                "currentPrice", "previousClose", "open", "dayLow", "dayHigh",
                "bid", "dividendYield", "volume", "fiftyTwoWeekRange",
                "marketCap", "trailingPE"
            ]
        },
        "BRK-B": {
            "finnhub_fields": [
                "currentPrice", "previousClose", "open", "dayLow", "dayHigh",
                "bid", "dividendYield",  "volume", "fiftyTwoWeekRange",
                "marketCap", "trailingPE"
            ]
        },
        "ILF": {
            "finnhub_fields": [
                "currentPrice", "previousClose", "open", "dayLow", "dayHigh",
                "bid", "dividendYield", "volume", "fiftyTwoWeekRange",
                "marketCap", "trailingPE"
            ],
            "is_etf": True,
            "field_mapping": {"volume": "volumen"}  # Mapeo de Finnhub -> CSV
        }
    }


class DataScraper:
    def __init__(self):
        self._setup_directories()
        self._setup_logging()
        self.logger = logging.getLogger(__name__)
        self.conn = None
        self.table_columns = []

        # Inicializar el cliente Finnhub
        self.finnhub_client = finnhub.Client(api_key=ScraperConfig.FINNHUB_API_KEY)

        # Conectar a PostgreSQL y verificar la tabla
        self._connect_to_postgres()
        self._check_and_update_table()
        self._get_table_columns()

    def _setup_directories(self):
        """Crea los directorios necesarios si no existen"""
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

    def _connect_to_postgres(self):
        """Establece la conexión con la base de datos PostgreSQL"""
        try:
            self.conn = psycopg2.connect(
                host=ScraperConfig.PG_HOST,
                port=ScraperConfig.PG_PORT,
                user=ScraperConfig.PG_USER,
                password=ScraperConfig.PG_PASSWORD,
                database=ScraperConfig.PG_DATABASE
            )
            self.logger.info("Conexión establecida con PostgreSQL")
        except Exception as e:
            self.logger.error(f"Error al conectar a PostgreSQL: {str(e)}", exc_info=True)
            raise

    def _get_table_columns(self):
        """Obtiene las columnas existentes en la tabla stock_data"""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                            SELECT column_name
                            FROM information_schema.columns
                            WHERE table_name = 'stock_data'
                            ORDER BY ordinal_position
                            """)
                self.table_columns = [row[0] for row in cur.fetchall()]
                self.logger.info(f"Columnas en stock_data: {self.table_columns}")
        except Exception as e:
            self.logger.error(f"Error al obtener columnas de la tabla: {str(e)}", exc_info=True)

    def _check_and_update_table(self):
        """Verifica si la tabla existe y tiene todas las columnas necesarias"""
        try:
            with self.conn.cursor() as cur:
                # Verificar si la tabla existe
                cur.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'stock_data')")
                table_exists = cur.fetchone()[0]

                if not table_exists:
                    # Crear la tabla si no existe
                    self.logger.info("Creando tabla stock_data")
                    cur.execute("""
                                CREATE TABLE stock_data
                                (
                                    id                   SERIAL PRIMARY KEY,
                                    symbol               VARCHAR(10) NOT NULL,
                                    timestamp            TIMESTAMP   NOT NULL,
                                    current_price        NUMERIC(10, 2),
                                    previous_close       NUMERIC(10, 2),
                                    open                 NUMERIC(10, 2),
                                    day_low              NUMERIC(10, 2),
                                    day_high             NUMERIC(10, 2),
                                    bid                  NUMERIC(10, 2),
                                    dividend_yield       NUMERIC(8, 4),
                                    volume               BIGINT,
                                    fifty_two_week_range VARCHAR(30),
                                    market_cap           BIGINT,
                                    trailing_pe          NUMERIC(10, 2),
                                    earnings_growth      NUMERIC(8, 4),
                                    revenue_growth       NUMERIC(8, 4),
                                    gross_margins        NUMERIC(8, 4),
                                    ebitda_margins       NUMERIC(8, 4),
                                    operating_margins    NUMERIC(8, 4),
                                    financial_currency   VARCHAR(10),
                                    return_on_assets     NUMERIC(8, 4),
                                    return_on_equity     NUMERIC(8, 4),
                                    book_value           NUMERIC(10, 2),
                                    price_to_book        NUMERIC(8, 4),
                                    UNIQUE (symbol, timestamp)
                                );

                                CREATE INDEX idx_stock_data_symbol ON stock_data (symbol);
                                CREATE INDEX idx_stock_data_timestamp ON stock_data (timestamp);
                                CREATE INDEX idx_stock_data_symbol_timestamp ON stock_data (symbol, timestamp);
                                """)
                    self.conn.commit()
                else:
                    # Obtener las columnas existentes
                    cur.execute("""
                                SELECT column_name
                                FROM information_schema.columns
                                WHERE table_name = 'stock_data'
                                """)
                    existing_columns = [row[0] for row in cur.fetchall()]

                    # Definir columnas que deberían existir
                    expected_columns = {
                        'id': 'SERIAL PRIMARY KEY',
                        'symbol': 'VARCHAR(10) NOT NULL',
                        'timestamp': 'TIMESTAMP NOT NULL',
                        'current_price': 'NUMERIC(10, 2)',
                        'previous_close': 'NUMERIC(10, 2)',
                        'open': 'NUMERIC(10, 2)',
                        'day_low': 'NUMERIC(10, 2)',
                        'day_high': 'NUMERIC(10, 2)',
                        'bid': 'NUMERIC(10, 2)',
                        'dividend_yield': 'NUMERIC(8, 4)',
                        'volume': 'BIGINT',
                        'fifty_two_week_range': 'VARCHAR(30)',
                        'market_cap': 'BIGINT',
                        'trailing_pe': 'NUMERIC(10, 2)',
                        'earnings_growth': 'NUMERIC(8, 4)',
                        'revenue_growth': 'NUMERIC(8, 4)',
                        'gross_margins': 'NUMERIC(8, 4)',
                        'ebitda_margins': 'NUMERIC(8, 4)',
                        'operating_margins': 'NUMERIC(8, 4)',
                        'financial_currency': 'VARCHAR(10)',
                        'return_on_assets': 'NUMERIC(8, 4)',
                        'return_on_equity': 'NUMERIC(8, 4)',
                        'book_value': 'NUMERIC(10, 2)',
                        'price_to_book': 'NUMERIC(8, 4)'
                    }

                    # Añadir columnas faltantes
                    for col_name, col_type in expected_columns.items():
                        if col_name.lower() not in [col.lower() for col in existing_columns]:
                            if col_name != 'id':  # No intentar añadir id, que debería ser la clave primaria
                                self.logger.info(f"Añadiendo columna faltante: {col_name}")
                                cur.execute(f"ALTER TABLE stock_data ADD COLUMN {col_name} {col_type}")
                                self.conn.commit()

                    # Comprobar si existe el índice de unicidad
                    cur.execute("""
                                SELECT COUNT(*)
                                FROM pg_constraint
                                WHERE conrelid = 'stock_data'::regclass 
                        AND contype = 'u' 
                        AND array_to_string(conkey, ',') = '2,3' -- Asume que symbol es la segunda columna y timestamp la tercera
                                """)
                    if cur.fetchone()[0] == 0:
                        self.logger.info("Añadiendo restricción de unicidad para (symbol, timestamp)")
                        cur.execute(
                            "ALTER TABLE stock_data ADD CONSTRAINT stock_data_symbol_timestamp_key UNIQUE (symbol, timestamp)")
                        self.conn.commit()

                    # Comprobar si existen los índices
                    cur.execute(
                        "SELECT COUNT(*) FROM pg_indexes WHERE tablename = 'stock_data' AND indexname = 'idx_stock_data_symbol'")
                    if cur.fetchone()[0] == 0:
                        self.logger.info("Creando índice idx_stock_data_symbol")
                        cur.execute("CREATE INDEX idx_stock_data_symbol ON stock_data(symbol)")
                        self.conn.commit()

                    cur.execute(
                        "SELECT COUNT(*) FROM pg_indexes WHERE tablename = 'stock_data' AND indexname = 'idx_stock_data_timestamp'")
                    if cur.fetchone()[0] == 0:
                        self.logger.info("Creando índice idx_stock_data_timestamp")
                        cur.execute("CREATE INDEX idx_stock_data_timestamp ON stock_data(timestamp)")
                        self.conn.commit()

                    cur.execute(
                        "SELECT COUNT(*) FROM pg_indexes WHERE tablename = 'stock_data' AND indexname = 'idx_stock_data_symbol_timestamp'")
                    if cur.fetchone()[0] == 0:
                        self.logger.info("Creando índice idx_stock_data_symbol_timestamp")
                        cur.execute("CREATE INDEX idx_stock_data_symbol_timestamp ON stock_data(symbol, timestamp)")
                        self.conn.commit()

                self.logger.info("Tabla stock_data verificada y actualizada correctamente")
        except Exception as e:
            self.logger.error(f"Error al verificar/actualizar tabla: {str(e)}", exc_info=True)
            self.conn.rollback()
            raise

    def _map_finnhub_to_db_field(self, finnhub_field):
        """Mapea un campo de Finnhub a su equivalente en la base de datos"""
        mapping = {
            "currentPrice": "current_price",
            "previousClose": "previous_close",
            "dayLow": "day_low",
            "dayHigh": "day_high",
            "dividendYield": "dividend_yield",
            "fiftyTwoWeekRange": "fifty_two_week_range",
            "marketCap": "market_cap",
            "trailingPE": "trailing_pe",
            "earningsGrowth": "earnings_growth",
            "revenueGrowth": "revenue_growth",
            "grossMargins": "gross_margins",
            "ebitdaMargins": "ebitda_margins",
            "operatingMargins": "operating_margins",
            "financialCurrency": "financial_currency",
            "returnOnAssets": "return_on_assets",
            "returnOnEquity": "return_on_equity",
            "bookValue": "book_value",
            "priceToBook": "price_to_book"
        }

        return mapping.get(finnhub_field, finnhub_field.lower())

    def fetch_instrument_data(self, symbol: str, config: Dict) -> Dict:
        """Obtiene los datos del instrumento financiero usando Finnhub API"""
        try:
            self.logger.info(f"Iniciando scraping para {symbol} con Finnhub")
            data = {}
            field_mapping = config.get("field_mapping", {})

            # Obtener datos de la cotización
            quote = self.finnhub_client.quote(symbol)
            self.logger.debug(f"Quote data para {symbol}: {quote}")

            # Mapear datos de la cotización
            data["current_price"] = quote["c"]  # Precio actual
            data["previous_close"] = quote["pc"]  # Cierre anterior
            data["open"] = quote["o"]  # Precio de apertura
            data["day_high"] = quote["h"]  # Precio más alto del día
            data["day_low"] = quote["l"]  # Precio más bajo del día

            # Obtener datos del perfil de la empresa
            try:
                profile = self.finnhub_client.company_profile2(symbol=symbol)
                self.logger.debug(f"Profile data para {symbol}: {profile}")

                if profile:
                    data["financial_currency"] = profile.get("currency", "USD")
                    data["market_cap"] = profile.get("marketCapitalization", None)
                    if data["market_cap"]:
                        data["market_cap"] = int(data["market_cap"] * 1000000)  # Convertir a valor completo
            except Exception as e:
                self.logger.warning(f"Error al obtener perfil para {symbol}: {str(e)}")

            # Obtener datos financieros básicos
            try:
                financials = self.finnhub_client.company_basic_financials(symbol, 'all')
                self.logger.debug(f"Metrics disponibles para {symbol}: {list(financials.get('metric', {}).keys())}")

                metrics = financials.get("metric", {})

                # Mapear métricas a campos de la base de datos
                finnhub_to_db_mapping = {
                    "peNormalizedAnnual": "trailing_pe",
                    "dividendYieldIndicatedAnnual": "dividend_yield",
                    "roaRfy": "return_on_assets",
                    "roeRfy": "return_on_equity",
                    "grossMarginAnnual": "gross_margins",
                    "ebitdaMarginAnnual": "ebitda_margins",
                    "operatingMarginAnnual": "operating_margins",
                    "bookValuePerShareAnnual": "book_value",
                    "pbAnnual": "price_to_book"
                }

                for finnhub_key, db_field in finnhub_to_db_mapping.items():
                    if finnhub_key in metrics and db_field in self.table_columns:
                        data[db_field] = metrics[finnhub_key]

                # Extraer rango de 52 semanas
                if "52WeekHigh" in metrics and "52WeekLow" in metrics:
                    data["fifty_two_week_range"] = f"{metrics['52WeekLow']}-{metrics['52WeekHigh']}"

            except Exception as e:
                self.logger.warning(f"Error al obtener financials para {symbol}: {str(e)}")

            # Obtener datos de velas para el volumen y otros datos dinámicos
            try:
                end_time = int(datetime.now().timestamp())
                start_time = int((datetime.now() - timedelta(days=1)).timestamp())

                candles = self.finnhub_client.stock_candles(symbol, 'D', start_time, end_time)
                self.logger.debug(f"Candles data para {symbol}: {candles}")

                if candles["s"] == "ok" and len(candles["v"]) > 0:
                    volume_field = field_mapping.get("volume", "volume")
                    data[volume_field] = candles["v"][-1]

            except Exception as e:
                self.logger.warning(f"Error al obtener candles para {symbol}: {str(e)}")

            # Usar recomendaciones para obtener datos de crecimiento
            try:
                recommendations = self.finnhub_client.recommendation_trends(symbol)
                self.logger.debug(f"Recommendations para {symbol}: {recommendations}")

                # Nota: Finnhub no proporciona directamente datos de crecimiento
                # en su API gratuita, esto es solo un ejemplo de cómo se podría
                # obtener información adicional

            except Exception as e:
                self.logger.warning(f"Error al obtener recommendations para {symbol}: {str(e)}")

            # Campos comunes
            data["symbol"] = symbol
            data["timestamp"] = datetime.now()

            self.logger.info(f"Datos obtenidos con éxito para {symbol} desde Finnhub")
            return data

        except Exception as e:
            self.logger.error(f"Error al obtener datos de {symbol} desde Finnhub: {str(e)}", exc_info=True)
            return None

    def save_to_postgres(self, data: Dict) -> None:
        """Guarda los datos en la tabla stock_data de PostgreSQL"""
        if data is None:
            self.logger.warning("No hay datos para guardar")
            return

        try:
            # Filtrar solo las columnas que existen en la tabla
            filtered_data = {}
            for col in data.keys():
                if col in self.table_columns:
                    filtered_data[col] = data[col]

            if not filtered_data:
                self.logger.warning("No hay datos válidos para guardar después de filtrar por columnas existentes")
                return

            # Preparar columnas y valores para la inserción
            columns = list(filtered_data.keys())

            # Convertir valores numpy a tipos Python nativos
            values = []
            for col in columns:
                val = filtered_data[col]
                # Convertir tipos numpy a tipos Python nativos
                if hasattr(val, 'dtype') and 'numpy' in str(type(val)):
                    if np.issubdtype(val.dtype, np.integer):
                        val = int(val)
                    elif np.issubdtype(val.dtype, np.floating):
                        val = float(val)
                    elif np.issubdtype(val.dtype, np.bool_):
                        val = bool(val)
                # Manejo especial de NaN e infinitos
                if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
                    val = None
                values.append(val)

            # Crear sentencia SQL para inserción
            column_str = ", ".join(columns)
            placeholder_str = ", ".join(["%s"] * len(columns))

            # Generar la parte SET de la cláusula ON CONFLICT
            update_parts = []
            for col in columns:
                if col not in ["symbol", "timestamp"]:  # No actualizar las claves
                    update_parts.append(f"{col} = EXCLUDED.{col}")

            # Completar la consulta SQL
            if update_parts:
                insert_sql = f"""
                INSERT INTO stock_data ({column_str})
                VALUES ({placeholder_str})
                ON CONFLICT (symbol, timestamp) DO UPDATE SET {', '.join(update_parts)}
                """
            else:
                insert_sql = f"""
                INSERT INTO stock_data ({column_str})
                VALUES ({placeholder_str})
                ON CONFLICT (symbol, timestamp) DO NOTHING
                """

            with self.conn.cursor() as cur:
                cur.execute(insert_sql, values)

            self.conn.commit()
            self.logger.info(f"Datos guardados en tabla stock_data para {data['symbol']}")
            self.logger.debug(f"Último registro: {data['timestamp']}")

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error al guardar datos: {str(e)}", exc_info=True)

    def run_scraping_cycle(self) -> None:
        """Ejecuta un ciclo completo de scraping con control de tasa para Finnhub"""
        self.logger.info("Iniciando ciclo de scraping")
        for symbol, config in ScraperConfig.INSTRUMENTS.items():
            data = self.fetch_instrument_data(symbol, config)
            if data:
                self.save_to_postgres(data)
            # Agregar retardo para respetar los límites de tasa de Finnhub (60 llamadas por minuto)
            time.sleep(1)  # 1 segundo entre llamadas
        self.logger.info("Ciclo de scraping completado")

    def run(self) -> None:
        """Ejecuta el scraping en intervalos regulares"""
        self.logger.info("Iniciando proceso de scraping")
        self.logger.info(f"Instrumentos monitoreados: {', '.join(ScraperConfig.INSTRUMENTS.keys())}")
        self.logger.info(f"Intervalo de actualización: {ScraperConfig.UPDATE_INTERVAL_MINUTES} minutos")
        self.logger.info(f"Duración total del proceso: {ScraperConfig.TOTAL_RUNTIME_HOURS} horas")
        self.logger.info(f"Base de datos: {ScraperConfig.PG_DATABASE}")
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
        finally:
            # Cerrar la conexión a PostgreSQL al finalizar
            if self.conn is not None:
                self.conn.close()
                self.logger.info("Conexión a PostgreSQL cerrada")


if __name__ == "__main__":
    scraper = DataScraper()
    scraper.run()