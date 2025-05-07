import time
import logging
import os
import schedule
import sys
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent
import time
import json
import re
import os
import psycopg2
from datetime import datetime
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"stock_scraper_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("stock_scraper")


# Database Configuration
class ScraperConfig:
    # PostgreSQL Configuration
    PG_HOST = "localhost"
    PG_PORT = 5432
    PG_USER = "bvl_user"
    PG_PASSWORD = "179fae82"
    PG_DATABASE = "bvl_monitor"

    # Retry Configuration
    MAX_RETRIES = 3
    RETRY_DELAY = 5  # seconds

    # Web Scraping Configuration
    METRICS_TO_EXTRACT = [
        "Symbol",
        "Current Price",
        "Previous Close",
        "Open",
        "Day's Range",
        "52 Week Range",
        "Volume",
        "PE Ratio (TTM)",
        "Yield"
    ]

    # Mapping from Yahoo Finance metrics to database columns
    METRICS_TO_DB_MAPPING = {
        "Symbol": "symbol",
        "Current Price": "current_price",
        "Previous Close": "previous_close",
        "Open": "open",
        "Day's Range": "day_range",  # Will need special processing
        "52 Week Range": "fifty_two_week_range",
        "Volume": "volume",
        "PE Ratio (TTM)": "trailing_pe",
        "Yield": "dividend_yield"  # Will need special processing
    }

    # Symbols to scrape
    SYMBOLS_TO_SCRAPE = ["ILF", "BAP", "BRK-B"]

    # Scheduling configuration
    SCHEDULE_INTERVAL_MINUTES = 10
    RESULTS_DIR = "yahoo_finance_results"


def setup_driver():
    """Configura y devuelve un driver de Chrome para Selenium"""
    # Generate random user agent
    ua = UserAgent()
    user_agent = ua.chrome

    # Configure Chrome options
    options = Options()
    options.add_argument(f"user-agent={user_agent}")
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # Performance optimizations
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    options.add_argument("--blink-settings=imagesEnabled=false")

    return webdriver.Chrome(options=options)


def get_db_connection():
    """
    Establishes a connection to the PostgreSQL database with retry mechanism

    Returns:
        psycopg2.connection: Database connection object
    """
    retry_count = 0

    while retry_count < ScraperConfig.MAX_RETRIES:
        try:
            conn = psycopg2.connect(
                host=ScraperConfig.PG_HOST,
                port=ScraperConfig.PG_PORT,
                user=ScraperConfig.PG_USER,
                password=ScraperConfig.PG_PASSWORD,
                database=ScraperConfig.PG_DATABASE
            )
            logger.info("Successfully connected to PostgreSQL database")
            return conn
        except Exception as e:
            retry_count += 1
            logger.error(f"Database connection error (attempt {retry_count}/{ScraperConfig.MAX_RETRIES}): {str(e)}")
            if retry_count < ScraperConfig.MAX_RETRIES:
                logger.info(f"Retrying in {ScraperConfig.RETRY_DELAY} seconds...")
                time.sleep(ScraperConfig.RETRY_DELAY)

    raise Exception("Failed to connect to the database after multiple attempts")


def preprocess_data_for_db(metrics: Dict) -> Dict:
    """
    Preprocesses the extracted metrics to match the database schema

    Args:
        metrics (Dict): Raw metrics from Yahoo Finance

    Returns:
        Dict: Processed data ready for insertion into the database
    """
    db_data = {}

    # First add the timestamp
    db_data["timestamp"] = datetime.now()

    # Then map and process each field
    for yahoo_metric, db_field in ScraperConfig.METRICS_TO_DB_MAPPING.items():
        value = metrics.get(yahoo_metric, "N/A")

        # Skip if value is N/A
        if value == "N/A":
            continue

        # Process based on the type of field
        if db_field == "symbol":
            db_data[db_field] = value

        elif db_field in ["current_price", "previous_close", "open", "trailing_pe"]:
            try:
                # Remove any non-numeric characters except decimal point
                numeric_value = re.sub(r'[^\d.]', '', value)
                db_data[db_field] = float(numeric_value) if numeric_value else None
            except (ValueError, TypeError):
                logger.warning(f"Could not convert {yahoo_metric} value '{value}' to float")

        elif db_field == "volume":
            try:
                # Remove commas and convert to integer
                numeric_value = re.sub(r'[^\d]', '', value)
                db_data[db_field] = int(numeric_value) if numeric_value else None
            except (ValueError, TypeError):
                logger.warning(f"Could not convert {yahoo_metric} value '{value}' to integer")

        elif db_field == "dividend_yield":
            try:
                # Extract percentage but store as percentage value (not decimal)
                match = re.search(r'(\d+\.?\d*)%', value)
                if match:
                    # Store as percentage value (e.g. 5.26 instead of 0.0526)
                    db_data[db_field] = float(match.group(1))
                    logger.info(f"Almacenando dividend yield como porcentaje: {db_data[db_field]}")
            except (ValueError, TypeError):
                logger.warning(f"Could not convert {yahoo_metric} value '{value}' to decimal")

        elif db_field == "fifty_two_week_range":
            # Store as string
            db_data[db_field] = value

        elif db_field == "day_range":
            # Extract low and high values
            match = re.search(r'(\d+\.?\d*)\s*[-–]\s*(\d+\.?\d*)', value)
            if match:
                try:
                    db_data["day_low"] = float(match.group(1))
                    db_data["day_high"] = float(match.group(2))
                except (ValueError, TypeError):
                    logger.warning(f"Could not extract day_low and day_high from '{value}'")

    # Add financial_currency field (default to USD)
    db_data["financial_currency"] = "USD"

    return db_data


def insert_stock_data(conn, data: Dict) -> bool:
    """
    Inserts or updates stock data in the database

    Args:
        conn: Database connection
        data (Dict): Processed data to insert

    Returns:
        bool: True if successful, False otherwise
    """
    # Ensure the required fields are present
    required_fields = ["symbol", "timestamp"]
    for field in required_fields:
        if field not in data:
            logger.error(f"Required field '{field}' missing from data")
            return False

    try:
        with conn.cursor() as cursor:
            # Prepare column names and values
            columns = list(data.keys())
            values = [data[col] for col in columns]

            # Create placeholders for values
            placeholders = ', '.join(['%s'] * len(columns))

            # Create column names string
            col_str = ', '.join(columns)

            # Create SET part for ON CONFLICT
            set_parts = []
            for col in columns:
                if col not in required_fields:  # Don't update the key fields
                    set_parts.append(f"{col} = EXCLUDED.{col}")

            set_clause = ', '.join(set_parts)

            # Build the query
            query = f"""
                INSERT INTO stock_data ({col_str})
                VALUES ({placeholders})
                ON CONFLICT (symbol, timestamp) 
                DO UPDATE SET {set_clause}
            """

            # Execute the query
            cursor.execute(query, values)
            conn.commit()

            logger.info(f"Successfully inserted/updated data for {data['symbol']}")
            return True

    except Exception as e:
        conn.rollback()
        logger.error(f"Database insertion error: {str(e)}")
        return False


def extract_stock_metrics(symbol, driver):
    """
    Extrae métricas específicas de una acción en Yahoo Finance

    Args:
        symbol (str): El símbolo de la acción
        driver (WebDriver): El driver de Selenium

    Returns:
        dict: Las métricas extraídas
    """
    metrics = {"Symbol": symbol}

    try:
        # Cargar la página con timeout optimizado
        url = f"https://finance.yahoo.com/quote/{symbol}/"
        driver.get(url)

        # Esperar a que el precio se cargue (elemento principal)
        wait = WebDriverWait(driver, 10)
        price_element = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'span[data-testid="qsp-price"]'))
        )

        # 1. Extraer el precio actual
        metrics["Current Price"] = price_element.text.strip()

        # 2. Obtener el HTML de la página
        html_content = driver.page_source

        # PARTE 1: Usar los patrones regex originales para extraer métricas básicas
        # 2.1 Previous Close y Open
        previous_close_match = re.search(r'Previous Close</span>.*?<span.*?>(\d+\.\d+)', html_content)
        if previous_close_match:
            metrics["Previous Close"] = previous_close_match.group(1)

        open_match = re.search(r'Open</span>.*?<span.*?>(\d+\.\d+)', html_content)
        if open_match:
            metrics["Open"] = open_match.group(1)

        # 2.2 Day's Range y 52 Week Range
        days_range_match = re.search(r'Day&#x27;s Range</span>.*?<span.*?>(\d+\.\d+\s*-\s*\d+\.\d+)', html_content) or \
                           re.search(r'Day&#39;s Range</span>.*?<span.*?>(\d+\.\d+\s*-\s*\d+\.\d+)', html_content) or \
                           re.search(r'Day[^<>]+Range</span>.*?<span.*?>(\d+\.\d+\s*[-–]\s*\d+\.\d+)', html_content)
        if days_range_match:
            metrics["Day's Range"] = days_range_match.group(1).replace('&#x27;', "'")

        week_range_match = re.search(r'52 Week Range</span>.*?<span.*?>(\d+\.\d+\s*-\s*\d+\.\d+)', html_content) or \
                           re.search(r'52[-\s]Week Range</span>.*?<span.*?>(\d+\.\d+\s*[-–]\s*\d+\.\d+)', html_content)
        if week_range_match:
            metrics["52 Week Range"] = week_range_match.group(1)

        #
        #
        volume_patterns = [
            r'Volume</span>.*?<span.*?>([\d,\.]+)',
            r'Volume</span>.*?<fin-streamer.*?>([\d,\.]+)',
            r'data-test="VOLUME-value".*?>([\d,\.]+)',
            r'Volume.*?>([0-9,\.]+)'
        ]

        for pattern in volume_patterns:
            match = re.search(pattern, html_content)
            if match:
                metrics["Volume"] = match.group(1).strip()
                logger.debug(f"Encontrado Volume (regex): {metrics['Volume']}")
                break

        #
        if "Volume" not in metrics or not metrics["Volume"]:
            volume_selectors = [
                "//td[contains(text(), 'Volume')]/following-sibling::td",
                "//span[contains(text(), 'Volume')]/parent::*/following-sibling::*",
                "//span[contains(text(), 'Volume')]/following-sibling::span",
                "//*[@data-test='VOLUME-value']",
                "//td[text()='Volume']/following-sibling::td"
            ]

            for selector in volume_selectors:
                try:
                    elements = driver.find_elements(By.XPATH, selector)
                    if elements and elements[0].text.strip():
                        volume_text = elements[0].text.strip()
                        if re.search(r'\d', volume_text):  # Asegurar que contiene dígitos
                            metrics["Volume"] = volume_text
                            logger.debug(f"Encontrado Volume (XPath): {metrics['Volume']}")
                            break
                except:
                    continue

        #
        if "Volume" not in metrics or not metrics["Volume"]:
            try:
                tables = driver.find_elements(By.TAG_NAME, "table")
                for table in tables:
                    rows = table.find_elements(By.TAG_NAME, "tr")
                    for row in rows:
                        cells = row.find_elements(By.TAG_NAME, "td")
                        if len(cells) >= 2 and cells[0].text.strip() == "Volume":
                            volume_text = cells[1].text.strip()
                            metrics["Volume"] = volume_text
                            logger.debug(f"Encontrado Volume (tabla): {metrics['Volume']}")
                            break
            except:
                pass

        # PARTE 3: MÉTODOS MEJORADOS PARA PE RATIO Y YIELD

        #
        # Esta es la forma más confiable de obtener estos valores
        quote_summary_divs = driver.find_elements(By.CSS_SELECTOR, 'div#quote-summary')
        if quote_summary_divs:
            summary_tables = quote_summary_divs[0].find_elements(By.TAG_NAME, 'table')
            for table in summary_tables:
                rows = table.find_elements(By.TAG_NAME, 'tr')
                for row in rows:
                    try:
                        cells = row.find_elements(By.TAG_NAME, 'td')
                        if len(cells) >= 2:
                            header = cells[0].text.strip()
                            value = cells[1].text.strip()

                            # Extraer PE Ratio
                            if "PE Ratio" in header:
                                # Si es un número válido, guardarlo
                                pe_match = re.search(r'(\d+\.\d+)', value)
                                if pe_match:
                                    metrics["PE Ratio (TTM)"] = pe_match.group(1)
                                    logger.debug(f"Encontrado PE Ratio (tabla): {metrics['PE Ratio (TTM)']}")

                            # Extraer Yield
                            if "Forward Dividend & Yield" in header:
                                # Extraer solo el porcentaje
                                yield_match = re.search(r'(\d+\.?\d*%)', value)
                                if yield_match:
                                    metrics["Yield"] = yield_match.group(1)
                                    logger.debug(f"Encontrado Yield (tabla): {metrics['Yield']}")
                                elif symbol == "BRK-B":  # Caso especial para BRK-B
                                    metrics["Yield"] = "N/A"
                                    logger.debug(f"BRK-B no paga dividendos, estableciendo Yield: N/A")
                    except Exception as e:
                        logger.warning(f"Error procesando fila: {str(e)}")
                        continue

        #
        if "PE Ratio (TTM)" not in metrics or not metrics["PE Ratio (TTM)"]:
            # B.1: Intentar con selectores específicos
            pe_selectors = [
                "//td[contains(text(), 'PE Ratio')]/following-sibling::td",
                "//span[contains(text(), 'PE Ratio')]/../../following-sibling::td/span",
                "//span[contains(text(), 'P/E')]/../../following-sibling::td/span",
                "//*[@data-test='PE_RATIO-value']"
            ]

            for selector in pe_selectors:
                try:
                    elements = driver.find_elements(By.XPATH, selector)
                    if elements and elements[0].text.strip():
                        value = elements[0].text.strip()
                        # Validar que es un número
                        pe_match = re.search(r'(\d+\.\d+)', value)
                        if pe_match:
                            metrics["PE Ratio (TTM)"] = pe_match.group(1)
                            logger.debug(f"Encontrado PE Ratio (XPath): {metrics['PE Ratio (TTM)']}")
                            break
                except:
                    continue

            # B.2: Intentar con patrones avanzados en HTML
            if "PE Ratio (TTM)" not in metrics or not metrics["PE Ratio (TTM)"]:
                pe_patterns = [
                    r'PE Ratio\s*\(TTM\)</span>.*?<span[^>]*>(\d+\.\d+)',
                    r'"PE_RATIO-value"[^>]*>(\d+\.\d+)',
                    r'P/E Ratio.*?<span[^>]*>(\d+\.\d+)',
                    r'PE Ratio.*?data-value="(\d+\.\d+)"',
                    r'PE Ratio.*?fin-streamer[^>]*>(\d+\.\d+)'
                ]

                for pattern in pe_patterns:
                    match = re.search(pattern, html_content)
                    if match:
                        metrics["PE Ratio (TTM)"] = match.group(1)
                        logger.debug(f"Encontrado PE Ratio (regex): {metrics['PE Ratio (TTM)']}")
                        break

        # Corrección específica para PE Ratio: evitar que sea el mismo valor que Current Price
        if "PE Ratio (TTM)" in metrics and "Current Price" in metrics and metrics["PE Ratio (TTM)"] == metrics[
            "Current Price"]:
            logger.warning(f"Corrigiendo PE Ratio (tiene el mismo valor que Current Price)")
            # Intentar nuevamente con métodos alternativos
            pe_patterns = [
                r'PE Ratio\s*\(TTM\)[^<>]*?[^0-9](\d+\.\d+)',
                r'"PE_RATIO-value"[^>]*?[^0-9](\d+\.\d+)'
            ]

            for pattern in pe_patterns:
                match = re.search(pattern, html_content)
                if match and match.group(1) != metrics["Current Price"]:
                    metrics["PE Ratio (TTM)"] = match.group(1)
                    logger.debug(f"Valor corregido de PE Ratio: {metrics['PE Ratio (TTM)']}")
                    break

            # Si sigue igual, marcar como N/A
            if metrics["PE Ratio (TTM)"] == metrics["Current Price"]:
                metrics["PE Ratio (TTM)"] = "N/A"
                logger.debug("No se pudo determinar PE Ratio, establecido a N/A")

        #
        if "Yield" not in metrics or not metrics["Yield"]:
            # C.1: Si es BRK-B, asignar N/A directamente
            if symbol == "BRK-B":
                metrics["Yield"] = "N/A"
                logger.debug("BRK-B no paga dividendos, asignando N/A")
            else:
                # C.2: Intentar con selectores específicos para otros símbolos
                yield_selectors = [
                    "//td[contains(text(), 'Forward Dividend & Yield')]/following-sibling::td",
                    "//span[contains(text(), 'Forward Dividend & Yield')]/../../following-sibling::td/span",
                    "//span[contains(text(), 'Yield')]/../../following-sibling::td/span",
                    "//*[@data-test='YIELD-value']"
                ]

                for selector in yield_selectors:
                    try:
                        elements = driver.find_elements(By.XPATH, selector)
                        if elements and elements[0].text.strip():
                            value = elements[0].text.strip()
                            # Extraer el porcentaje
                            yield_match = re.search(r'(\d+\.?\d*%)', value)
                            if yield_match:
                                metrics["Yield"] = yield_match.group(1)
                                logger.debug(f"Encontrado Yield (XPath): {metrics['Yield']}")
                                break
                    except:
                        continue

                # C.3: Intentar con patrones avanzados en HTML
                if "Yield" not in metrics or not metrics["Yield"]:
                    yield_patterns = [
                        r'Forward Dividend[^>]*Yield</span>.*?<span[^>]*>(.*?\d+\.?\d*%)',
                        r'Yield \(.*?\)</span>.*?<span[^>]*>(\d+\.?\d*%)',
                        r'"YIELD-value"[^>]*>(.*?\d+\.?\d*%)',
                        r'Dividend.*?<span[^>]*>.*?(\d+\.?\d*%)',
                        r'Yield.*?data-value="(\d+\.?\d*%)"'
                    ]

                    for pattern in yield_patterns:
                        match = re.search(pattern, html_content)
                        if match:
                            # Asegúrate de que el valor extraído contiene un porcentaje
                            extracted_value = match.group(1)
                            if "%" in extracted_value:
                                # Extraer solo el porcentaje
                                percent_match = re.search(r'(\d+\.?\d*%)', extracted_value)
                                if percent_match:
                                    metrics["Yield"] = percent_match.group(1)
                                    logger.debug(f"Encontrado Yield (regex): {metrics['Yield']}")
                                    break

    except Exception as e:
        logger.error(f"Error extrayendo métricas para {symbol}: {str(e)}")

    # Limpiar valores y asegurar que todos los campos estén presentes
    for field in ScraperConfig.METRICS_TO_EXTRACT:
        if field not in metrics or not metrics[field]:
            metrics[field] = "N/A"

    return metrics


def format_metrics_table(metrics_list):
    """
    Formatea una lista de diccionarios de métricas en una tabla para mostrar en consola

    Args:
        metrics_list (list): Lista de diccionarios con métricas de diferentes acciones

    Returns:
        str: Tabla formateada
    """
    # Determinar el ancho máximo para cada columna
    widths = {}
    for metric in ScraperConfig.METRICS_TO_EXTRACT:
        widths[metric] = len(metric)
        for stock_data in metrics_list:
            if metric in stock_data:
                widths[metric] = max(widths[metric], len(str(stock_data[metric])))

    # Crear encabezados
    header = " | ".join([f"{metric:<{widths[metric]}}" for metric in ScraperConfig.METRICS_TO_EXTRACT])
    separator = "-" * len(header)

    # Crear filas
    rows = []
    for stock_data in metrics_list:
        row = []
        for metric in ScraperConfig.METRICS_TO_EXTRACT:
            value = stock_data.get(metric, "N/A")
            row.append(f"{value:<{widths[metric]}}")
        rows.append(" | ".join(row))

    # Combinar
    return f"{header}\n{separator}\n" + "\n".join(rows)


def run_scraping_job():
    """Run the scraping job"""
    logger.info("Starting scheduled scraping job...")

    # Create directory for results if it doesn't exist
    os.makedirs(ScraperConfig.RESULTS_DIR, exist_ok=True)

    # Timestamp for files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Initialize the driver and extract metrics
    driver = None
    conn = None
    all_metrics = []

    try:
        # Initialize the WebDriver
        driver = setup_driver()

        # Connect to the database
        conn = get_db_connection()

        logger.info(f"Extrayendo métricas para: {', '.join(ScraperConfig.SYMBOLS_TO_SCRAPE)}")
        start_time = time.time()

        for symbol in ScraperConfig.SYMBOLS_TO_SCRAPE:
            try:
                # Extract metrics from Yahoo Finance
                metrics = extract_stock_metrics(symbol, driver)
                all_metrics.append(metrics)

                # Process data for database insertion
                db_data = preprocess_data_for_db(metrics)

                # Insert data into database
                success = insert_stock_data(conn, db_data)
                if success:
                    logger.info(f"Successfully inserted data for {symbol} into database")
                else:
                    logger.error(f"Failed to insert data for {symbol} into database")

            except Exception as e:
                logger.error(f"Error processing {symbol}: {str(e)}")

        # Imprimir tiempo de ejecución
        elapsed_time = time.time() - start_time
        logger.info(f"Tiempo de ejecución: {elapsed_time:.2f} segundos")

        # Guardar resultados en JSON
        json_file = os.path.join(ScraperConfig.RESULTS_DIR, f"stocks_metrics_{timestamp}.json")
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(all_metrics, f, indent=2)
        logger.info(f"Resultados guardados en: {json_file}")

        # Mostrar tabla en consola
        print("\n----- MÉTRICAS DE ACCIONES -----")
        print(format_metrics_table(all_metrics))
        print("-------------------------------")

    except Exception as e:
        logger.error(f"Error en la ejecución del trabajo: {str(e)}")
    finally:
        # Close database connection
        if conn:
            conn.close()
            logger.info("Database connection closed")

        # Close the driver
        if driver:
            driver.quit()
            logger.info("Selenium driver closed")


def run_continuous_scraper():
    """Run the scraper continuously based on the schedule"""

    try:
        # Schedule the job to run every X minutes
        schedule.every(ScraperConfig.SCHEDULE_INTERVAL_MINUTES).minutes.do(run_scraping_job)

        logger.info(f"Scheduled scraping job to run every {ScraperConfig.SCHEDULE_INTERVAL_MINUTES} minutes")

        # Run the job immediately for the first time
        logger.info("Running initial scraping job...")
        run_scraping_job()

        # Keep the script running
        while True:
            schedule.run_pending()
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Scraper stopped by user (Keyboard Interrupt)")
    except Exception as e:
        logger.critical(f"Fatal error in scraper: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    # Print banner
    print("=" * 80)
    print(f"AUTOMATED STOCK DATA SCRAPER")
    print(f"Interval: Every {ScraperConfig.SCHEDULE_INTERVAL_MINUTES} minutes")
    print(f"Symbols: {', '.join(ScraperConfig.SYMBOLS_TO_SCRAPE)}")
    print(f"Database: {ScraperConfig.PG_DATABASE} @ {ScraperConfig.PG_HOST}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    run_continuous_scraper()