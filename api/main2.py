# ---- IMPORT LIBRARIES --------
# ------------------------------
import os
import time
import threading
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple
import json
import traceback

# Third-party libraries
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.responses import FileResponse, JSONResponse, HTMLResponse, Response
from starlette.requests import Request
from starlette.status import HTTP_404_NOT_FOUND, HTTP_500_INTERNAL_SERVER_ERROR
from pydantic import BaseModel, Field
import pytz

# ---- CONFIGURATION --------
# ------------------------------
# PostgreSQL Database Configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "bvl_monitor",
    "user": "bvl_user",
    "password": "179fae82"
}

# Original prices and portfolio data
ORIGINAL_PRICES = {
    "BAP": 184.88,
    "BRK-B": 479.20,
    "ILF": 24.10
}

# Portfolio data
PORTFOLIO_DATA = {
    "BAP": {
        "description": "Credicorp Ltd.",
        "purchase_price": 184.88,
        "qty": 26
    },
    "BRK-B": {
        "description": "Berkshire Hathaway Inc. Class B",
        "purchase_price": 479.20,
        "qty": 10
    },
    "ILF": {
        "description": "iShares Latin America 40 ETF",
        "purchase_price": 24.10,
        "qty": 200
    }
}

# Cache configuration
MAX_CACHE_SIZE = 10
CACHE_ITEM_TTL = 60  # 1 minute in seconds - Reduced to get more recent data frequently

# Performance monitoring
PERFORMANCE_METRICS = {
    "api_calls": 0,
    "db_reads": 0,
    "cache_hits": 0,
    "cache_misses": 0,
    "errors": 0,
    "last_error_time": None,
    "last_error_message": None
}

# ---- LOGGING CONFIGURATION --------
# ------------------------------
log_dir = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(
            os.path.join(log_dir, 'api.log'),
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding='utf-8'
        ),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("main_postgres")
cache_logger = logging.getLogger('cache')
perf_logger = logging.getLogger('performance')
db_logger = logging.getLogger('database')


# ---- CACHE IMPLEMENTATION --------
# ------------------------------
class CacheItem:
    def __init__(self, df, last_modified):
        self.df = df
        self.last_load_time = datetime.now()
        self.last_modified = last_modified
        self.size = df.memory_usage(deep=True).sum() if df is not None else 0
        self.access_count = 0

    def __lt__(self, other):
        return self.access_count < other.access_count


dataframes_cache = {}
cache_lock = threading.Lock()
cache_size = 0


# ---- PYDANTIC MODELS --------
# ------------------------------
class StockData(BaseModel):
    symbol: str
    currentPrice: float
    previousClose: Optional[float] = None
    open: Optional[float] = None
    dayLow: Optional[float] = None
    dayHigh: Optional[float] = None
    dividendYield: Optional[float] = None
    financialCurrency: str = "USD"
    volumen: Optional[int] = None
    timestamp: str


class ProfitabilityData(BaseModel):
    symbol: str
    name: str
    original_price: float
    current_price: float
    profitability_percentage: float


class TimeSeriesPoint(BaseModel):
    timestamp: str
    price: float
    return_percentage: Optional[float] = None
    volume: Optional[float] = None
    open: Optional[float] = None
    day_low: Optional[float] = None
    day_high: Optional[float] = None
    previous_close: Optional[float] = None


class SymbolTimeSeries(BaseModel):
    symbol: str
    data: List[TimeSeriesPoint]
    period: str
    current_price: Optional[float] = None
    original_price: Optional[float] = None
    current_profitability: Optional[float] = None
    average_volume: Optional[float] = None
    open_price: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    fifty_two_week_range: Optional[str] = None
    market_cap: Optional[float] = None
    trailing_pe: Optional[float] = None
    dividend_yield: Optional[float] = None
    daily_variation: float = 0.0
    volatility: float = 0.0


class TimeSeriesResponse(BaseModel):
    series: List[SymbolTimeSeries]
    available_periods: List[str] = ["realtime", "1d", "1w", "1m", "3m"]
    available_symbols: List[str] = ["BAP", "BRK-B", "ILF"]


class StockHolding(BaseModel):
    symbol: str
    description: str
    current_price: float
    todays_change: float
    todays_change_percent: float
    purchase_price: float
    qty: int
    total_value: float
    total_gain_loss: float
    total_gain_loss_percent: float


class PortfolioHoldings(BaseModel):
    total_value: float
    todays_change: float
    todays_change_percent: float
    total_gain_loss: float
    total_gain_loss_percent: float
    holdings: List[StockHolding]


class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")
    code: str = Field(..., description="Error code")
    timestamp: str = Field(..., description="Error timestamp")
    details: Optional[Dict[str, Any]] = Field(None, description="Error details")


class StockAPIException(HTTPException):
    """Custom exception for stock API errors"""

    def __init__(self, status_code: int, detail: str, code: str = "GENERAL_ERROR"):
        super().__init__(status_code=status_code, detail=detail)
        self.code = code
        self.timestamp = datetime.now().isoformat()
        self.details = {}


# ---- DATABASE HELPER FUNCTIONS --------
# ------------------------------
def get_db_connection():
    """Create a database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        db_logger.error(f"Database connection error: {str(e)}")
        PERFORMANCE_METRICS["errors"] += 1
        PERFORMANCE_METRICS["last_error_time"] = datetime.now().isoformat()
        PERFORMANCE_METRICS["last_error_message"] = f"Database connection error: {str(e)}"
        raise


def execute_query(query, params=None, fetch=True, use_dict_cursor=False):
    """Execute a query and return results"""
    conn = None
    try:
        conn = get_db_connection()
        cursor_factory = RealDictCursor if use_dict_cursor else None
        with conn.cursor(cursor_factory=cursor_factory) as cursor:
            cursor.execute(query, params)

            if fetch:
                results = cursor.fetchall()
                return results
            else:
                conn.commit()
                return cursor.rowcount
    except Exception as e:
        if conn:
            conn.rollback()
        db_logger.error(f"Query execution error: {str(e)}\nQuery: {query}\nParams: {params}")
        PERFORMANCE_METRICS["errors"] += 1
        PERFORMANCE_METRICS["last_error_time"] = datetime.now().isoformat()
        PERFORMANCE_METRICS["last_error_message"] = f"Query execution error: {str(e)}"
        raise
    finally:
        if conn:
            conn.close()


def list_available_stocks_from_db():
    """List all available stock symbols from the database"""
    try:
        query = "SELECT DISTINCT symbol FROM stock_data ORDER BY symbol"
        results = execute_query(query)

        # Handle the results as a list of tuples
        symbols = [row[0] for row in results] if results else []
        return symbols
    except Exception as e:
        logger.error(f"Error listing stocks from database: {str(e)}")
        # Fallback to default symbols
        return ["BAP", "BRK-B", "ILF"]


def get_last_updated_timestamp(symbol):
    """Get the last updated timestamp for a symbol"""
    try:
        query = "SELECT MAX(timestamp) FROM stock_data WHERE symbol = %s"
        result = execute_query(query, (symbol,))

        # Handle the result safely
        if result and result[0] and result[0][0]:
            return result[0][0].timestamp()
        return 0
    except Exception as e:
        logger.error(f"Error getting last updated timestamp for {symbol}: {str(e)}")
        return 0


def load_stock_data_from_db(symbol, force_reload=False, limit=None):
    """
    Load stock data from database with caching

    Args:
        symbol: Stock symbol to load
        force_reload: Whether to force reload from DB
        limit: Optional limit of most recent records to fetch
    """
    global dataframes_cache, cache_size

    current_modified_time = get_last_updated_timestamp(symbol)

    with cache_lock:
        cache_item = dataframes_cache.get(symbol)

        if cache_item and not force_reload:
            cache_expired = (datetime.now() - cache_item.last_load_time).total_seconds() > CACHE_ITEM_TTL
            data_modified = current_modified_time > cache_item.last_modified

            if not cache_expired and not data_modified:
                cache_item.access_count += 1
                PERFORMANCE_METRICS["cache_hits"] += 1
                cache_logger.debug(f"Using cache for {symbol} (accesses: {cache_item.access_count})")
                return cache_item.df

        PERFORMANCE_METRICS["cache_misses"] += 1
        PERFORMANCE_METRICS["db_reads"] += 1

        try:
            start_time = time.time()

            # Base query with optional limit for the most recent records
            base_query = """
                         SELECT symbol, timestamp, current_price, previous_close, open, day_low, day_high, dividend_yield, volume, fifty_two_week_range, market_cap, trailing_pe, financial_currency
                         FROM stock_data
                         WHERE symbol = %s
                         ORDER BY timestamp \
                         """

            # If limit is specified, modify the query to get the most recent records first
            if limit:
                query = f"""
                    SELECT * FROM (
                        SELECT 
                            symbol, 
                            timestamp, 
                            current_price, 
                            previous_close, 
                            open, 
                            day_low, 
                            day_high, 
                            dividend_yield, 
                            volume, 
                            fifty_two_week_range, 
                            market_cap,
                            trailing_pe,
                            financial_currency
                        FROM stock_data 
                        WHERE symbol = %s 
                        ORDER BY timestamp DESC
                        LIMIT {limit}
                    ) AS recent_data
                    ORDER BY timestamp
                """
            else:
                query = base_query

            results = execute_query(query, (symbol,), use_dict_cursor=True)

            if not results:
                logger.warning(f"No data found for symbol {symbol}")
                return pd.DataFrame()

            # Directly convert to DataFrame
            df = pd.DataFrame(results)

            # Rename columns to match the expected format in the code
            column_mapping = {
                'current_price': 'currentPrice',
                'previous_close': 'previousClose',
                'day_low': 'dayLow',
                'day_high': 'dayHigh',
                'dividend_yield': 'dividendYield',
                'volume': 'volumen',
                'fifty_two_week_range': 'fiftyTwoWeekRange',
                'market_cap': 'marketCap',
                'trailing_pe': 'trailingPE',
                'financial_currency': 'financialCurrency'
            }

            df = df.rename(columns=column_mapping)

            # Ensure timestamp is datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            # Calculate DataFrame size and log performance
            df_size = df.memory_usage(deep=True).sum()
            load_time = time.time() - start_time
            perf_logger.info(
                f"DataFrame {symbol} loaded in {load_time:.2f}s - Size: {df_size / 1024:.2f} KB - Records: {len(df)}")

            # Store in cache
            new_cache_item = CacheItem(df, current_modified_time)
            new_cache_item.access_count = 1

            if symbol in dataframes_cache:
                cache_size -= dataframes_cache[symbol].size

            dataframes_cache[symbol] = new_cache_item
            cache_size += df_size
            clear_cache_if_needed()

            return df

        except Exception as e:
            logger.error(f"Error loading data for {symbol} from database: {str(e)}", exc_info=True)
            PERFORMANCE_METRICS["errors"] += 1
            PERFORMANCE_METRICS["last_error_time"] = datetime.now().isoformat()
            PERFORMANCE_METRICS["last_error_message"] = f"DataFrame load error: {str(e)}"
            return pd.DataFrame()


def clear_cache_if_needed():
    """Clears cache if it exceeds maximum size using LRU policy"""
    global cache_size, dataframes_cache

    if len(dataframes_cache) <= MAX_CACHE_SIZE:
        return

    cache_logger.info(f"Clearing cache (current: {len(dataframes_cache)} items)")
    sorted_items = sorted(dataframes_cache.items(), key=lambda x: x[1].access_count)

    while len(dataframes_cache) > MAX_CACHE_SIZE * 0.8 and sorted_items:  # Clear to 80% capacity
        symbol, item = sorted_items.pop(0)
        cache_size -= item.size
        del dataframes_cache[symbol]
        cache_logger.debug(f"Removed {symbol} from cache (size: {item.size / 1024:.2f} KB)")


# ---- HELPER FUNCTIONS --------
# ------------------------------
def get_latest_data(symbol: str) -> Dict:
    """Gets latest data for a symbol from database"""
    # Get the most recent record from the database
    try:
        query = """
                SELECT symbol, timestamp, current_price, previous_close, open, day_low, day_high, dividend_yield, volume, fifty_two_week_range, market_cap, trailing_pe, financial_currency
                FROM stock_data
                WHERE symbol = %s
                ORDER BY timestamp DESC
                    LIMIT 1 \
                """

        results = execute_query(query, (symbol,), use_dict_cursor=True)

        if not results:
            logger.warning(f"No data available for {symbol}, returning default values")
            return {
                "symbol": symbol,
                "currentPrice": ORIGINAL_PRICES.get(symbol, 0.0),
                "previousClose": ORIGINAL_PRICES.get(symbol, 0.0),
                "open": ORIGINAL_PRICES.get(symbol, 0.0),
                "dayLow": ORIGINAL_PRICES.get(symbol, 0.0) * 0.98,
                "dayHigh": ORIGINAL_PRICES.get(symbol, 0.0) * 1.02,
                "dividendYield": 0.0,
                "financialCurrency": "USD",
                "volumen": 0,
                "timestamp": datetime.now().isoformat()
            }

        latest_data = results[0]

        # Rename keys to match expected format
        column_mapping = {
            'current_price': 'currentPrice',
            'previous_close': 'previousClose',
            'day_low': 'dayLow',
            'day_high': 'dayHigh',
            'dividend_yield': 'dividendYield',
            'volume': 'volumen',
            'fifty_two_week_range': 'fiftyTwoWeekRange',
            'market_cap': 'marketCap',
            'trailing_pe': 'trailingPE',
            'financial_currency': 'financialCurrency'
        }

        for old_key, new_key in column_mapping.items():
            if old_key in latest_data:
                latest_data[new_key] = latest_data.pop(old_key)

        # Format timestamp
        if 'timestamp' in latest_data and latest_data['timestamp']:
            latest_data['timestamp'] = latest_data['timestamp'].isoformat()

        # Ensure all required fields exist
        required_columns = ['currentPrice', 'previousClose', 'open', 'dayLow', 'dayHigh',
                            'dividendYield', 'financialCurrency', 'volumen', 'timestamp']

        for col in required_columns:
            if col not in latest_data:
                if col == 'currentPrice' and symbol in ORIGINAL_PRICES:
                    latest_data[col] = ORIGINAL_PRICES[symbol]
                elif col == 'financialCurrency':
                    latest_data[col] = "USD"
                elif col == 'timestamp':
                    latest_data[col] = datetime.now().isoformat()
                else:
                    latest_data[col] = None

        # Ensure volume is integer
        try:
            latest_data['volumen'] = int(latest_data['volumen']) if latest_data['volumen'] is not None else 0
        except (ValueError, TypeError):
            latest_data['volumen'] = 0

        return latest_data

    except Exception as e:
        logger.error(f"Error getting latest data for {symbol}: {str(e)}", exc_info=True)
        # Return default values
        return {
            "symbol": symbol,
            "currentPrice": ORIGINAL_PRICES.get(symbol, 0.0),
            "previousClose": ORIGINAL_PRICES.get(symbol, 0.0),
            "open": ORIGINAL_PRICES.get(symbol, 0.0),
            "dayLow": ORIGINAL_PRICES.get(symbol, 0.0) * 0.98,
            "dayHigh": ORIGINAL_PRICES.get(symbol, 0.0) * 1.02,
            "dividendYield": 0.0,
            "financialCurrency": "USD",
            "volumen": 0,
            "timestamp": datetime.now().isoformat()
        }


def background_update_all_dataframes():
    """Updates all dataframes in background with rate limiting"""
    try:
        symbols = list_available_stocks_from_db()
        logger.info(f"Starting background update for {len(symbols)} symbols")

        for i, symbol in enumerate(symbols):
            try:
                logger.info(f"Updating {symbol} ({i + 1}/{len(symbols)})")
                # Force reload but also limit to recent data to improve performance
                load_stock_data_from_db(symbol, force_reload=True, limit=1000)
                # Add a small delay between updates to avoid overloading the DB
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error updating {symbol}: {str(e)}")

        logger.info(f"Update completed for {len(symbols)} symbols from database")
    except Exception as e:
        logger.error(f"Error during background update: {str(e)}", exc_info=True)
        PERFORMANCE_METRICS["errors"] += 1
        PERFORMANCE_METRICS["last_error_time"] = datetime.now().isoformat()
        PERFORMANCE_METRICS["last_error_message"] = f"Background update error: {str(e)}"


def calculate_percentage_change(price_series):
    """Calculates percentage change between first and last price in series with safety checks"""
    if len(price_series) < 2 or price_series.isna().all():
        return 0.0

    # Get first non-null price
    first_valid_idx = price_series.first_valid_index()
    if first_valid_idx is None:
        return 0.0
    first_price = price_series.loc[first_valid_idx]

    # Get last non-null price
    last_valid_idx = price_series.last_valid_index()
    if last_valid_idx is None:
        return 0.0
    last_price = price_series.loc[last_valid_idx]

    if first_price == 0 or pd.isna(first_price) or pd.isna(last_price):
        return 0.0

    return ((last_price - first_price) / first_price) * 100


def get_date_range_for_period(period: str) -> Tuple[datetime, datetime]:
    """Gets the date range for a given period with fallbacks for short timeframes"""
    end_date = datetime.now()

    # Definir una fecha base para asegurar que capturemos datos desde enero 2025
    base_date = datetime(2025, 1, 1)

    # Define period mappings
    periods_map = {
        "realtime": timedelta(days=1),  # un día para tiempo real
        "1d": timedelta(days=2),  # dos días para tener datos suficientes
        "1w": timedelta(weeks=1),  # dos semanas
        "1m": timedelta(days=35),  # poco más de un mes
        "3m": timedelta(days=150)  # ampliar a 5 meses para capturar desde enero
    }

    # Si period es "3m" o períodos largos, usar la fecha base (enero 2025)
    if period in ["3m"]:
        start_date = base_date
    else:
        # Para otros períodos, calcular desde la fecha actual
        start_date = end_date - periods_map.get(period, timedelta(weeks=1))

        # Validación extra: si la fecha calculada es anterior a la fecha base, usar la fecha base
        if start_date < base_date:
            start_date = base_date

    logger.info(f"Período {period}: fecha inicio {start_date}, fecha fin {end_date}")
    return start_date, end_date


def filter_dataframe_by_date_range(df: pd.DataFrame, start_date: datetime, end_date: datetime,
                                   period: str = None) -> pd.DataFrame:
    """Filters a DataFrame by date range with fallbacks for empty results"""
    if df.empty:
        return df

    # Ensure timestamp is datetime
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'])

    # First attempt: strict filtering by date range
    filtered_df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)].copy()

    # For realtime period, apply market hours filtering only if we're in market hours
    if period == "realtime" and filtered_df.empty is False:
        current_hour = datetime.now().hour
        if 8 <= current_hour <= 16:
            market_hours_df = filtered_df[
                (filtered_df['timestamp'].dt.hour >= 8) &
                (filtered_df['timestamp'].dt.hour <= 16)
                ]
            # Only use market hours filter if it doesn't make the DataFrame empty
            if not market_hours_df.empty:
                filtered_df = market_hours_df

    # If filtering resulted in empty DataFrame, try with expanded date range
    if filtered_df.empty and not df.empty:
        logger.warning(f"Date filtering resulted in empty DataFrame, trying expanded range")
        expanded_start = start_date - (end_date - start_date)  # Double the range
        filtered_df = df[(df['timestamp'] >= expanded_start) & (df['timestamp'] <= end_date)].copy()

    # Last resort: if still empty, take the most recent records
    if filtered_df.empty and not df.empty:
        logger.warning(f"Expanded date filtering still resulted in empty DataFrame, using most recent data")
        filtered_df = df.sort_values('timestamp', ascending=False).head(10).sort_values('timestamp')

    return filtered_df


def calculate_variation_percentage(df: pd.DataFrame, period: str) -> pd.DataFrame:
    """Calculates price variation percentage with appropriate method based on period"""
    if df.empty or 'currentPrice' not in df.columns:
        return df

    # Make sure currentPrice is numeric
    df['currentPrice'] = pd.to_numeric(df['currentPrice'], errors='coerce')

    # For realtime, calculate point-to-point percentage change
    if period == "realtime":
        df['variation_pct'] = df['currentPrice'].pct_change() * 100
    # For other periods, calculate percentage change from first valid point
    else:
        first_valid_idx = df['currentPrice'].first_valid_index()
        if first_valid_idx is not None:
            first_price = df.loc[first_valid_idx, 'currentPrice']
            if first_price > 0:
                df['variation_pct'] = ((df['currentPrice'] - first_price) / first_price) * 100
            else:
                df['variation_pct'] = 0.0
        else:
            df['variation_pct'] = 0.0

    # Clean up any invalid values
    df['variation_pct'] = df['variation_pct'].replace([np.inf, -np.inf], np.nan).fillna(0)

    return df


def clean_json_data(data):
    """Cleans data to ensure JSON serializability"""
    if isinstance(data, dict):
        return {k: clean_json_data(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [clean_json_data(item) for item in data]
    elif pd.isna(data) or data is pd.NA:
        return None
    elif isinstance(data, float) and (pd.isna(data) or data == float('inf') or data == float('-inf')):
        return None
    elif isinstance(data, pd.Timestamp):
        return data.isoformat()
    else:
        return data


# ---- FASTAPI SETUP --------
# ------------------------------
app = FastAPI(
    title="BVL Live Tracker API",
    description="API for real-time stock data from PostgreSQL",
    version="2.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json"
)

# Static files and CORS configuration
static_dir = os.path.join(os.path.dirname(__file__), 'static')
os.makedirs(static_dir, exist_ok=True)  # Ensure static directory exists
app.mount("/static", StaticFiles(directory=static_dir), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---- REQUEST LOGGING MIDDLEWARE --------
# ------------------------------
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Middleware to log requests and capture performance metrics"""
    PERFORMANCE_METRICS["api_calls"] += 1

    request_id = f"{int(time.time())}-{PERFORMANCE_METRICS['api_calls']}"
    start_time = time.time()
    path = request.url.path
    query_params = dict(request.query_params)
    client_host = request.client.host if request.client else "unknown"

    logger.info(f"Request {request_id} started: {request.method} {path} from {client_host}")

    try:
        response = await call_next(request)
        process_time = time.time() - start_time

        logger.info(
            f"Request {request_id} completed: {request.method} {path} - "
            f"Status: {response.status_code} - Time: {process_time:.3f}s"
        )

        # Add timing header to response
        response.headers["X-Process-Time"] = str(process_time)
        return response
    except Exception as e:
        process_time = time.time() - start_time
        logger.error(
            f"Request {request_id} failed: {request.method} {path} - "
            f"Error: {str(e)} - Time: {process_time:.3f}s"
        )
        PERFORMANCE_METRICS["errors"] += 1
        PERFORMANCE_METRICS["last_error_time"] = datetime.now().isoformat()
        PERFORMANCE_METRICS["last_error_message"] = f"Request error: {str(e)}"
        return JSONResponse(
            status_code=500,
            content={"error": "Internal Server Error", "detail": str(e), "path": path}
        )


# ---- ERROR HANDLING --------
# ------------------------------
@app.exception_handler(StockAPIException)
async def stock_api_exception_handler(request: Request, exc: StockAPIException):
    """Custom exception handler for StockAPIException"""
    logger.error(f"StockAPIException: {exc.detail} (Code: {exc.code})")
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error=exc.detail,
            code=exc.code,
            timestamp=exc.timestamp,
            details=exc.details
        ).dict()
    )

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Custom exception handler for HTTPException"""
    logger.error(f"HTTPException: {exc.detail} (Status: {exc.status_code})")
    PERFORMANCE_METRICS["errors"] += 1
    PERFORMANCE_METRICS["last_error_time"] = datetime.now().isoformat()
    PERFORMANCE_METRICS["last_error_message"] = f"HTTP Exception: {exc.detail}"
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error=exc.detail,
            code=f"HTTP_{exc.status_code}",
           timestamp=datetime.now().isoformat(),
            details={}
        ).dict()
    )


def general_exception_handler(request: Request, exc: Exception):
    """General exception handler for all unhandled exceptions"""
    logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
    PERFORMANCE_METRICS["errors"] += 1
    PERFORMANCE_METRICS["last_error_time"] = datetime.now().isoformat()
    PERFORMANCE_METRICS["last_error_message"] = f"Unhandled exception: {str(exc)}"
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error="Internal Server Error",
            code="INTERNAL_ERROR",
            timestamp=datetime.now().isoformat(),
            details={"exception": str(exc), "type": str(type(exc).__name__)}
        ).dict()
    )


# ---- MONITORING ENDPOINTS --------
# ------------------------------
@app.get("/monitor/cache", tags=["Monitoring"])
def monitor_cache():
    """Endpoint to monitor cache status"""
    with cache_lock:
        cache_info = {
            "total_items": len(dataframes_cache),
            "total_size_kb": f"{cache_size / 1024:.2f}",
            "max_size": MAX_CACHE_SIZE,
            "item_ttl_seconds": CACHE_ITEM_TTL,
            "items": []
        }

        for symbol, item in dataframes_cache.items():
            cache_info["items"].append({
                "symbol": symbol,
                "size_kb": f"{item.size / 1024:.2f}",
                "last_access": item.last_load_time.isoformat(),
                "access_count": item.access_count,
                "age_seconds": (datetime.now() - item.last_load_time).total_seconds(),
                "records": len(item.df) if item.df is not None else 0
            })

    return cache_info


@app.get("/monitor/logs", tags=["Monitoring"])
def monitor_logs(lines: int = 100):
    """Endpoint to view recent logs"""
    log_file = os.path.join(log_dir, 'api.log')

    if not os.path.exists(log_file):
        raise HTTPException(status_code=404, detail="Log file not found")

    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            all_lines = f.readlines()
            recent_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines

        return HTMLResponse(content="<pre>" + "".join(recent_lines) + "</pre>")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading logs: {str(e)}")


@app.get("/monitor/database", tags=["Monitoring"])
def monitor_database():
    """Endpoint to monitor database status and table statistics"""
    try:
        stats = {}

        # Check database connectivity
        try:
            conn = get_db_connection()
            conn.close()
            stats["connection"] = "OK"
        except Exception as e:
            stats["connection"] = f"Error: {str(e)}"
            return stats

        # Get table statistics
        try:
            query = """
                    SELECT COUNT(*)               as total_rows, \
                           MIN(timestamp)         as oldest_record, \
                           MAX(timestamp)         as newest_record, \
                           COUNT(DISTINCT symbol) as distinct_symbols
                    FROM stock_data \
                    """
            result = execute_query(query, use_dict_cursor=True)
            if result:
                stats["stock_data"] = result[0]
                # Format timestamps for readability
                if stats["stock_data"]["oldest_record"]:
                    stats["stock_data"]["oldest_record"] = stats["stock_data"]["oldest_record"].isoformat()
                if stats["stock_data"]["newest_record"]:
                    stats["stock_data"]["newest_record"] = stats["stock_data"]["newest_record"].isoformat()
        except Exception as e:
            stats["stock_data_stats_error"] = str(e)

        # Get count by symbol
        try:
            query = """
                    SELECT symbol, \
                           COUNT(*)       as record_count, \
                           MIN(timestamp) as oldest, \
                           MAX(timestamp) as newest
                    FROM stock_data
                    GROUP BY symbol
                    ORDER BY symbol \
                    """
            result = execute_query(query, use_dict_cursor=True)
            if result:
                stats["symbols"] = []
                for row in result:
                    if row["oldest"]:
                        row["oldest"] = row["oldest"].isoformat()
                    if row["newest"]:
                        row["newest"] = row["newest"].isoformat()
                    stats["symbols"].append(row)
        except Exception as e:
            stats["symbols_stats_error"] = str(e)

        # Get latest 10 records for quick overview
        try:
            query = """
                    SELECT symbol, timestamp, current_price
                    FROM stock_data
                    ORDER BY timestamp DESC
                        LIMIT 10 \
                    """
            result = execute_query(query, use_dict_cursor=True)
            if result:
                stats["recent_records"] = []
                for row in result:
                    if row["timestamp"]:
                        row["timestamp"] = row["timestamp"].isoformat()
                    stats["recent_records"].append(row)
        except Exception as e:
            stats["recent_records_error"] = str(e)

        return stats

    except Exception as e:
        logger.error(f"Error in database monitor: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error monitoring database: {str(e)}")


@app.get("/monitor/metrics", tags=["Monitoring"])
def monitor_metrics():
    """Endpoint to view performance metrics"""
    # Add current stats
    current_metrics = PERFORMANCE_METRICS.copy()
    current_metrics["uptime_seconds"] = (datetime.now() - startup_time).total_seconds()
    current_metrics["cache_size"] = len(dataframes_cache)
    current_metrics["memory_usage_mb"] = cache_size / (1024 * 1024)

    # Avoid division by zero
    cache_hits = current_metrics["cache_hits"]
    cache_misses = current_metrics["cache_misses"]
    total_cache_access = cache_hits + cache_misses

    current_metrics["cache_hit_ratio"] = (
        cache_hits / total_cache_access if total_cache_access > 0 else 0
    )

    return current_metrics


# ---- MAIN ENDPOINTS --------
# ------------------------------
@app.get("/api/timeseries-with-profitability", response_model=TimeSeriesResponse, tags=["Data"])
async def get_time_series_with_profitability(
        symbol: str = Query("BAP", description="Symbol to query (BAP, BRK-B, ILF)"),
        period: str = Query("1w", description="Period (realtime, 1d, 1w, 1m, 3m)"),
        compare_all: bool = Query(False, description="Show all symbols together")
):
    """Gets time series with profitability information"""
    symbols_to_fetch = list_available_stocks_from_db() if compare_all else [symbol]
    start_date, end_date = get_date_range_for_period(period)

    logger.info(f"Processing symbols: {symbols_to_fetch}, period: {period}, date range: {start_date} to {end_date}")
    result = []

    for sym in symbols_to_fetch:
        try:
            # Modificada para tomar toda la información disponible si es 3m
            query = """
                    SELECT symbol, timestamp, current_price as "currentPrice", previous_close as "previousClose", open, day_low as "dayLow", day_high as "dayHigh", dividend_yield as "dividendYield", volume as "volumen", fifty_two_week_range as "fiftyTwoWeekRange", market_cap as "marketCap", trailing_pe as "trailingPE"
                    FROM stock_data
                    WHERE symbol = %s
                      AND timestamp BETWEEN %s \
                      AND %s
                    ORDER BY timestamp \
                    """

            # Para debug: contar cuántos registros hay para el período
            count_query = """
                          SELECT COUNT(*)
                          FROM stock_data
                          WHERE symbol = %s
                            AND timestamp BETWEEN %s \
                            AND %s \
                          """
            count_result = execute_query(count_query, (sym, start_date, end_date))
            record_count = count_result[0][0] if count_result and count_result[0] else 0
            logger.info(f"Symbol {sym}, período {period}: encontrados {record_count} registros en DB")

            db_data = execute_query(query, (sym, start_date, end_date), use_dict_cursor=True)

            if not db_data:
                logger.warning(f"No data available for {sym} in database for the specified period")
                continue

            # Get the latest data for additional metrics
            last_current_data = get_latest_data(sym) or {}

            # Create time series points
            series_data = []
            for row in db_data:
                # Make timestamp a proper string if it's a datetime object
                if isinstance(row['timestamp'], datetime):
                    row['timestamp'] = row['timestamp'].isoformat()

                # Ensure values are the right type
                price = float(row['currentPrice']) if row.get('currentPrice') is not None else None
                volume = float(row.get('volumen', 0)) if row.get('volumen') is not None else None
                day_open = float(row.get('open', 0)) if row.get('open') is not None else None
                day_low = float(row.get('dayLow', 0)) if row.get('dayLow') is not None else None
                day_high = float(row.get('dayHigh', 0)) if row.get('dayHigh') is not None else None

                if price is not None:
                    point = TimeSeriesPoint(
                        timestamp=row['timestamp'],
                        price=price,
                        volume=volume,
                        open=day_open,
                        day_low=day_low,
                        day_high=day_high
                    )
                    series_data.append(point)

            if not series_data:
                logger.warning(f"No valid data points for {sym}")
                continue

            # Get key metrics
            original_price = ORIGINAL_PRICES.get(sym)
            last_price = series_data[-1].price if series_data else None
            profitability = None
            if last_price and original_price and original_price > 0:
                profitability = ((last_price - original_price) / original_price) * 100

            # Create the series object
            symbol_series = SymbolTimeSeries(
                symbol=sym,
                data=series_data,
                period=period,
                original_price=original_price,
                current_price=last_price,
                current_profitability=profitability,
                market_cap=(
                    float(last_current_data['marketCap'])
                    if last_current_data and 'marketCap' in last_current_data
                       and last_current_data['marketCap'] is not None
                    else None
                ),
                trailing_pe=(
                    float(last_current_data['trailingPE'])
                    if last_current_data and 'trailingPE' in last_current_data
                       and last_current_data['trailingPE'] is not None
                    else None
                ),
                dividend_yield=(
                    float(last_current_data['dividendYield'])
                    if last_current_data and 'dividendYield' in last_current_data
                       and last_current_data['dividendYield'] is not None
                    else None
                ),
                fifty_two_week_range=(
                    last_current_data.get('fiftyTwoWeekRange', None)
                )
            )
            result.append(symbol_series)

        except Exception as e:
            logger.error(f"Error processing {sym}: {str(e)}", exc_info=True)

    # If no data was found, return a helpful error
    if not result:
        raise StockAPIException(
            status_code=404,
            detail=f"No data found for the requested symbols and period",
            code="NO_DATA_FOUND"
        )

    return TimeSeriesResponse(series=result)


@app.get("/api/timeseries-variations", response_model=TimeSeriesResponse)
async def get_time_series_variations(
        symbol: str = Query("BAP", description="Símbolo a consultar (BAP, BRK-B, ILF)"),
        period: str = Query("1w", description="Periodo (realtime, 1d, 1w, 1m, 3m)"),
        compare_all: bool = Query(False, description="Mostrar todos los símbolos juntos")
):
    """Obtiene series temporales con información de variación porcentual entre precios"""
    try:
        # Define periodo de tiempo
        periods_map = {
            "realtime": timedelta(hours=6),
            "1d": timedelta(days=1),
            "1w": timedelta(weeks=1),
            "1m": timedelta(days=30),
            "3m": timedelta(days=90)
        }

        # Validar periodo
        if period not in periods_map:
            logger.warning(f"Periodo no reconocido: {period}, usando 1w como valor predeterminado")
            period = "1w"

        symbols_to_fetch = list(ORIGINAL_PRICES.keys()) if compare_all else [symbol]
        end_date = datetime.now()
        start_date = end_date - periods_map[period]
        transition_date = datetime(2025, 1, 1)  # Fecha de transición más temprana

        logger.info(f"Procesando símbolos para variaciones: {symbols_to_fetch}, período: {period}")
        result = []

        for sym in symbols_to_fetch:
            try:
                # Cargar datos con manejo seguro
                current_df = load_dataframe(sym)

                # Manejo seguro de DataFrames vacíos
                if current_df is None or current_df.empty:
                    logger.warning(f"No hay datos actuales para {sym}, continuando con siguiente símbolo")
                    continue

                # Asegurar que timestamp sea datetime
                if 'timestamp' in current_df.columns:
                    current_df['timestamp'] = pd.to_datetime(current_df['timestamp'], errors='coerce')
                else:
                    logger.error(f"Columna 'timestamp' no encontrada para {sym}")
                    continue

                # Filtrar por período con manejo de casos extremos
                filtered_df = current_df[
                    (current_df['timestamp'] >= start_date) &
                    (current_df['timestamp'] <= end_date)
                    ].copy()  # Usar copy() para evitar SettingWithCopyWarning

                # Si no hay datos en el período, intentar con un rango más amplio
                if filtered_df.empty and not current_df.empty:
                    logger.warning(f"No hay datos para {sym} en el período {period}, intentando con rango ampliado")
                    expanded_start = start_date - periods_map[period]  # Duplicar el rango
                    filtered_df = current_df[
                        (current_df['timestamp'] >= expanded_start) &
                        (current_df['timestamp'] <= end_date)
                        ].copy()

                # Como último recurso, usar los datos más recientes si aún está vacío
                if filtered_df.empty and not current_df.empty:
                    logger.warning(f"Usando datos más recientes para {sym} como alternativa")
                    filtered_df = current_df.sort_values('timestamp', ascending=False).head(10).sort_values(
                        'timestamp').copy()

                if filtered_df.empty:
                    logger.warning(f"No hay datos suficientes para {sym}, omitiendo")
                    continue

                # Procesamiento de datos numéricos con manejo de errores
                numeric_cols = ['currentPrice', 'open', 'dayLow', 'dayHigh', 'volumen', 'previousClose']
                for col in numeric_cols:
                    if col in filtered_df.columns:
                        filtered_df[col] = pd.to_numeric(filtered_df[col], errors='coerce')

                        # Llenar NaN con valores razonables
                        if col == 'currentPrice' and filtered_df[col].isna().any():
                            # Si hay valores válidos, usar la media; si no, usar ORIGINAL_PRICES
                            if filtered_df[col].notna().any():
                                mean_val = filtered_df[col].mean()
                                filtered_df[col] = filtered_df[col].fillna(mean_val)
                            else:
                                default_val = ORIGINAL_PRICES.get(sym, 0.0)
                                filtered_df[col] = filtered_df[col].fillna(default_val)
                                logger.info(f"Usando precio original {default_val} para valores faltantes de {sym}")

                # Ordenar por timestamp para cálculos correctos
                filtered_df = filtered_df.sort_values('timestamp')

                # Calcular variaciones según el período con manejo de errores
                try:
                    if period == "realtime":
                        # Variación punto a punto para tiempo real
                        filtered_df['variation_pct'] = filtered_df['currentPrice'].pct_change() * 100
                    else:
                        # Variación respecto al primer punto para otros períodos
                        first_valid_idx = filtered_df['currentPrice'].first_valid_index()
                        if first_valid_idx is not None:
                            first_price = filtered_df.loc[first_valid_idx, 'currentPrice']
                            if first_price > 0:  # Evitar división por cero
                                filtered_df['variation_pct'] = ((filtered_df[
                                                                     'currentPrice'] - first_price) / first_price) * 100
                            else:
                                filtered_df['variation_pct'] = 0.0
                                logger.warning(f"Precio inicial para {sym} es cero, usando 0% como variación")
                        else:
                            filtered_df['variation_pct'] = 0.0
                            logger.warning(f"No se encontró precio inicial válido para {sym}")
                except Exception as calc_error:
                    logger.error(f"Error calculando variaciones para {sym}: {str(calc_error)}")
                    filtered_df['variation_pct'] = 0.0

                # Limpiar valores problemáticos
                filtered_df['variation_pct'] = filtered_df['variation_pct'].replace([np.inf, -np.inf], np.nan).fillna(0)

                # Obtener datos actuales para metadatos
                last_current_data = get_latest_data(sym) or {}

                # Construir puntos de datos con manejo de NaN
                series_data = []
                for _, row in filtered_df.iterrows():
                    try:
                        point = TimeSeriesPoint(
                            timestamp=row['timestamp'].isoformat(),
                            price=float(row['variation_pct']),
                            volume=float(row['volumen']) if pd.notna(row.get('volumen')) else None,
                            open=float(row['open']) if pd.notna(row.get('open')) else None,
                            day_low=float(row['dayLow']) if pd.notna(row.get('dayLow')) else None,
                            day_high=float(row['dayHigh']) if pd.notna(row.get('dayHigh')) else None,
                            previous_close=float(row['previousClose']) if pd.notna(row.get('previousClose')) else None
                        )
                        series_data.append(point)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Error al procesar punto de datos para {sym}: {str(e)}")
                        continue

                if not series_data:
                    logger.warning(f"No se pudieron crear puntos de datos para {sym}")
                    continue

                # Calcular métricas adicionales con manejo de errores
                original_price = ORIGINAL_PRICES.get(sym)

                # Obtener último precio con manejo de errores
                try:
                    last_price = filtered_df['currentPrice'].iloc[-1] if not filtered_df.empty else None
                except IndexError:
                    last_price = None
                    logger.warning(f"Error al obtener último precio para {sym}")

                # Obtener última variación con manejo de errores
                try:
                    last_variation = filtered_df['variation_pct'].iloc[-1] if not filtered_df.empty else 0.0
                except IndexError:
                    last_variation = 0.0
                    logger.warning(f"Error al obtener última variación para {sym}")

                # Calcular volatilidad con manejo de errores
                try:
                    volatility = filtered_df['variation_pct'].std() if len(filtered_df) > 1 else 0
                    if pd.isna(volatility):
                        volatility = 0
                except Exception:
                    volatility = 0
                    logger.warning(f"Error al calcular volatilidad para {sym}")

                # Calcular rentabilidad con manejo de errores
                current_profitability = None
                if last_price is not None and original_price is not None and original_price > 0:
                    current_profitability = ((last_price - original_price) / original_price) * 100

                # Crear series con manejo seguro de valores
                symbol_series = SymbolTimeSeries(
                    symbol=sym,
                    data=series_data,
                    period=period,
                    original_price=original_price,
                    current_price=last_price,
                    current_profitability=current_profitability,
                    market_cap=float(last_current_data.get('marketCap', 0))
                    if last_current_data and pd.notna(last_current_data.get('marketCap'))
                    else None,
                    trailing_pe=float(last_current_data.get('trailingPE', 0))
                    if last_current_data and pd.notna(last_current_data.get('trailingPE'))
                    else None,
                    dividend_yield=float(last_current_data.get('dividendYield', 0))
                    if last_current_data and pd.notna(last_current_data.get('dividendYield'))
                    else None,
                    fifty_two_week_range=last_current_data.get('fiftyTwoWeekRange'),
                    daily_variation=float(last_variation),
                    volatility=float(volatility)
                )
                result.append(symbol_series)

            except Exception as e:
                logger.error(f"Error procesando {sym}: {str(e)}", exc_info=True)
                # Continuamos con el siguiente símbolo en lugar de fallar toda la solicitud

        # Si no hay datos, devolver una respuesta útil en lugar de error
        if not result:
            logger.warning(f"No se encontraron datos para ninguno de los símbolos solicitados")
            # Crear una respuesta vacía con la estructura correcta
            return TimeSeriesResponse(
                series=[],
                available_periods=["realtime", "1d", "1w", "1m", "3m"],
                available_symbols=list(ORIGINAL_PRICES.keys())
            )

        return TimeSeriesResponse(series=result)

    except Exception as e:
        # Capturar cualquier excepción no manejada en el nivel superior
        logger.error(f"Error no manejado en get_time_series_variations: {str(e)}", exc_info=True)
        raise StockAPIException(
            status_code=500,
            detail=f"Error interno al procesar variaciones: {str(e)}",
            code="INTERNAL_PROCESSING_ERROR"
        )

@app.get("/portfolio/holdings/live", response_model=PortfolioHoldings, tags=["Portfolio"])
def get_portfolio_holdings_live():
    """Gets real-time portfolio holdings data"""
    holdings = []
    portfolio_total_value = 0
    portfolio_todays_change_value = 0
    portfolio_total_gain_loss = 0
    portfolio_previous_value = 0

    for symbol, data in PORTFOLIO_DATA.items():
        try:
            current_data = get_latest_data(symbol)

            if current_data is None:
                logger.error(f"No data found for {symbol}")
                continue

            current_price = current_data.get('currentPrice')
            previous_close = current_data.get('previousClose')

            if previous_close is None and current_price is not None:
                logger.warning(f"No previous close data for {symbol}, using current price")
                previous_close = current_price

            if current_price is None:
                logger.error(f"No current price for {symbol}")
                continue

            purchase_price = data["purchase_price"]
            qty = data["qty"]

            todays_change = current_price - previous_close
            todays_change_percent = (todays_change / previous_close) * 100 if previous_close > 0 else 0

            total_value = current_price * qty
            total_gain_loss = total_value - (purchase_price * qty)
            total_gain_loss_percent = (total_gain_loss / (purchase_price * qty)) * 100 if purchase_price > 0 else 0

            portfolio_total_value += total_value
            portfolio_todays_change_value += todays_change * qty
            portfolio_total_gain_loss += total_gain_loss
            portfolio_previous_value += previous_close * qty

            holding = StockHolding(
                symbol=symbol,
                description=data["description"],
                current_price=round(current_price, 2),
                todays_change=round(todays_change, 2),
                todays_change_percent=round(todays_change_percent, 2),
                purchase_price=round(purchase_price, 2),
                qty=qty,
                total_value=round(total_value, 2),
                total_gain_loss=round(total_gain_loss, 2),
                total_gain_loss_percent=round(total_gain_loss_percent, 2)
            )
            holdings.append(holding)
        except Exception as e:
            logger.error(f"Error processing portfolio holding for {symbol}: {str(e)}", exc_info=True)

    # Check if we have any holdings
    if not holdings:
        logger.error("No portfolio holdings could be processed")
        raise StockAPIException(
            status_code=500,
            detail="Failed to process portfolio holdings",
            code="PORTFOLIO_PROCESSING_ERROR"
        )

    portfolio_initial_value = portfolio_total_value - portfolio_total_gain_loss
    portfolio_todays_change_percent = (
        (portfolio_todays_change_value / portfolio_previous_value) * 100
        if portfolio_previous_value > 0 else 0
    )
    portfolio_total_gain_loss_percent = (
        (portfolio_total_gain_loss / portfolio_initial_value) * 100
        if portfolio_initial_value > 0 else 0
    )

    return PortfolioHoldings(
        total_value=round(portfolio_total_value, 2),
        todays_change=round(portfolio_todays_change_value, 2),
        todays_change_percent=round(portfolio_todays_change_percent, 2),
        total_gain_loss=round(portfolio_total_gain_loss, 2),
        total_gain_loss_percent=round(portfolio_total_gain_loss_percent, 2),
        holdings=holdings
    )


@app.get("/portfolio/profitability", response_model=List[ProfitabilityData], tags=["Portfolio"])
def get_portfolio_profitability():
    """Gets profitability data for all portfolio holdings"""
    result = []

    for symbol, data in PORTFOLIO_DATA.items():
        try:
            current_data = get_latest_data(symbol)
            if current_data is None or current_data.get('currentPrice') is None:
                continue

            purchase_price = data["purchase_price"]
            current_price = current_data.get('currentPrice')
            profitability = ((current_price - purchase_price) / purchase_price) * 100

            result.append(ProfitabilityData(
                symbol=symbol,
                name=data["description"],
                original_price=purchase_price,
                current_price=current_price,
                profitability_percentage=profitability
            ))
        except Exception as e:
            logger.error(f"Error calculating profitability for {symbol}: {str(e)}")

    return sorted(result, key=lambda x: x.profitability_percentage, reverse=True)


@app.post("/refresh", tags=["Admin"])
def refresh_data(background_tasks: BackgroundTasks):
    """Forces a refresh of all cached data"""
    background_tasks.add_task(background_update_all_dataframes)
    return {"message": "Background data refresh started", "timestamp": datetime.now().isoformat()}


@app.get("/health", tags=["Admin"])
def health_check():
    """API health check endpoint"""
    try:
        # Basic database health check
        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if we can query the database
        cursor.execute("SELECT COUNT(*) FROM stock_data")
        row_count = cursor.fetchone()[0]

        # Check for latest data
        cursor.execute("SELECT MAX(timestamp) FROM stock_data")
        latest_data = cursor.fetchone()[0]

        conn.close()

        db_status = {
            "connection": "OK",
            "row_count": row_count,
            "latest_data": latest_data.isoformat() if latest_data else None
        }
    except Exception as e:
        db_status = {
            "connection": f"Error: {str(e)}",
            "row_count": None,
            "latest_data": None
        }

    return {
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "cached_symbols": list(dataframes_cache.keys()),
        "database_status": db_status,
        "uptime_seconds": (datetime.now() - startup_time).total_seconds(),
        "version": "2.0.0"
    }


# ---- HTML ENDPOINTS --------
# ------------------------------
@app.get("/api/symbols", tags=["Data"])
async def get_available_symbols():
    """Returns list of available symbols"""
    symbols = list_available_stocks_from_db()
    return {"symbols": symbols}


@app.get("/html/timeseries-profitability", response_class=HTMLResponse, tags=["HTML"])
async def get_timeseries_profitability_html():
    """Serves the full analysis interface"""
    return FileResponse(os.path.join(static_dir, "timeseries-profitability.html"))


@app.get("/html/market/variations", response_class=HTMLResponse, tags=["HTML"])
async def get_market_variations_html():
    """Serves the price variations HTML page"""
    return FileResponse(os.path.join(static_dir, "market_variations.html"))


@app.get("/html/portfolio/holdings", response_class=HTMLResponse, tags=["HTML"])
async def get_portfolio_holdings_html():
    """Serves the portfolio holdings HTML page"""
    return FileResponse(os.path.join(static_dir, "portfolio_holdings.html"))


@app.get("/html/portfolio/profitability", response_class=HTMLResponse, tags=["HTML"])
async def get_portfolio_profitability_html():
    """Serves the portfolio profitability HTML page"""
    return FileResponse(os.path.join(static_dir, "portfolio_profitability.html"))


@app.get("/html", tags=["HTML"])
async def get_html_index():
    """Serves the main HTML page"""
    return FileResponse(os.path.join(static_dir, "index.html"))


@app.get("/", include_in_schema=False)
async def redirect_to_html():
    """Redirects root to the HTML index"""
    return HTMLResponse(content="""
    <html>
        <head>
            <meta http-equiv="refresh" content="0;url=/html" />
        </head>
        <body>
            <p>Redirecting to <a href="/html">HTML interface</a>...</p>
        </body>
    </html>
    """)


# ---- CUSTOM ROUTES FOR DIRECT FILE ACCESS --------
# -------------------------------------------
@app.get("/css/{filename}", include_in_schema=False)
async def get_css(filename: str):
    """Serves CSS files"""
    css_dir = os.path.join(static_dir, "css")
    os.makedirs(css_dir, exist_ok=True)
    return FileResponse(os.path.join(css_dir, filename))


@app.get("/js/{filename}", include_in_schema=False)
async def get_js(filename: str):
    """Serves JavaScript files"""
    js_dir = os.path.join(static_dir, "js")
    os.makedirs(js_dir, exist_ok=True)
    return FileResponse(os.path.join(js_dir, filename))


# ---- ADDITIONAL DATA ENDPOINTS --------
# ------------------------------
@app.get("/api/realtime-prices", tags=["Data"])
async def get_realtime_prices():
    """Gets real-time prices for all available symbols"""
    symbols = list_available_stocks_from_db()
    result = {}

    for symbol in symbols:
        try:
            # Get only the most recent record
            query = """
                    SELECT current_price, timestamp, previous_close
                    FROM stock_data
                    WHERE symbol = %s
                    ORDER BY timestamp DESC
                        LIMIT 1 \
                    """

            data = execute_query(query, (symbol,), use_dict_cursor=True)

            if data:
                # Calculate change
                current = data[0]['current_price']
                previous = data[0]['previous_close']
                change = current - previous if previous is not None else 0
                change_percent = (change / previous * 100) if previous and previous > 0 else 0

                result[symbol] = {
                    "price": current,
                    "change": change,
                    "change_percent": change_percent,
                    "timestamp": data[0]['timestamp'].isoformat() if isinstance(data[0]['timestamp'], datetime) else
                    data[0]['timestamp']
                }

        except Exception as e:
            logger.error(f"Error getting realtime price for {symbol}: {str(e)}")
            result[symbol] = {"error": str(e)}

    return result


@app.get("/api/last-updates", tags=["Data"])
async def get_last_updates(limit: int = 10):
    """Gets the most recent updates to the database"""
    try:
        query = """
                SELECT symbol, timestamp, current_price, previous_close, volume
                FROM stock_data
                ORDER BY timestamp DESC
                    LIMIT %s \
                """

        results = execute_query(query, (limit,), use_dict_cursor=True)

        if not results:
            return {"updates": []}

        updates = []
        for row in results:
            # Format timestamp
            if 'timestamp' in row and isinstance(row['timestamp'], datetime):
                row['timestamp'] = row['timestamp'].isoformat()

            # Calculate price change
            current = row.get('current_price')
            previous = row.get('previous_close')
            if current is not None and previous is not None and previous > 0:
                row['change'] = current - previous
                row['change_percent'] = (row['change'] / previous) * 100
            else:
                row['change'] = 0
                row['change_percent'] = 0

            updates.append(row)

        return {"updates": updates}

    except Exception as e:
        logger.error(f"Error getting last updates: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting last updates: {str(e)}")

# ---- START PERIODIC UPDATES FUNCTION --------
# ------------------------------
def start_periodic_updates():
    """
    Starts a thread to periodically update data from the database
    with reduced frequency and staggered updates
    """

    def update_loop():
        while True:
            try:
                logger.info("Starting periodic data update from database")

                # Check connection to database
                try:
                    conn = get_db_connection()
                    conn.close()
                    logger.info("Database connection verified")
                except Exception as db_err:
                    logger.error(f"Database connection error: {str(db_err)}")
                    time.sleep(60)  # Wait a minute before retrying
                    continue

                # Get all available symbols
                symbols = list_available_stocks_from_db()
                logger.info(f"Symbols found in database: {symbols}")

                # Update each symbol with a delay between them to avoid load spikes
                for symbol in symbols:
                    try:
                        logger.info(f"Updating data for {symbol} from database")
                        # Use the limit parameter to only load the most recent data for performance
                        load_stock_data_from_db(symbol, force_reload=True, limit=200)
                        # Wait 2 seconds between each symbol
                        time.sleep(2)
                    except Exception as e:
                        logger.error(f"Error updating {symbol} from database: {str(e)}")

                logger.info(f"Update completed for {len(symbols)} symbols from database")

                # Wait 2 minutes (120 seconds) before the next update
                # More responsive than 5 minutes but still reasonable for database load
                time.sleep(120)

            except Exception as e:
                logger.error(f"Error in database update loop: {str(e)}", exc_info=True)
                # Wait before retrying if there's an error
                time.sleep(30)

    # Start the update thread
    update_thread = threading.Thread(target=update_loop, daemon=True)
    update_thread.start()
    logger.info("Periodic database update thread started")


# ---- STARTUP/SHUTDOWN EVENTS --------
# ------------------------------
startup_time = datetime.now()


@app.on_event("startup")
def startup_event():
    """
    Event that runs on application startup

    Performs:
    - Initial database configuration
    - Starts the periodic updates thread
    - Logs the initial state
    """
    global startup_time
    startup_time = datetime.now()

    logger.info("Starting application with PostgreSQL integration...")
    logger.info(f"Cache configuration - Maximum: {MAX_CACHE_SIZE} items, TTL: {CACHE_ITEM_TTL}s")
    logger.info(f"Database configuration - Host: {DB_CONFIG['host']}, Database: {DB_CONFIG['database']}")

    # Check database connection at startup
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM stock_data")
        count = cursor.fetchone()[0]
        logger.info(f"Database connection verified - {count} records in stock_data table")
        conn.close()
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        # Continue starting the application, but warn about the issue
        logger.warning("Application will continue, but there may be issues with database connectivity")

    start_periodic_updates()
    logger.info("API initialized and periodic update configured")


@app.on_event("shutdown")
def shutdown_event():
    """
    Event that runs when the application stops

    Performs:
    - Cache cleanup
    - Logs the closing state
    """
    logger.info("Stopping application...")
    with cache_lock:
        logger.info(f"Clearing cache ({len(dataframes_cache)} items)")
        dataframes_cache.clear()


if __name__ == "__main__":
    import uvicorn

    # More robust server configuration
    uvicorn.run(
        "main2:app",
        host="0.0.0.0",
        port=8080,
        reload=False,  # Disable reload in production
        workers=2,  # Reduce number of workers
        timeout_keep_alive=65,  # Longer keep-alive time
        log_level="info"
    )
