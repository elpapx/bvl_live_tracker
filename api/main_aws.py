# ---- IMPORT LIBRARIES --------
# ------------------------------
# Standard libraries
import os
import time
import threading
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple
from io import BytesIO, StringIO
import traceback

# Third-party libraries
import numpy as np
import pandas as pd
import boto3
from botocore.exceptions import ClientError
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
# AWS Configuration
AWS_REGION = "us-east-1"  # Region where you created the bucket
S3_BUCKET_NAME = "bvl-monitor-data"  # Name of your existing bucket
S3_DATA_DIR = "data"  # Folder within the bucket for data
S3_HISTORICAL_DIR = "historical_data"  # Folder for historical data

# Original prices and portfolio data
ORIGINAL_PRICES = {
    "BAP": 184.88,
    "BRK-B": 479.20,
    "ILF": 24.10
}

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
MAX_CACHE_SIZE = 10  # Increased from 5
CACHE_ITEM_TTL = 300  # 5 minutes in seconds (reduced from 600)

# Date configuration
TRANSITION_DATE = datetime(2025, 1, 1)  # Earlier transition date for more flexibility
APP_TIMEZONE = pytz.timezone('America/New_York')  # NYSE timezone

# Performance monitoring
PERFORMANCE_METRICS = {
    "api_calls": 0,
    "s3_reads": 0,
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

logger = logging.getLogger(__name__)
cache_logger = logging.getLogger('cache')
perf_logger = logging.getLogger('performance')
s3_logger = logging.getLogger('s3')

# Initialize S3 client
s3_client = boto3.client('s3', region_name=AWS_REGION)


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


class FinancialDataPoint(BaseModel):
    timestamp: str
    price: float
    volume: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    dividend_yield: Optional[float] = None
    profitability: Optional[float] = None


class SymbolFinancialData(BaseModel):
    symbol: str
    period: str
    data: List[FinancialDataPoint]
    stats: Dict[str, float]


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


# ---- S3 HELPER FUNCTIONS --------
# ------------------------------
def list_s3_files(prefix=""):
    """Lists files in the S3 bucket with a given prefix"""
    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=prefix
        )

        if 'Contents' not in response:
            return []

        return [item['Key'] for item in response['Contents']]
    except Exception as e:
        s3_logger.error(f"Error listing files in S3: {str(e)}")
        PERFORMANCE_METRICS["errors"] += 1
        PERFORMANCE_METRICS["last_error_time"] = datetime.now().isoformat()
        PERFORMANCE_METRICS["last_error_message"] = f"S3 listing error: {str(e)}"
        return []


def get_s3_file_last_modified(s3_key):
    """Gets the last modified date of a file in S3"""
    try:
        response = s3_client.head_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key
        )
        return response.get('LastModified', datetime.now()).timestamp()
    except Exception as e:
        s3_logger.warning(f"Error getting last modified date for {s3_key}: {str(e)}")
        return 0


def download_file_from_s3(s3_key):
    """Downloads a file from S3 to an in-memory buffer"""
    try:
        PERFORMANCE_METRICS["s3_reads"] += 1
        start_time = time.time()

        response = s3_client.get_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key
        )
        content = response['Body'].read()

        s3_logger.debug(f"Download time for {s3_key}: {time.time() - start_time:.3f}s")
        return content
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            s3_logger.warning(f"File not found in S3: {s3_key}")
        else:
            s3_logger.error(f"Error downloading {s3_key} from S3: {str(e)}")
            PERFORMANCE_METRICS["errors"] += 1
            PERFORMANCE_METRICS["last_error_time"] = datetime.now().isoformat()
            PERFORMANCE_METRICS["last_error_message"] = f"S3 download error: {str(e)}"
        return None
    except Exception as e:
        s3_logger.error(f"Error downloading {s3_key} from S3: {str(e)}")
        PERFORMANCE_METRICS["errors"] += 1
        PERFORMANCE_METRICS["last_error_time"] = datetime.now().isoformat()
        PERFORMANCE_METRICS["last_error_message"] = f"S3 download error: {str(e)}"
        return None


# ---- HELPER FUNCTIONS --------
# ------------------------------
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


def get_s3_csv_path(symbol: str) -> str:
    """Gets the CSV file path in S3 based on symbol"""
    if symbol.upper() == "BRK-B":
        filename = "brk-b_stock_data.csv"
    elif symbol.upper() == "ILF":
        filename = "ilf_etf_data.csv"
    else:
        filename = f"{symbol.lower()}_{'etf_data' if 'ETF' in symbol.upper() else 'stock_data'}.csv"

    return f"{S3_DATA_DIR}/{filename}"


def get_s3_historical_path(symbol: str) -> str:
    """Gets the historical CSV file path in S3 based on symbol"""
    return f"{S3_HISTORICAL_DIR}/{symbol.lower()}_historical.csv"


def check_file_exists_in_s3(s3_key) -> bool:
    """Checks if a file exists in S3"""
    try:
        s3_client.head_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key
        )
        return True
    except Exception:
        return False


def load_dataframe(symbol: str, force_reload: bool = False) -> pd.DataFrame:
    """Loads DataFrame from S3 with optimized cache handling and error recovery"""
    global dataframes_cache, cache_size

    s3_key = get_s3_csv_path(symbol)
    current_modified_time = get_s3_file_last_modified(s3_key)

    with cache_lock:
        cache_item = dataframes_cache.get(symbol)

        if cache_item and not force_reload:
            cache_expired = (datetime.now() - cache_item.last_load_time).total_seconds() > CACHE_ITEM_TTL
            file_modified = current_modified_time > cache_item.last_modified

            if not cache_expired and not file_modified:
                cache_item.access_count += 1
                PERFORMANCE_METRICS["cache_hits"] += 1
                cache_logger.debug(f"Using cache for {symbol} (accesses: {cache_item.access_count})")
                return cache_item.df

        PERFORMANCE_METRICS["cache_misses"] += 1
        perf_logger.info(f"Loading data for {symbol} from S3 {'(forced)' if force_reload else ''}")
        start_time = time.time()

        try:
            if not check_file_exists_in_s3(s3_key):
                logger.error(f"CSV file not found in S3: {s3_key}")
                return pd.DataFrame()  # Return empty DataFrame instead of None

            # Download the file from S3
            file_content = download_file_from_s3(s3_key)
            if file_content is None:
                return pd.DataFrame()  # Return empty DataFrame instead of None

            # Convert the content to a DataFrame
            try:
                # Read first to see what columns it has
                buffer = BytesIO(file_content)
                cols_preview = pd.read_csv(buffer, nrows=5)
                buffer.seek(0)  # Reset the buffer for the next read

                needed_cols = ['timestamp', 'currentPrice', 'previousClose', 'open',
                               'dayLow', 'dayHigh', 'dividendYield', 'volumen',
                               'marketCap', 'trailingPE', 'fiftyTwoWeekRange']
                cols_to_use = [col for col in needed_cols if col in cols_preview.columns]

                buffer = BytesIO(file_content)
                df = pd.read_csv(buffer, usecols=cols_to_use,
                                 parse_dates=['timestamp']) if cols_to_use else pd.read_csv(buffer)
            except Exception as e:
                logger.warning(f"Error reading specific columns: {str(e)}, reading all columns")
                buffer = BytesIO(file_content)
                df = pd.read_csv(buffer)

            # Drop unnecessary columns
            columns_to_drop = [col for col in ['symbol', 'Symbol', 'Ticker'] if col in df.columns]
            df = df.drop(columns=columns_to_drop)

            # Check for required columns and add them if missing
            required_cols = ['timestamp', 'currentPrice']
            for col in required_cols:
                if col not in df.columns:
                    logger.warning(f"Required column '{col}' not found in {s3_key}, adding empty column")
                    df[col] = None
                    if col == 'timestamp':
                        df[col] = pd.to_datetime('now')  # Use current time as fallback

            # Handle timestamp column
            try:
                df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                df = df.dropna(subset=['timestamp'])
            except Exception as e:
                logger.error(f"Error processing timestamp column: {str(e)}")
                # Add a timestamp column if conversion failed
                df['timestamp'] = pd.date_range(
                    start=datetime.now() - timedelta(days=30),
                    periods=len(df),
                    freq='H'
                )

            # Handle currentPrice column
            if 'currentPrice' in df.columns:
                df['currentPrice'] = pd.to_numeric(df['currentPrice'], errors='coerce', downcast='float')
                # Fill NaN values with mean or last valid value
                if df['currentPrice'].isna().any():
                    if df['currentPrice'].notna().any():
                        mean_price = df['currentPrice'].mean()
                        df['currentPrice'] = df['currentPrice'].fillna(mean_price)
                    else:
                        df['currentPrice'] = ORIGINAL_PRICES.get(symbol, 0.0)

            # Handle volume column
            if 'volumen' in df.columns:
                df['volumen'] = pd.to_numeric(df['volumen'], errors='coerce', downcast='integer')
            elif 'volume' in df.columns:
                df['volumen'] = pd.to_numeric(df['volume'], errors='coerce', downcast='integer')
                # Drop the original column to avoid confusion
                if 'volume' in df.columns:
                    df = df.drop(columns=['volume'])
            else:
                df['volumen'] = 0

            # Handle all other numeric columns
            numeric_cols = ['open', 'dayHigh', 'dayLow', 'marketCap', 'trailingPE', 'dividendYield', 'previousClose']
            existing_numeric_cols = [col for col in numeric_cols if col in df.columns]
            for col in existing_numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce', downcast='float')
                # Fill NA values with appropriate defaults
                if col in ['open', 'dayHigh', 'dayLow', 'previousClose'] and df[col].isna().any():
                    if 'currentPrice' in df.columns and df['currentPrice'].notna().any():
                        df[col] = df[col].fillna(df['currentPrice'])

            # Sort by timestamp and handle duplicates
            df = df.sort_values('timestamp')
            df = df.drop_duplicates(subset=['timestamp'], keep='last')

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
            logger.error(f"Error loading data for {symbol} from S3: {str(e)}", exc_info=True)
            PERFORMANCE_METRICS["errors"] += 1
            PERFORMANCE_METRICS["last_error_time"] = datetime.now().isoformat()
            PERFORMANCE_METRICS["last_error_message"] = f"DataFrame load error: {str(e)}"
            return pd.DataFrame()  # Return empty DataFrame instead of None


def load_historical_data(symbol: str) -> pd.DataFrame:
    """Loads historical data from S3 files including volume with improved error handling"""
    hist_s3_key = get_s3_historical_path(symbol)

    if not check_file_exists_in_s3(hist_s3_key):
        s3_logger.warning(f"Historical data file not found: {hist_s3_key}")
        return pd.DataFrame()

    try:
        file_content = download_file_from_s3(hist_s3_key)
        if file_content is None:
            return pd.DataFrame()

        buffer = BytesIO(file_content)
        df = pd.read_csv(buffer)

        # Process date column
        if 'Date' in df.columns:
            df['timestamp'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
        elif 'timestamp' not in df.columns:
            logger.warning(f"No date column found in historical data for {symbol}")
            # Create a date column as a fallback
            df['timestamp'] = pd.date_range(
                start=datetime.now() - timedelta(days=365),
                periods=len(df),
                freq='D'
            )

        df = df.dropna(subset=['timestamp'])

        # Process price and volume columns
        price_column = None
        if 'Close' in df.columns:
            price_column = 'Close'
        elif 'Price' in df.columns:
            price_column = 'Price'
        elif 'currentPrice' in df.columns:
            price_column = 'currentPrice'

        if price_column:
            df['currentPrice'] = pd.to_numeric(df[price_column], errors='coerce')
        else:
            logger.warning(f"No price column found in historical data for {symbol}")
            # Use a default price as fallback
            df['currentPrice'] = ORIGINAL_PRICES.get(symbol, 100.0)

        # Handle volume column
        volume_column = None
        if 'Volume' in df.columns:
            volume_column = 'Volume'
        elif 'volume' in df.columns:
            volume_column = 'volume'
        elif 'volumen' in df.columns:
            volume_column = 'volumen'

        if volume_column:
            df['volumen'] = pd.to_numeric(df[volume_column], errors='coerce')
        else:
            # Use a default volume as fallback
            df['volumen'] = 10000

        # Ensure we have the minimum required columns
        required_columns = ['timestamp', 'currentPrice', 'volumen']
        current_columns = list(df.columns)

        # Keep only necessary columns
        columns_to_keep = required_columns + [col for col in current_columns if
                                              col not in required_columns and col not in ['Date', price_column,
                                                                                          volume_column]]
        df = df[columns_to_keep]

        # Sort by timestamp
        df = df.sort_values('timestamp')

        logger.info(f"Loaded historical data for {symbol}: {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"Error loading historical data for {symbol} from S3: {str(e)}", exc_info=True)
        PERFORMANCE_METRICS["errors"] += 1
        PERFORMANCE_METRICS["last_error_time"] = datetime.now().isoformat()
        PERFORMANCE_METRICS["last_error_message"] = f"Historical data load error: {str(e)}"
        return pd.DataFrame()


def merge_historical_and_current_data(symbol: str) -> pd.DataFrame:
    """Merges historical and current data for a symbol with proper handling of overlaps"""
    historical_df = load_historical_data(symbol)
    current_df = load_dataframe(symbol)

    if historical_df.empty and current_df.empty:
        logger.warning(f"No data available for {symbol} in either historical or current sources")
        return pd.DataFrame()

    if historical_df.empty:
        logger.info(f"No historical data available for {symbol}, using only current data")
        return current_df

    if current_df.empty:
        logger.info(f"No current data available for {symbol}, using only historical data")
        return historical_df

    # Set transition date as the cutoff between datasets
    transition_date = TRANSITION_DATE

    # Filter both datasets by the transition date
    historical_df = historical_df[historical_df['timestamp'] < transition_date]
    current_df = current_df[current_df['timestamp'] >= transition_date]

    # Ensure columns match before concatenation
    for col in ['currentPrice', 'volumen']:
        if col not in historical_df.columns:
            historical_df[col] = np.nan
        if col not in current_df.columns:
            current_df[col] = np.nan

    # Concatenate the datasets
    combined_df = pd.concat([historical_df, current_df], ignore_index=True)

    # Sort and clean up the combined dataset
    combined_df = combined_df.sort_values('timestamp')
    combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='last')

    logger.info(
        f"Merged data for {symbol}: {len(historical_df)} historical + {len(current_df)} current = {len(combined_df)} total records")
    return combined_df


def background_update_all_dataframes():
    """Updates all dataframes in background with rate limiting"""
    try:
        symbols = list_available_stocks_internal()
        logger.info(f"Starting background update for {len(symbols)} symbols")

        for i, symbol in enumerate(symbols):
            try:
                logger.info(f"Updating {symbol} ({i + 1}/{len(symbols)})")
                load_dataframe(symbol, force_reload=True)
                # Add a small delay between updates to avoid API rate limits
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error updating {symbol}: {str(e)}")

        logger.info(f"Background update completed for {len(symbols)} symbols")
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


def list_available_stocks_internal():
    """Lists all available symbols from S3 with improved error handling"""
    try:
        # List all CSV files in the data directory in S3
        s3_files = list_s3_files(f"{S3_DATA_DIR}/")

        symbols = []
        for s3_key in s3_files:
            # Extract the filename without the path
            filename = s3_key.split('/')[-1]

            if not filename.endswith('.csv'):
                continue

            if filename.startswith('brk-b'):
                symbols.append("BRK-B")
            else:
                base_name = filename.split('_')[0].upper()
                if base_name not in symbols:
                    symbols.append(base_name)

        return sorted(symbols)
    except Exception as e:
        logger.error(f"Error listing stocks from S3: {str(e)}", exc_info=True)
        return ["BAP", "BRK-B", "ILF"]  # Return default symbols in case of error


def get_latest_data(symbol: str) -> Dict:
    """Gets latest data for a symbol from S3 with improved error handling"""
    df = load_dataframe(symbol)

    if df is None or df.empty:
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

    try:
        # Get the latest row by timestamp
        latest_data = df.loc[df['timestamp'].idxmax()].to_dict()
    except Exception:
        # Fallback if idxmax fails - sort and take last row
        df = df.sort_values('timestamp')
        if df.empty:
            logger.error(f"DataFrame for {symbol} is empty after sorting")
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
        latest_data = df.iloc[-1].to_dict()

    # Clean up NaN values
    for key, value in latest_data.items():
        if pd.isna(value):
            latest_data[key] = None

    # Add symbol to the data
    latest_data['symbol'] = symbol

    # Ensure all required columns exist
    required_columns = ['currentPrice', 'previousClose', 'open', 'dayLow', 'dayHigh',
                        'dividendYield', 'financialCurrency', 'volumen', 'timestamp']

    for col in required_columns:
        if col not in latest_data:
            if col == 'currentPrice' and symbol in ORIGINAL_PRICES:
                latest_data[col] = ORIGINAL_PRICES[symbol]
                logger.warning(
                    f"{col} is missing for {symbol}, using original price {ORIGINAL_PRICES[symbol]} as fallback")
            elif col == 'financialCurrency':
                latest_data[col] = "USD"
            elif col == 'timestamp':
                latest_data[col] = datetime.now().isoformat()
            else:
                latest_data[col] = None
                logger.warning(f"{col} is missing for {symbol}")

    # Ensure volume is integer
    try:
        latest_data['volumen'] = int(latest_data['volumen']) if latest_data['volumen'] is not None else 0
    except (ValueError, TypeError):
        latest_data['volumen'] = 0

    # Handle timestamp formatting
    if isinstance(latest_data['timestamp'], pd.Timestamp):
        latest_data['timestamp'] = latest_data['timestamp'].isoformat()

    return latest_data


def get_historical_data(symbol: str, days: int = 30) -> List[Dict]:
    """Gets historical data for a symbol from S3 with improved error handling"""
    df = load_dataframe(symbol)

    if df is None or df.empty:
        logger.warning(f"No data available for {symbol}, returning empty list")
        return []

    try:
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp', ascending=False)

        # Get the most recent records
        historical_df = df.head(days)

        # Convert DataFrame to list of dictionaries
        records = []
        for _, row in historical_df.iterrows():
            record = {}
            for col in row.index:
                value = row[col]
                # Handle NaN values
                if pd.isna(value):
                    record[col] = None
                # Handle timestamp objects
                elif col == 'timestamp' and isinstance(value, pd.Timestamp):
                    record[col] = value.isoformat()
                # Handle other values
                else:
                    record[col] = value
            records.append(record)

        return records
    except Exception as e:
        logger.error(f"Error processing historical data for {symbol}: {str(e)}", exc_info=True)
        return []


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


def get_date_range_for_period(period: str) -> Tuple[datetime, datetime]:
    """Gets the date range for a given period with fallbacks for short timeframes"""
    end_date = datetime.now()

    # Define period mappings with more generous timeframes
    periods_map = {
        "realtime": timedelta(days=1),  # Changed from hours=6 to days=1
        "1d": timedelta(days=2),  # Changed from days=1 to days=2
        "1w": timedelta(weeks=2),  # Changed from weeks=1 to weeks=2
        "1m": timedelta(days=35),  # Slightly increased from 30
        "3m": timedelta(days=95)  # Slightly increased from 90
    }

    # If period is not recognized, default to 1 week
    if period not in periods_map:
        logger.warning(f"Unrecognized period: {period}, defaulting to 1w")
        period = "1w"

    start_date = end_date - periods_map[period]
    return start_date, end_date


def filter_dataframe_by_date_range(df: pd.DataFrame, start_date: datetime, end_date: datetime,
                                   period: str = None) -> pd.DataFrame:
    """Filters a DataFrame by date range with fallbacks for empty results"""
    if df.empty:
        return df

    # Ensure timestamp is datetime
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


# ---- FASTAPI SETUP --------
# ------------------------------
app = FastAPI(
    title="BVL Live Tracker API",
    description="API for real-time stock data from AWS S3",
    version="2.1.0",
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


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
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
            "s3_bucket": S3_BUCKET_NAME,
            "s3_data_dir": S3_DATA_DIR,
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


@app.get("/monitor/s3", tags=["Monitoring"])
def monitor_s3():
    """Endpoint to monitor S3 status"""
    try:
        # List files in S3
        s3_files = list_s3_files(f"{S3_DATA_DIR}/")
        historical_files = list_s3_files(f"{S3_HISTORICAL_DIR}/")

        # Get metadata for each file
        files_info = []
        for s3_key in s3_files + historical_files:
            try:
                response = s3_client.head_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=s3_key
                )

                files_info.append({
                    "file": s3_key,
                    "size_kb": f"{response.get('ContentLength', 0) / 1024:.2f}",
                    "last_modified": response.get('LastModified').isoformat() if 'LastModified' in response else None,
                    "content_type": response.get('ContentType'),
                    "type": "historical" if s3_key.startswith(S3_HISTORICAL_DIR) else "current"
                })
            except Exception as e:
                files_info.append({
                    "file": s3_key,
                    "error": str(e),
                    "type": "historical" if s3_key.startswith(S3_HISTORICAL_DIR) else "current"
                })

        return {
            "bucket": S3_BUCKET_NAME,
            "region": AWS_REGION,
            "data_dir": S3_DATA_DIR,
            "historical_dir": S3_HISTORICAL_DIR,
            "current_file_count": len(s3_files),
            "historical_file_count": len(historical_files),
            "files": files_info
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error monitoring S3: {str(e)}")


@app.get("/monitor/metrics", tags=["Monitoring"])
def monitor_metrics():
    """Endpoint to view performance metrics"""
    # Add current stats
    current_metrics = PERFORMANCE_METRICS.copy()
    current_metrics["uptime_seconds"] = (datetime.now() - startup_time).total_seconds()
    current_metrics["cache_size"] = len(dataframes_cache)
    current_metrics["memory_usage_mb"] = cache_size / (1024 * 1024)
    current_metrics["cache_hit_ratio"] = (
        current_metrics["cache_hits"] /
        (current_metrics["cache_hits"] + current_metrics["cache_misses"])
        if (current_metrics["cache_hits"] + current_metrics["cache_misses"]) > 0
        else 0
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
    symbols_to_fetch = list_available_stocks_internal() if compare_all else [symbol]
    start_date, end_date = get_date_range_for_period(period)

    logger.info(f"Processing symbols: {symbols_to_fetch}, period: {period}")
    result = []

    for sym in symbols_to_fetch:
        try:
            # Get merged data with both historical and current
            combined_df = merge_historical_and_current_data(sym)

            if combined_df.empty:
                logger.warning(f"No data available for {sym}")
                continue

            # Filter by date range
            filtered_df = filter_dataframe_by_date_range(combined_df, start_date, end_date, period)

            if filtered_df.empty:
                logger.warning(f"No data for {sym} in period {period} after filtering")
                continue

            # Get the latest data for additional metrics
            last_current_data = get_latest_data(sym) or {}

            # Create time series points
            series_data = []
            for _, row in filtered_df.iterrows():
                if pd.notna(row['currentPrice']):
                    volume_value = (
                        float(row['volumen'])
                        if 'volumen' in row and pd.notna(row['volumen'])
                        else None
                    )

                    point = TimeSeriesPoint(
                        timestamp=row['timestamp'].isoformat(),
                        price=float(row['currentPrice']),
                        volume=volume_value,
                        open=(
                            float(row['open'])
                            if 'open' in row and pd.notna(row['open'])
                            else None
                        ),
                        day_low=(
                            float(row['dayLow'])
                            if 'dayLow' in row and pd.notna(row['dayLow'])
                            else None
                        ),
                        day_high=(
                            float(row['dayHigh'])
                            if 'dayHigh' in row and pd.notna(row['dayHigh'])
                            else None
                        )
                    )
                    series_data.append(point)

            # Get key metrics
            original_price = ORIGINAL_PRICES.get(sym)
            last_price = filtered_df['currentPrice'].iloc[-1] if not filtered_df.empty else None
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
                       and pd.notna(last_current_data['marketCap'])
                    else None
                ),
                trailing_pe=(
                    float(last_current_data['trailingPE'])
                    if last_current_data and 'trailingPE' in last_current_data
                       and pd.notna(last_current_data['trailingPE'])
                    else None
                ),
                dividend_yield=(
                    float(last_current_data['dividendYield'])
                    if last_current_data and 'dividendYield' in last_current_data
                       and pd.notna(last_current_data['dividendYield'])
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


@app.get("/api/timeseries-variations", response_model=TimeSeriesResponse, tags=["Data"])
async def get_time_series_variations(
        symbol: str = Query("BAP", description="Symbol to query (BAP, BRK-B, ILF)"),
        period: str = Query("1w", description="Period (realtime, 1d, 1w, 1m, 3m)"),
        compare_all: bool = Query(False, description="Show all symbols together")
):
    """Gets time series with price variation information"""
    symbols_to_fetch = list_available_stocks_internal() if compare_all else [symbol]
    start_date, end_date = get_date_range_for_period(period)

    logger.info(f"Processing symbols for variations: {symbols_to_fetch}, period: {period}")
    result = []

    for sym in symbols_to_fetch:
        try:
            # Get merged data with both historical and current
            combined_df = merge_historical_and_current_data(sym)

            if combined_df.empty:
                logger.warning(f"No data available for {sym}")
                continue

            # Filter by date range with fallbacks
            filtered_df = filter_dataframe_by_date_range(combined_df, start_date, end_date, period)

            if filtered_df.empty:
                logger.warning(f"No data for {sym} in period {period} after filtering")
                continue

            # Convert numeric columns
            numeric_cols = ['currentPrice', 'open', 'dayLow', 'dayHigh', 'volumen', 'previousClose']
            for col in numeric_cols:
                if col in filtered_df.columns:
                    filtered_df[col] = pd.to_numeric(filtered_df[col], errors='coerce')

            # Calculate variation percentages
            filtered_df = calculate_variation_percentage(filtered_df, period)

            # Get latest data for additional metrics
            last_current_data = get_latest_data(sym) or {}

            # Create time series points
            series_data = []
            for _, row in filtered_df.iterrows():
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

            # Get key metrics
            original_price = ORIGINAL_PRICES.get(sym)
            last_price = filtered_df['currentPrice'].iloc[
                -1] if not filtered_df.empty and 'currentPrice' in filtered_df.columns else None
            last_variation = filtered_df['variation_pct'].iloc[-1] if not filtered_df.empty else 0.0
            volatility = filtered_df['variation_pct'].std() if len(filtered_df) > 1 else 0

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
                    float(last_current_data.get('marketCap'))
                    if last_current_data and pd.notna(last_current_data.get('marketCap'))
                    else None
                ),
                trailing_pe=(
                    float(last_current_data.get('trailingPE'))
                    if last_current_data and pd.notna(last_current_data.get('trailingPE'))
                    else None
                ),
                dividend_yield=(
                    float(last_current_data.get('dividendYield'))
                    if last_current_data and pd.notna(last_current_data.get('dividendYield'))
                    else None
                ),
                fifty_two_week_range=last_current_data.get('fiftyTwoWeekRange'),
                daily_variation=float(last_variation),
                volatility=float(volatility)
            )
            result.append(symbol_series)

        except Exception as e:
            logger.error(f"Error processing {sym}: {str(e)}", exc_info=True)
            continue

    # If no data was found, return a helpful error
    if not result:
        raise StockAPIException(
            status_code=404,
            detail=f"No data found for the requested symbols and period",
            code="NO_DATA_FOUND"
        )

    return TimeSeriesResponse(series=result)


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
    return {
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "cached_symbols": list(dataframes_cache.keys()),
        "s3_bucket": S3_BUCKET_NAME,
        "uptime_seconds": (datetime.now() - startup_time).total_seconds(),
        "version": "2.1.0"
    }


# ---- HTML ENDPOINTS --------
# ------------------------------
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


@app.get("/api/symbols", tags=["Data"])
async def get_available_symbols():
    """Returns list of available symbols"""
    symbols = list_available_stocks_internal()
    return {"symbols": symbols}


# ---- STARTUP/SHUTDOWN EVENTS --------
# ------------------------------
startup_time = datetime.now()


def start_periodic_updates():
    """
    Starts a thread to periodically update data from S3
    with reduced frequency and staggered updates
    """

    def update_loop():
        while True:
            try:
                logger.info("Starting periodic data update from S3")

                # Check connection with S3
                try:
                    s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
                    logger.info(f"S3 bucket connection {S3_BUCKET_NAME} verified")
                except Exception as s3_err:
                    logger.error(f"S3 connection error: {str(s3_err)}")
                    time.sleep(60)  # Wait a minute before retrying
                    continue

                # Get all available symbols
                symbols = list_available_stocks_internal()
                logger.info(f"Symbols found in S3: {symbols}")

                # Update each symbol with a delay between them to avoid load spikes
                for symbol in symbols:
                    try:
                        logger.info(f"Updating data for {symbol} from S3")
                        load_dataframe(symbol, force_reload=True)
                        # Wait 5 seconds between each symbol
                        time.sleep(5)
                    except Exception as e:
                        logger.error(f"Error updating {symbol} from S3: {str(e)}")

                logger.info(f"Update completed for {len(symbols)} symbols from S3")

                # Wait 5 minutes (300 seconds) before the next update
                # Much friendlier than 60 seconds
                time.sleep(300)

            except Exception as e:
                logger.error(f"Error in S3 update loop: {str(e)}", exc_info=True)
                # Wait before retrying if there's an error
                time.sleep(30)

    # Start the update thread
    update_thread = threading.Thread(target=update_loop, daemon=True)
    update_thread.start()
    logger.info("Periodic S3 update thread started")


@app.on_event("startup")
def startup_event():
    """
    Event that runs on application startup

    Performs:
    - Initial S3 configuration
    - Starts the periodic updates thread
    - Logs the initial state
    """
    global startup_time
    startup_time = datetime.now()

    logger.info("Starting application with S3 integration...")
    logger.info(f"Cache configuration - Maximum: {MAX_CACHE_SIZE} items, TTL: {CACHE_ITEM_TTL}s")
    logger.info(f"S3 configuration - Bucket: {S3_BUCKET_NAME}, Region: {AWS_REGION}")

    # Check S3 connection at startup
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
        logger.info(f"S3 bucket connection {S3_BUCKET_NAME} verified")
    except Exception as e:
        logger.error(f"Error connecting to S3: {str(e)}")
        # Continue starting the application, but warn about the issue
        logger.warning("Application will continue, but there may be issues with S3")

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
        "main_aws:app",
        host="127.0.0.1",
        port=8080,
        reload=False,  # Disable reload in production
        workers=2,  # Reduce number of workers
        timeout_keep_alive=65,  # Longer keep-alive time
        log_level="info"
    )