# ---- IMPORT LIBRARIES --------
# ------------------------------
# Standard libraries
import os
import time
import threading
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from io import BytesIO, StringIO
import logger

# Third-party libraries
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.responses import FileResponse, JSONResponse, HTMLResponse
from starlette.requests import Request
from pydantic import BaseModel
import boto3
from botocore.exceptions import ClientError

# ---- AWS S3 CONFIGURATION --------
# ------------------------------
# Configure these values in your environment variables or AWS config
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'your-bucket-name')

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# ---- ORIGINAL CONFIGURATION (keep as is) --------
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

# Rest of your configuration remains the same...
MAX_CACHE_SIZE = 5
CACHE_ITEM_TTL = 600  # 5 minutes in seconds


# ---- MODIFIED HELPER FUNCTIONS FOR S3 --------
# ------------------------------
def get_s3_file_path(symbol: str) -> str:
    """Gets the S3 file path based on symbol"""
    if symbol.upper() == "BRK-B":
        filename = "brk-b_stock_data.csv"
    elif symbol.upper() == "ILF":
        filename = "ilf_etf_data.csv"
    else:
        filename = f"{symbol.lower()}_{'etf_data' if 'ETF' in symbol.upper() else 'stock_data'}.csv"

    return f"data/{filename}"


def get_s3_historical_file_path(symbol: str) -> str:
    """Gets the S3 historical file path based on symbol"""
    filename = f"{symbol.lower()}_historical.csv"
    return f"historical_data/{filename}"


def get_s3_file_last_modified(s3_path: str) -> float:
    """Gets file last modified timestamp from S3"""
    try:
        response = s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=s3_path)
        last_modified = response['LastModified']
        return last_modified.timestamp()
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return 0
        logger.error(f"Error getting S3 file modification date: {str(e)}")
        return 0


def load_dataframe_from_s3(s3_path: str) -> Optional[pd.DataFrame]:
    """Loads DataFrame from CSV in S3"""
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=s3_path)
        csv_content = response['Body'].read().decode('utf-8')
        return pd.read_csv(StringIO(csv_content))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.error(f"CSV file not found in S3: {s3_path}")
        else:
            logger.error(f"Error loading data from S3: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error processing data from S3: {str(e)}")
        return None


def load_dataframe(symbol: str, force_reload: bool = False) -> pd.DataFrame:
    """Loads DataFrame from S3 with optimized cache handling"""
    global dataframes_cache, cache_size

    s3_path = get_s3_file_path(symbol)
    current_modified_time = get_s3_file_last_modified(s3_path)

    with cache_lock:
        cache_item = dataframes_cache.get(symbol)

        if cache_item:
            cache_expired = (datetime.now() - cache_item.last_load_time).total_seconds() > CACHE_ITEM_TTL
            file_modified = current_modified_time > cache_item.last_modified

            if not force_reload and not cache_expired and not file_modified:
                cache_item.access_count += 1
                cache_logger.debug(f"Using cache for {symbol} (accesses: {cache_item.access_count})")
                return cache_item.df

        perf_logger.info(f"Loading data for {symbol} {'(forced)' if force_reload else ''}")
        start_time = time.time()

        try:
            df = load_dataframe_from_s3(s3_path)
            if df is None:
                return None

            # Rest of your DataFrame processing remains the same...
            cols_preview = df.head(5)
            needed_cols = ['timestamp', 'currentPrice', 'previousClose', 'open',
                           'dayLow', 'dayHigh', 'dividendYield', 'volumen',
                           'marketCap', 'trailingPE', 'fiftyTwoWeekRange']
            cols_to_use = [col for col in needed_cols if col in cols_preview.columns]
            df = df[cols_to_use] if cols_to_use else df

            columns_to_drop = [col for col in ['symbol', 'Symbol', 'Ticker'] if col in df.columns]
            df = df.drop(columns=columns_to_drop)

            required_cols = ['timestamp', 'currentPrice']
            for col in required_cols:
                if col not in df.columns:
                    logger.error(f"Required column '{col}' not found in {s3_path}")
                    return None

            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df = df.dropna(subset=['timestamp'])

            if 'currentPrice' in df.columns:
                df['currentPrice'] = pd.to_numeric(df['currentPrice'], errors='coerce', downcast='float')

            if 'volumen' in df.columns:
                df['volumen'] = pd.to_numeric(df['volumen'], errors='coerce', downcast='integer')
            elif 'volume' in df.columns:
                df['volumen'] = pd.to_numeric(df['volume'], errors='coerce', downcast='integer')
            else:
                df['volumen'] = 0

            numeric_cols = ['open', 'dayHigh', 'dayLow', 'marketCap', 'trailingPE', 'dividendYield']
            existing_numeric_cols = [col for col in numeric_cols if col in df.columns]
            for col in existing_numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce', downcast='float')

            if 'fiftyTwoWeekRange' in df.columns:
                df['fiftyTwoWeekRange'] = df['fiftyTwoWeekRange'].astype('category')

            df = df.sort_values('timestamp')
            df_size = df.memory_usage(deep=True).sum()
            perf_logger.info(
                f"DataFrame {symbol} loaded in {time.time() - start_time:.2f}s - Size: {df_size / 1024:.2f} KB")

            new_cache_item = CacheItem(df, current_modified_time)
            new_cache_item.access_count = 1

            if symbol in dataframes_cache:
                cache_size -= dataframes_cache[symbol].size

            dataframes_cache[symbol] = new_cache_item
            cache_size += df_size
            clear_cache_if_needed()

            return df

        except Exception as e:
            logger.error(f"Error loading data for {symbol}: {str(e)}", exc_info=True)
            return None


def load_historical_data(symbol: str):
    """Loads historical data from S3 including volume"""
    s3_path = get_s3_historical_file_path(symbol)

    try:
        df = load_dataframe_from_s3(s3_path)
        if df is None:
            return None

        df['timestamp'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
        df = df.dropna(subset=['timestamp'])
        df = df.rename(columns={'Open': 'currentPrice', 'Volume': 'volumen'})
        return df[['timestamp', 'currentPrice', 'volumen']]
    except Exception as e:
        logger.error(f"Error loading historical data for {symbol}: {str(e)}")
        return None


def background_update_all_dataframes():
    """Updates all dataframes in background"""
    try:
        symbols = list_available_stocks_internal()
        for symbol in symbols:
            load_dataframe(symbol, force_reload=True)
        logger.info(f"Background update completed for {len(symbols)} symbols")
    except Exception as e:
        logger.error(f"Error during background update: {str(e)}")

def calculate_percentage_change(price_series):
    """Calculates percentage change between first and last price in series"""
    if len(price_series) < 2:
        return 0.0

    first_price = price_series.iloc[0]
    last_price = price_series.iloc[-1]

    if first_price == 0:
        return 0.0

    return ((last_price - first_price) / first_price) * 100

def list_available_stocks_internal():
    """Lists all available symbols (internal version)"""
    try:
        root_path = get_project_root()
        data_dir = os.path.join(root_path, "data")
        files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]

        symbols = []
        for f in files:
            if f.startswith('brk-b'):
                symbols.append("BRK-B")
            else:
                base_name = f.split('_')[0].upper()
                if base_name not in symbols:
                    symbols.append(base_name)
        return sorted(symbols)
    except Exception as e:
        logger.error(f"Error listing stocks: {str(e)}")
        return []

def get_latest_data(symbol: str) -> Dict:
    """Gets latest data for a symbol"""
    df = load_dataframe(symbol)

    if df is None or df.empty:
        return None

    latest_data = df.iloc[-1].to_dict()

    for key, value in latest_data.items():
        if pd.isna(value):
            latest_data[key] = None

    latest_data['symbol'] = symbol

    required_columns = ['currentPrice', 'previousClose', 'open', 'dayLow', 'dayHigh',
                       'dividendYield', 'financialCurrency', 'volumen', 'timestamp']

    for col in required_columns:
        if col not in latest_data:
            latest_data[col] = None

    if latest_data['financialCurrency'] is None:
        latest_data['financialCurrency'] = "USD"

    if latest_data['currentPrice'] is None and symbol in ORIGINAL_PRICES:
        latest_data['currentPrice'] = ORIGINAL_PRICES[symbol]
        logger.warning(f"currentPrice is None for {symbol}, using original price {ORIGINAL_PRICES[symbol]} as fallback")
    elif latest_data['currentPrice'] is None:
        latest_data['currentPrice'] = 0.0
        logger.warning(f"currentPrice is None for {symbol} and no original price found, using 0.0 as fallback")

    try:
        latest_data['volumen'] = int(latest_data['volumen']) if latest_data['volumen'] is not None else 0
    except (ValueError, TypeError):
        latest_data['volumen'] = 0

    return latest_data

def get_historical_data(symbol: str, days: int = 30) -> List[Dict]:
    """Gets historical data for a symbol"""
    df = load_dataframe(symbol)

    if df is None or df.empty:
        return []

    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp', ascending=False)

    historical_df = df.head(days)
    records = historical_df.to_dict(orient='records')

    for record in records:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None
            elif key == 'timestamp' and isinstance(value, pd.Timestamp):
                record[key] = value.isoformat()

    return records

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
    description="API for real-time stock data",
    version="1.0.0"
)

# Static files and CORS configuration
static_dir = os.path.join(os.path.dirname(__file__), 'static')
app.mount("/static", StaticFiles(directory=static_dir), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- MONITORING ENDPOINTS --------
# ------------------------------
@app.get("/monitor/cache")
def monitor_cache():
    """Endpoint to monitor cache status"""
    with cache_lock:
        cache_info = {
            "total_items": len(dataframes_cache),
            "total_size": f"{cache_size / 1024:.2f} KB",
            "max_size": MAX_CACHE_SIZE,
            "item_ttl": CACHE_ITEM_TTL,
            "items": []
        }

        for symbol, item in dataframes_cache.items():
            cache_info["items"].append({
                "symbol": symbol,
                "size": f"{item.size / 1024:.2f} KB",
                "last_access": item.last_load_time.isoformat(),
                "access_count": item.access_count,
                "age_seconds": (datetime.now() - item.last_load_time).total_seconds()
            })

    return cache_info

@app.get("/monitor/logs")
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

# ---- MAIN ENDPOINTS --------
# ------------------------------
@app.get("/api/timeseries-with-profitability", response_model=TimeSeriesResponse)
async def get_time_series_with_profitability(
        symbol: str = Query("BAP", description="Symbol to query (BAP, BRK-B, ILF)"),
        period: str = Query("1w", description="Period (realtime, 1d, 1w, 1m, 3m)"),
        compare_all: bool = Query(False, description="Show all symbols together")
):
    """Gets time series with profitability information"""
    periods_map = {
        "realtime": timedelta(days=1),
        "1d": timedelta(days=1),
        "1w": timedelta(weeks=1),
        "1m": timedelta(days=30),
        "3m": timedelta(days=90)
    }

    symbols_to_fetch = ["BAP", "BRK-B", "ILF"] if compare_all else [symbol]
    end_date = datetime.now()
    transition_date = datetime(2025, 4, 5)

    logger.info(f"Processing symbols: {symbols_to_fetch}, period: {period}")
    result = []

    for sym in symbols_to_fetch:
        try:
            current_df = load_dataframe(sym)
            if current_df is None:
                current_df = pd.DataFrame()

            hist_df = load_historical_data(sym)

            if hist_df is not None:
                hist_df = hist_df[hist_df['timestamp'] < transition_date]
                logger.info(f"Historical data for {sym}: {len(hist_df)} records")

            if not current_df.empty:
                current_df = current_df[current_df['timestamp'] >= transition_date]
                logger.info(f"Current data for {sym}: {len(current_df)} records")

            combined_df = pd.concat([hist_df, current_df], ignore_index=True) if hist_df is not None else current_df

            if combined_df.empty:
                logger.warning(f"No combined data for {sym}")
                continue

            if period == "realtime":
                today = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
                filtered_df = combined_df[(combined_df['timestamp'] >= today) &
                                        (combined_df['timestamp'] <= end_date)]
                filtered_df = filtered_df[
                    (filtered_df['timestamp'].dt.hour >= 8) &
                    (filtered_df['timestamp'].dt.hour <= 16)
                ]
            else:
                start_date = end_date - periods_map[period]
                filtered_df = combined_df[(combined_df['timestamp'] >= start_date) &
                                        (combined_df['timestamp'] <= end_date)]

            filtered_df = filtered_df.sort_values('timestamp')

            logger.info(f"Filtered data for {sym} ({period}): {len(filtered_df)} records")

            if filtered_df.empty:
                logger.warning(f"No data for {sym} in period {period}")
                continue

            last_current_data = get_latest_data(sym) or {}

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

            original_price = ORIGINAL_PRICES.get(sym)
            last_price = filtered_df['currentPrice'].iloc[-1] if not filtered_df.empty else None

            symbol_series = SymbolTimeSeries(
                symbol=sym,
                data=series_data,
                period=period,
                original_price=original_price,
                current_price=last_price,
                current_profitability=(
                    ((last_price - original_price) / original_price) * 100
                    if last_price and original_price and original_price > 0
                    else None
                ),
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

    return TimeSeriesResponse(series=result)

@app.get("/api/timeseries-variations", response_model=TimeSeriesResponse)
async def get_time_series_variations(
        symbol: str = Query("BAP", description="Symbol to query (BAP, BRK-B, ILF)"),
        period: str = Query("1w", description="Period (realtime, 1d, 1w, 1m, 3m)"),
        compare_all: bool = Query(False, description="Show all symbols together")
):
    """Gets time series with price variation information"""
    periods_map = {
        "realtime": timedelta(hours=6),
        "1d": timedelta(days=1),
        "1w": timedelta(weeks=1),
        "1m": timedelta(days=30),
        "3m": timedelta(days=90)
    }

    symbols_to_fetch = ["BAP", "BRK-B", "ILF"] if compare_all else [symbol]
    end_date = datetime.now()
    transition_date = datetime(2025, 4, 5)

    logger.info(f"Processing symbols: {symbols_to_fetch}, period: {period}")
    result = []

    for sym in symbols_to_fetch:
        try:
            current_df = load_dataframe(sym)
            current_df = pd.DataFrame() if current_df is None or current_df.empty else current_df

            hist_df = load_historical_data(sym)
            hist_df = pd.DataFrame() if hist_df is None or hist_df.empty else hist_df

            hist_df = hist_df[hist_df['timestamp'] < transition_date] if not hist_df.empty else hist_df
            current_df = current_df[current_df['timestamp'] >= transition_date] if not current_df.empty else current_df

            combined_df = pd.concat([hist_df, current_df], ignore_index=True)

            if combined_df.empty:
                logger.warning(f"No combined data for {sym}")
                continue

            start_date = end_date - periods_map[period]
            filtered_df = combined_df[
                (combined_df['timestamp'] >= start_date) &
                (combined_df['timestamp'] <= end_date)
            ].copy()

            if period == "realtime":
                market_open = end_date.replace(hour=8, minute=0, second=0, microsecond=0)
                market_close = end_date.replace(hour=16, minute=0, second=0, microsecond=0)
                filtered_df = filtered_df[
                    (filtered_df['timestamp'] >= market_open) &
                    (filtered_df['timestamp'] <= market_close)
                ]

            if filtered_df.empty:
                logger.warning(f"No data for {sym} in period {period}")
                continue

            numeric_cols = ['currentPrice', 'open', 'dayLow', 'dayHigh', 'volumen', 'previousClose']
            for col in numeric_cols:
                if col in filtered_df.columns:
                    filtered_df[col] = pd.to_numeric(filtered_df[col], errors='coerce')

            if period == "realtime":
                filtered_df['variation_pct'] = filtered_df['currentPrice'].pct_change() * 100
            else:
                first_valid_price = filtered_df['currentPrice'].first_valid_index()
                if first_valid_price is not None:
                    first_price = filtered_df.loc[first_valid_price, 'currentPrice']
                    filtered_df['variation_pct'] = ((filtered_df['currentPrice'] - first_price) / first_price) * 100
                else:
                    filtered_df['variation_pct'] = 0.0

            filtered_df['variation_pct'] = filtered_df['variation_pct'].replace([np.inf, -np.inf], np.nan).fillna(0)

            last_current_data = get_latest_data(sym) or {}

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

            original_price = ORIGINAL_PRICES.get(sym)
            last_price = filtered_df['currentPrice'].iloc[-1] if not filtered_df.empty and 'currentPrice' in filtered_df.columns else None
            last_variation = filtered_df['variation_pct'].iloc[-1] if not filtered_df.empty else 0.0
            volatility = filtered_df['variation_pct'].std() if len(filtered_df) > 1 else 0

            symbol_series = SymbolTimeSeries(
                symbol=sym,
                data=series_data,
                period=period,
                original_price=original_price,
                current_price=last_price,
                current_profitability=(
                    ((last_price - original_price) / original_price) * 100
                    if last_price and original_price and original_price > 0
                    else None
                ),
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

    return TimeSeriesResponse(series=result)

@app.get("/portfolio/holdings/live", response_model=PortfolioHoldings)
def get_portfolio_holdings_live():
    """Gets real-time portfolio holdings data"""
    holdings = []
    portfolio_total_value = 0
    portfolio_todays_change_value = 0
    portfolio_total_gain_loss = 0
    portfolio_previous_value = 0

    for symbol, data in PORTFOLIO_DATA.items():
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

@app.post("/refresh")
def refresh_data(background_tasks: BackgroundTasks):
    """Forces a refresh of all cached data"""
    background_tasks.add_task(background_update_all_dataframes)
    return {"message": "Background data refresh started"}

@app.get("/health")
def health_check():
    """API health check endpoint"""
    return {
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "cached_symbols": list(dataframes_cache.keys())
    }

# ---- HTML ENDPOINTS --------
# ------------------------------
@app.get("/html/timeseries-profitability", response_class=HTMLResponse)
async def get_timeseries_profitability_html():
    """Serves the full analysis interface"""
    return FileResponse(os.path.join(static_dir, "timeseries-profitability.html"))

@app.get("/html/market/variations", response_class=HTMLResponse)
async def get_market_variations_html():
    """Serves the price variations HTML page"""
    return FileResponse(os.path.join(static_dir, "market_variations.html"))

@app.get("/html/portfolio/holdings", response_class=HTMLResponse)
async def get_portfolio_holdings_html():
    """Serves the portfolio holdings HTML page"""
    return FileResponse(os.path.join(static_dir, "portfolio_holdings.html"))

@app.get("/html")
async def get_html_index():
    """Serves the main HTML page"""
    return FileResponse(os.path.join(static_dir, "index.html"))

# ---- STARTUP/SHUTDOWN EVENTS --------
# ------------------------------

def start_periodic_updates():
    """
    Inicia un hilo para actualizar periódicamente todos los datos
    con frecuencia reducida y actualizaciones escalonadas
    """

    def update_loop():
        while True:
            try:
                logger.info("Iniciando actualización periódica de datos")

                # Obtener todos los símbolos disponibles
                symbols = list_available_stocks_internal()

                # Actualizar cada símbolo con un retraso entre ellos para evitar picos de carga
                for symbol in symbols:
                    try:
                        logger.info(f"Actualizando datos para {symbol}")
                        load_dataframe(symbol, force_reload=True)
                        # Esperar 5 segundos entre cada símbolo
                        time.sleep(5)
                    except Exception as e:
                        logger.error(f"Error actualizando {symbol}: {str(e)}")

                logger.info(f"Actualización completada para {len(symbols)} símbolos")

                # Esperar 5 minutos (300 segundos) antes de la próxima actualización
                # Mucho más amigable que 60 segundos
                time.sleep(300)

            except Exception as e:
                logger.error(f"Error en el bucle de actualización: {str(e)}")
                # Si hay un error, esperar antes de reintentar
                time.sleep(30)

    # Iniciar el hilo de actualización
    update_thread = threading.Thread(target=update_loop, daemon=True)
    update_thread.start()
    logger.info("Hilo de actualización periódica iniciado")


@app.on_event("startup")
def startup_event():
    """
    Evento que se ejecuta al iniciar la aplicación

    Realiza:
    - Configuración inicial
    - Inicio del hilo de actualizaciones periódicas
    - Logging del estado inicial
    """
    logger.info("Iniciando aplicación...")
    logger.info(f"Configuración de caché - Máximo: {MAX_CACHE_SIZE} items, TTL: {CACHE_ITEM_TTL}s")
    start_periodic_updates()
    logger.info("API inicializada y actualización periódica configurada")


@app.on_event("shutdown")
def shutdown_event():
    """
    Evento que se ejecuta al detener la aplicación

    Realiza:
    - Limpieza de la caché
    - Logging del estado de cierre
    """
    logger.info("Deteniendo aplicación...")
    with cache_lock:
        logger.info(f"Limpiando caché ({len(dataframes_cache)} items)")
        dataframes_cache.clear()


if __name__ == "__main__":
    import uvicorn

    # Configuración más robusta para el servidor
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,  # Deshabilitar reload en producción
        workers=2,  # Reducir número de workers
        timeout_keep_alive=65,  # Mayor tiempo de keep-alive
        log_level="info"
    )
