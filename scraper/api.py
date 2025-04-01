# api_server.py
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
from typing import List, Dict, Optional
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define the fields we want to expose
REQUESTED_FIELDS = [
    'previousClose', 'open', 'dayLow', 'dayHigh',
    'dividendRate', 'dividendYield', 'volume',
    'regularMarketVolume', 'averageVolume'
]

# Path to your CSV files (modify this if needed)
CSV_DIR = "E:/papx/end_to_end_ml/nb_pr/tickets_live_tracker/scraper/data"

app = FastAPI(
    title="Stock Data API",
    description="API to serve stock data collected from Yahoo Finance",
    version="1.0.0"
)


def get_ticker_filename(ticker: str) -> str:
    """Convert ticker symbol to corresponding filename."""
    safe_ticker = ticker.replace('.', '-')  # Handle cases like BRK-B
    return f"{safe_ticker}_stockdata.csv"


def get_latest_data_for_ticker(ticker: str) -> Optional[Dict]:
    """Get the latest data record for a specific ticker."""
    filename = get_ticker_filename(ticker)
    filepath = os.path.join(CSV_DIR, filename)

    if not os.path.exists(filepath):
        logger.warning(f"File not found for ticker {ticker}: {filepath}")
        return None

    try:
        df = pd.read_csv(filepath)
        if df.empty:
            logger.warning(f"Empty CSV file for ticker {ticker}")
            return None

        # Get the most recent record (assuming data is appended chronologically)
        latest_record = df.iloc[-1].to_dict()

        # Filter only the requested fields
        filtered_data = {
            field: latest_record.get(field, None)
            for field in REQUESTED_FIELDS
        }

        # Add ticker symbol and timestamp for reference
        filtered_data['ticker'] = ticker
        filtered_data['timestamp'] = latest_record.get('timestamp', None)

        return filtered_data

    except Exception as e:
        logger.error(f"Error reading data for ticker {ticker}: {str(e)}")
        return None


@app.get("/api/stocks/{ticker}", response_model=Dict[str, Optional[float]])
async def get_stock_data(ticker: str):
    """Get the latest stock data for a specific ticker."""
    data = get_latest_data_for_ticker(ticker)
    if data is None:
        raise HTTPException(status_code=404, detail=f"Data not found for ticker {ticker}")
    return data


@app.get("/api/stocks/", response_model=Dict[str, Dict[str, Optional[float]]])
async def get_all_stocks():
    """Get the latest data for all available tickers."""
    all_data = {}

    # Scan the directory for CSV files
    try:
        for filename in os.listdir(CSV_DIR):
            if filename.endswith("_stockdata.csv"):
                ticker = filename.replace("_stockdata.csv", "").replace("-", ".")
                data = get_latest_data_for_ticker(ticker)
                if data:
                    all_data[ticker] = data
    except Exception as e:
        logger.error(f"Error scanning directory: {str(e)}")
        raise HTTPException(status_code=500, detail="Error reading stock data")

    if not all_data:
        raise HTTPException(status_code=404, detail="No stock data found")

    return all_data


if __name__ == "__main__":
    import uvicorn
    from socket import socket


    # Function to find available port
    def find_free_port():
        with socket() as s:
            s.bind(('', 0))
            return s.getsockname()[1]


    port = find_free_port()
    print(f"Starting server on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)