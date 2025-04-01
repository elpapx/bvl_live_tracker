import yfinance as yf
import pandas as pd
import logging
import os
import time
from datetime import datetime, timedelta
from flask import Flask, jsonify
import threading

app = Flask(__name__)

# Configuration
DATA_DIR = "data"
LOG_DIR = "logs"
TICKERS = ["BAP", "ILF", "BRK-B"]
INTERVAL_MINUTES = 10
TOTAL_HOURS = 7
REQUESTED_FIELDS = [
    'previousClose', 'open', 'dayLow', 'dayHigh',
    'dividendRate', 'dividendYield', 'volume',
    'regularMarketVolume', 'averageVolume'
]


# Setup logging
def setup_logging():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(os.path.join(LOG_DIR, "stock_api.log")),
            logging.StreamHandler()
        ]
    )


setup_logging()
logger = logging.getLogger(__name__)


def get_stock_data(ticker):
    """Fetch current stock data from Yahoo Finance"""
    try:
        stock = yf.Ticker(ticker)
        data = stock.info
        if not data:
            logger.warning(f"No data retrieved for {ticker}")
            return None

        # Add timestamp and filter only requested fields
        filtered_data = {'timestamp': datetime.now().isoformat()}
        for field in REQUESTED_FIELDS:
            filtered_data[field] = data.get(field, None)

        return filtered_data
    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {str(e)}")
        return None


def save_data_to_csv(ticker, data):
    """Save stock data to CSV file"""
    if not data:
        return False

    try:
        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)

        filename = f"{ticker.replace('.', '_')}_data.csv"
        filepath = os.path.join(DATA_DIR, filename)

        df = pd.DataFrame([data])
        header = not os.path.exists(filepath)

        df.to_csv(filepath, mode='a', header=header, index=False)
        return True
    except Exception as e:
        logger.error(f"Error saving data for {ticker}: {str(e)}")
        return False


def data_collection_worker():
    """Background worker for periodic data collection"""
    end_time = datetime.now() + timedelta(hours=TOTAL_HOURS)
    cycle = 0

    while datetime.now() < end_time:
        cycle += 1
        logger.info(f"\nStarting data collection cycle {cycle}")

        for ticker in TICKERS:
            data = get_stock_data(ticker)
            if data:
                save_data_to_csv(ticker, data)
                logger.info(f"Saved data for {ticker}")

        # Calculate sleep time, checking every minute to avoid overshooting
        sleep_until = datetime.now() + timedelta(minutes=INTERVAL_MINUTES)
        while datetime.now() < sleep_until and datetime.now() < end_time:
            time.sleep(60)  # Check every minute

    logger.info("Data collection completed")


# API Endpoints
@app.route('/api/stocks', methods=['GET'])
def get_all_stocks():
    """Get latest data for all stocks"""
    response = {}
    for ticker in TICKERS:
        data = get_stock_data(ticker)
        if data:
            response[ticker] = data
    return jsonify(response)


@app.route('/api/stocks/<ticker>', methods=['GET'])
def get_single_stock(ticker):
    """Get latest data for a specific stock"""
    if ticker not in TICKERS:
        return jsonify({"error": "Ticker not monitored"}), 404

    data = get_stock_data(ticker)
    if not data:
        return jsonify({"error": "Could not fetch data"}), 500

    return jsonify(data)


@app.route('/api/history/<ticker>', methods=['GET'])
def get_historical_data(ticker):
    """Get all historical data collected for a stock"""
    if ticker not in TICKERS:
        return jsonify({"error": "Ticker not monitored"}), 404

    try:
        filename = f"{ticker.replace('.', '_')}_data.csv"
        filepath = os.path.join(DATA_DIR, filename)

        if not os.path.exists(filepath):
            return jsonify({"error": "No historical data available"}), 404

        df = pd.read_csv(filepath)
        return jsonify(df.to_dict(orient='records'))
    except Exception as e:
        logger.error(f"Error reading historical data: {str(e)}")
        return jsonify({"error": "Could not read data"}), 500


if __name__ == '__main__':
    # Start data collection in background thread
    collector_thread = threading.Thread(target=data_collection_worker, daemon=True)
    collector_thread.start()

    # Start Flask API
    app.run(host='0.0.0.0', port=5000, debug=False)