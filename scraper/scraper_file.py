import yfinance as yf
import pandas as pd
import time
import os
import logging
from datetime import datetime
import argparse
from typing import List, Dict, Any, Optional


class StockDataCollector:
    """Class to collect stock data from Yahoo Finance at regular intervals."""

    def __init__(
            self,
            symbols: List[str],
            output_dir: str = "data",
            interval_minutes: int = 5,
            total_runtime_minutes: int = 420,
            columns: Optional[List[str]] = None
    ):
        """
        Initialize the stock data collector.

        Args:
            symbols: List of stock symbols to track
            output_dir: Directory to save CSV files
            interval_minutes: Time between data collection in minutes
            total_runtime_minutes: Total runtime in minutes
            columns: Specific data columns to extract (None for all available)
        """
        self.symbols = symbols
        self.output_dir = output_dir
        self.interval_minutes = interval_minutes
        self.total_runtime_minutes = total_runtime_minutes
        self.iterations = total_runtime_minutes // interval_minutes

        # Default columns to collect if none specified
        self.columns = columns or [
            "currentPrice", "previousClose", "open", "dayLow", "dayHigh",
            "bid", "ask", "volume", "averageVolume", "dividendYield",
            "earningsGrowth", "revenueGrowth", "grossMargins",
            "ebitdaMargins", "operatingMargins", "financialCurrency",
            "returnOnAssets", "returnOnEquity", "bookValue", "priceToBook"
        ]

        # Set up logging
        self.setup_logging()

        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)

    def setup_logging(self):
        """Configure logging settings."""
        log_dir = os.path.join(self.output_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)

        log_file = os.path.join(log_dir, f"stock_collection_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

        # Configure logging to handle encoding issues
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def get_csv_path(self, symbol: str) -> str:
        """Get the CSV file path for a specific symbol."""
        return os.path.join(self.output_dir, f"{symbol.replace('-', '_').lower()}_stock_data.csv")

    def fetch_stock_data(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch stock data for a specific symbol.

        Args:
            symbol: Stock symbol to fetch

        Returns:
            Dictionary containing the requested stock data
        """
        try:
            ticker = yf.Ticker(symbol)

            # Get the current price using history() which is more reliable
            current_price = ticker.history(period="1d")["Close"].iloc[-1]

            # Get the rest of the info
            stock_info = ticker.info

            # Extract requested columns
            extracted_data = {key: stock_info.get(key, None) for key in self.columns}

            # Override currentPrice with the value from history()
            extracted_data["currentPrice"] = current_price

            # Add metadata
            extracted_data["symbol"] = symbol
            extracted_data["timestamp"] = pd.Timestamp.now()

            return extracted_data

        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {e}")
            return {"symbol": symbol, "timestamp": pd.Timestamp.now(), "error": str(e)}

    def save_data(self, data: Dict[str, Any]) -> None:
        """
        Save the collected data to a CSV file.

        Args:
            data: Dictionary containing stock data
        """
        try:
            symbol = data["symbol"]
            csv_path = self.get_csv_path(symbol)

            df = pd.DataFrame([data])

            # Write to CSV
            file_exists = os.path.exists(csv_path)
            df.to_csv(
                csv_path,
                mode='a',
                header=not file_exists,
                index=False
            )

            # Use ASCII characters instead of emoji to avoid encoding issues
            self.logger.info(f"[OK] Data saved for {symbol} - Price: {data.get('currentPrice', 'N/A')}")

        except Exception as e:
            self.logger.error(f"Error saving data for {data.get('symbol', 'unknown')}: {e}")

    def collect_data_for_all_symbols(self) -> None:
        """Collect and save data for all configured symbols."""
        for symbol in self.symbols:
            try:
                data = self.fetch_stock_data(symbol)
                self.save_data(data)
            except Exception as e:
                self.logger.error(f"Failed to process {symbol}: {e}")

    def run(self) -> None:
        """Run the data collection process for the configured duration."""
        self.logger.info(f"Starting data collection for symbols: {', '.join(self.symbols)}")
        self.logger.info(
            f"Data will be collected every {self.interval_minutes} minutes for {self.total_runtime_minutes} minutes")

        start_time = datetime.now()

        for iteration in range(self.iterations):
            try:
                self.logger.info(f"Collection cycle {iteration + 1}/{self.iterations}")
                self.collect_data_for_all_symbols()

                # Don't sleep on the last iteration
                if iteration < self.iterations - 1:
                    next_collection = start_time.timestamp() + ((iteration + 1) * self.interval_minutes * 60)
                    sleep_seconds = next_collection - datetime.now().timestamp()

                    # Ensure we don't have negative sleep time due to processing delays
                    if sleep_seconds > 0:
                        self.logger.info(f"Sleeping for {sleep_seconds:.1f} seconds until next collection")
                        time.sleep(sleep_seconds)
                    else:
                        self.logger.warning("Processing took longer than interval! Continuing immediately")

            except Exception as e:
                self.logger.error(f"Error in collection cycle {iteration + 1}: {e}")

        self.logger.info(f"Data collection completed after {self.total_runtime_minutes} minutes")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Collect stock data from Yahoo Finance")

    parser.add_argument("--symbols", nargs="+", default=["BAP", "ILF", "BRK-B"],
                        help="List of stock symbols to track")

    parser.add_argument("--output-dir", type=str, default="data",
                        help="Directory to save CSV files")

    parser.add_argument("--interval", type=int, default=5,
                        help="Time between data collection in minutes")

    parser.add_argument("--duration", type=int, default=420,
                        help="Total runtime in minutes")

    return parser.parse_args()


if __name__ == "__main__":
    # Fix for Windows console encoding issues
    if os.name == 'nt':
        # For Windows systems
        # Try to change console code page to support Unicode
        try:
            import subprocess

            subprocess.run(["chcp", "65001"], shell=True, check=False)
        except Exception:
            pass  # Ignore if it fails, we'll use ASCII characters instead

    args = parse_arguments()

    collector = StockDataCollector(
        symbols=args.symbols,
        output_dir=args.output_dir,
        interval_minutes=args.interval,
        total_runtime_minutes=args.duration
    )

    collector.run()