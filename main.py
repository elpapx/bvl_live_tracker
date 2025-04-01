import pandas as pd
import os
import glob
from typing import Dict, List, Optional, Union, Tuple
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import numpy as np


class StockDataAPI:
    """API for accessing and analyzing collected stock data."""

    def __init__(self, data_dir: str = "data"):
        """
        Initialize the Stock Data API.

        Args:
            data_dir: Directory containing the CSV files with stock data
        """
        self.data_dir = data_dir
        self.data_cache: Dict[str, pd.DataFrame] = {}

    def get_available_symbols(self) -> List[str]:
        """
        Get a list of all available stock symbols.

        Returns:
            List of stock symbols found in the data directory
        """
        # Find all CSV files in the data directory
        csv_pattern = os.path.join(self.data_dir, "*_stock_data.csv")
        csv_files = glob.glob(csv_pattern)

        # Extract symbol from filename
        symbols = []
        for file_path in csv_files:
            filename = os.path.basename(file_path)
            # Convert from lowercase_with_underscores back to original format
            if "_" in filename:
                symbol = filename.split("_stock_data.csv")[0].upper()
                # Handle special case for BRK-B
                if symbol == "BRK_B":
                    symbol = "BRK-B"
                symbols.append(symbol)

        return sorted(symbols)

    def load_data(self, symbol: str, force_reload: bool = False) -> pd.DataFrame:
        """
        Load data for a specific symbol.

        Args:
            symbol: Stock symbol to load
            force_reload: Whether to force reload from disk even if cached

        Returns:
            DataFrame containing the stock data
        """
        # Check cache first unless forced to reload
        if symbol in self.data_cache and not force_reload:
            return self.data_cache[symbol]

        # Format the symbol for filename matching
        file_symbol = symbol.lower().replace("-", "_")
        file_path = os.path.join(self.data_dir, f"{file_symbol}_stock_data.csv")

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"No data file found for symbol {symbol}")

        # Load the data
        df = pd.read_csv(file_path)

        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Set timestamp as index
        df = df.set_index('timestamp').sort_index()

        # Store in cache
        self.data_cache[symbol] = df

        return df

    def get_latest_price(self, symbol: str) -> float:
        """
        Get the most recent price for a symbol.

        Args:
            symbol: Stock symbol

        Returns:
            Latest price value
        """
        df = self.load_data(symbol)
        return df['currentPrice'].iloc[-1]

    def get_price_history(self, symbol: str,
                          start_date: Optional[Union[str, datetime]] = None,
                          end_date: Optional[Union[str, datetime]] = None) -> pd.Series:
        """
        Get price history for a symbol within a date range.

        Args:
            symbol: Stock symbol
            start_date: Start date (None for all available)
            end_date: End date (None for all available)

        Returns:
            Series of prices indexed by timestamp
        """
        df = self.load_data(symbol)

        # Filter by date range if specified
        if start_date:
            df = df[df.index >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df.index <= pd.to_datetime(end_date)]

        return df['currentPrice']

    def get_daily_summary(self, symbol: str, date: Optional[Union[str, datetime]] = None) -> Dict:
        """
        Get a summary of a symbol's data for a specific day.

        Args:
            symbol: Stock symbol
            date: The date to summarize (None for most recent day)

        Returns:
            Dictionary with summary statistics
        """
        df = self.load_data(symbol)

        if date:
            # Convert to datetime and get that day's data
            date = pd.to_datetime(date).date()
            day_data = df[df.index.date == date]
        else:
            # Get the most recent day
            latest_date = df.index.date.max()
            day_data = df[df.index.date == latest_date]

        if day_data.empty:
            return {"error": f"No data available for {symbol} on the specified date"}

        # Calculate summary metrics
        summary = {
            "symbol": symbol,
            "date": day_data.index.date[0].strftime("%Y-%m-%d"),
            "open": day_data['currentPrice'].iloc[0],
            "close": day_data['currentPrice'].iloc[-1],
            "high": day_data['currentPrice'].max(),
            "low": day_data['currentPrice'].min(),
            "change": day_data['currentPrice'].iloc[-1] - day_data['currentPrice'].iloc[0],
            "change_percent": ((day_data['currentPrice'].iloc[-1] / day_data['currentPrice'].iloc[0]) - 1) * 100,
            "observations": len(day_data)
        }

        return summary

    def calculate_metrics(self, symbol: str, days: int = 7) -> Dict:
        """
        Calculate various metrics for a symbol over a period.

        Args:
            symbol: Stock symbol
            days: Number of days to look back

        Returns:
            Dictionary with various metrics
        """
        df = self.load_data(symbol)

        # Get data for the specified period
        end_date = df.index.max()
        start_date = end_date - timedelta(days=days)
        period_data = df[df.index >= start_date]

        if period_data.empty:
            return {"error": f"Not enough data for {symbol} in the last {days} days"}

        # Calculate metrics
        price_series = period_data['currentPrice']
        metrics = {
            "symbol": symbol,
            "period_days": days,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "start_price": price_series.iloc[0],
            "end_price": price_series.iloc[-1],
            "min_price": price_series.min(),
            "max_price": price_series.max(),
            "mean_price": price_series.mean(),
            "std_dev": price_series.std(),
            "total_return": ((price_series.iloc[-1] / price_series.iloc[0]) - 1) * 100,
            "volatility": price_series.pct_change().std() * 100,  # Daily volatility as percentage
        }

        return metrics

    def plot_price_chart(self, symbols: Union[str, List[str]],
                         days: int = 7,
                         save_path: Optional[str] = None) -> None:
        """
        Plot price chart for one or more symbols.

        Args:
            symbols: Stock symbol or list of symbols
            days: Number of days to look back
            save_path: Path to save the plot (None to display)
        """
        if isinstance(symbols, str):
            symbols = [symbols]

        plt.figure(figsize=(12, 6))

        for symbol in symbols:
            try:
                df = self.load_data(symbol)

                # Get data for the specified period
                end_date = df.index.max()
                start_date = end_date - timedelta(days=days)
                period_data = df[df.index >= start_date]

                if not period_data.empty:
                    plt.plot(period_data.index, period_data['currentPrice'], label=symbol)
            except Exception as e:
                print(f"Error plotting {symbol}: {e}")

        plt.title(f"Stock Price Comparison - Last {days} Days")
        plt.xlabel("Date")
        plt.ylabel("Price")
        plt.legend()
        plt.grid(True)

        if save_path:
            plt.savefig(save_path)
        else:
            plt.show()

    def compare_performance(self, symbols: List[str], days: int = 7) -> pd.DataFrame:
        """
        Compare performance of multiple stocks.

        Args:
            symbols: List of stock symbols to compare
            days: Number of days to compare

        Returns:
            DataFrame with normalized performance
        """
        result = pd.DataFrame()

        for symbol in symbols:
            try:
                df = self.load_data(symbol)

                # Get data for the specified period
                end_date = df.index.max()
                start_date = end_date - timedelta(days=days)
                period_data = df[df.index >= start_date]

                if not period_data.empty:
                    # Normalize to starting value = 100
                    normalized = (period_data['currentPrice'] / period_data['currentPrice'].iloc[0]) * 100
                    result[symbol] = normalized
            except Exception as e:
                print(f"Error processing {symbol}: {e}")

        return result

    def export_to_csv(self, symbol: str, output_path: Optional[str] = None) -> str:
        """
        Export data for a symbol to a new CSV file.

        Args:
            symbol: Stock symbol
            output_path: Path for output file (None for auto-generated)

        Returns:
            Path to the exported file
        """
        df = self.load_data(symbol)

        # Generate output path if not provided
        if not output_path:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(self.data_dir, f"{symbol}_export_{timestamp}.csv")

        # Reset index to include timestamp as column
        export_df = df.reset_index()

        # Export to CSV
        export_df.to_csv(output_path, index=False)

        return output_path

    def calculate_correlation(self, symbols: List[str], days: int = 30) -> pd.DataFrame:
        """
        Calculate price correlation between multiple symbols.

        Args:
            symbols: List of stock symbols
            days: Number of days to analyze

        Returns:
            Correlation matrix
        """
        # Collect price data for all symbols
        price_data = {}

        for symbol in symbols:
            try:
                df = self.load_data(symbol)

                # Get data for the specified period
                end_date = df.index.max()
                start_date = end_date - timedelta(days=days)
                period_data = df[df.index >= start_date]

                if not period_data.empty:
                    price_data[symbol] = period_data['currentPrice']
            except Exception as e:
                print(f"Error processing {symbol}: {e}")

        # Create DataFrame with all price series
        combined = pd.DataFrame(price_data)

        # Calculate correlation matrix
        return combined.corr()


# Example usage
if __name__ == "__main__":
    api = StockDataAPI()

    # Display available symbols
    symbols = api.get_available_symbols()
    print(f"Available symbols: {symbols}")

    if symbols:
        # Choose first symbol for demonstration
        symbol = symbols[0]

        # Get latest price
        latest_price = api.get_latest_price(symbol)
        print(f"\nLatest price for {symbol}: ${latest_price:.2f}")

        # Get daily summary
        summary = api.get_daily_summary(symbol)
        print(f"\nDaily summary for {symbol}:")
        for key, value in summary.items():
            print(f"  {key}: {value}")

        # Calculate metrics
        metrics = api.calculate_metrics(symbol)
        print(f"\nMetrics for {symbol} over {metrics['period_days']} days:")
        for key, value in metrics.items():
            print(f"  {key}: {value}")

        # Compare if multiple symbols available
        if len(symbols) > 1:
            comparison = api.compare_performance(symbols[:3])  # Compare first 3 symbols
            print(f"\nPerformance comparison (normalized to 100):")
            print(comparison.head())

            # Calculate correlation
            correlation = api.calculate_correlation(symbols[:3])
            print(f"\nPrice correlation:")
            print(correlation)

        # Plot price chart for demonstration
        print("\nGenerating price chart...")
        api.plot_price_chart(symbols[:3], save_path="price_comparison.png")
        print("Chart saved to price_comparison.png")