from fastapi import FastAPI, HTTPException
from pathlib import Path
import pandas as pd
import uvicorn
import os
from datetime import datetime
from typing import List, Optional
from fastapi import Query  # Añade esto al inicio con los demás imports
import matplotlib.pyplot as plt
import seaborn as sns
import io
from fastapi.responses import StreamingResponse
# Importamos las clases desde tu módulo
from csv_processor import StockPriceExtractor, StockPriceColumnExtractor



app = FastAPI(title="Stock API", version="1.0")

class StockPriceExtractor:
    def __init__(self, symbols=None):
        self.data_dir = self._get_data_directory()
        self.csv_files = self.get_csv_files()
        self.symbols = symbols if symbols else []

    def _get_data_directory(self):
        current_dir = Path(__file__).resolve().parent
        data_dir = current_dir / "data"
        if not data_dir.exists():
            raise FileNotFoundError("Carpeta 'data' no encontrada")
        return data_dir

    def get_csv_files(self):
        return list(self.data_dir.glob("*.csv"))

    def get_latest_price(self):
        latest_prices = {}
        for file in self.csv_files:
            try:
                df = pd.read_csv(file, usecols=['symbol', 'timestamp', 'currentPrice'], parse_dates=['timestamp'])
                if self.symbols:
                    df = df[df['symbol'].isin(self.symbols)]
                latest_prices.update(df[['currentPrice']].to_dict()['currentPrice'])
            except Exception as e:
                continue
        return latest_prices

@app.get("/latest-price/{symbol}")
def get_latest_price(symbol: str):
    extractor = StockPriceExtractor(symbols=[symbol])
    prices = extractor.get_latest_price()
    if symbol not in prices:
        raise HTTPException(status_code=404, detail="Símbolo no encontrado")
    return {"symbol": symbol, "price": prices[symbol]}

@app.get("/symbols")
def get_symbols():
    extractor = StockPriceExtractor()
    symbols = set()
    for file in extractor.csv_files:
        df = pd.read_csv(file, usecols=['symbol'])
        symbols.update(df['symbol'].unique())
    return {"symbols": sorted(list(symbols))}


@app.get("/symbol/{symbol}/all-data")
def get_all_data(symbol: str):
    extractor = StockPriceExtractor(symbols=[symbol])
    all_data = []
    for file in extractor.csv_files:
        try:
            df = pd.read_csv(file)
            df = df[df['symbol'] == symbol]
            all_data.extend(df.to_dict(orient="records"))
        except Exception:
            continue
    if not all_data:
        raise HTTPException(status_code=404, detail="No hay datos para este símbolo")
    return all_data



@app.get("/symbol/{symbol}/filter")
def filter_data(
        symbol: str,
        start_date: str = "2020-01-01",
        end_date: str = datetime.now().strftime("%Y-%m-%d")
):
    try:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Formato de fecha inválido. Use YYYY-MM-DD")

    extractor = StockPriceExtractor(symbols=[symbol])
    filtered_data = []
    for file in extractor.csv_files:
        try:
            df = pd.read_csv(file, parse_dates=['timestamp'])
            df = df[(df['symbol'] == symbol) & (df['timestamp'].between(start, end))]
            filtered_data.extend(df.to_dict(orient="records"))
        except Exception:
            continue

    return filtered_data


@app.get("/current-prices/")
def get_current_prices(
        symbols: Optional[List[str]] = Query(None,
                                             description="Lista de símbolos (ej: BAP, ILF). Si no se especifica, devuelve todos.")
):
    """
    Obtiene el último 'currentPrice' para los símbolos solicitados.
    """
    extractor = StockPriceExtractor(symbols=symbols if symbols else None)
    prices = extractor.get_latest_price()

    if not prices:
        raise HTTPException(
            status_code=404,
            detail="No se encontraron precios. Verifica los símbolos o los archivos CSV."
        )

    return {"current_prices": prices}


@app.get("/plot-prices/")
def plot_prices(
        symbols: List[str] = Query(["BAP", "ILF", "BRK-B"], description="Símbolos a graficar"),
        days: int = Query(30, description="Número de días a visualizar"),
        style: str = Query("ggplot", description="Estilo del gráfico (ggplot, classic, etc.)")
):
    """
    Genera gráficos individuales del 'currentPrice' para cada símbolo.
    """
    try:
        # Verificar estilos disponibles
        available_styles = plt.style.available
        if style not in available_styles:
            raise HTTPException(
                status_code=400,
                detail=f"Estilo no válido. Opciones disponibles: {available_styles}"
            )

        # Obtener datos
        extractor = StockPriceColumnExtractor()
        all_data = extractor.get_all_column_data("currentPrice")
        all_data = all_data[all_data['symbol'].isin(symbols)]

        if all_data.empty:
            raise HTTPException(status_code=404, detail="No hay datos para los símbolos especificados")

        # Filtrar por fecha
        min_date = pd.Timestamp.now() - pd.Timedelta(days=days)
        filtered_data = all_data[all_data['timestamp'] >= min_date]

        if filtered_data.empty:
            raise HTTPException(
                status_code=404,
                detail=f"No hay datos en los últimos {days} días"
            )

        # Configurar estilo
        plt.style.use(style)
        figures = {}

        # Generar gráficos
        for symbol in symbols:
            symbol_data = filtered_data[filtered_data['symbol'] == symbol]
            if symbol_data.empty:
                continue

            plt.figure(figsize=(10, 5))

            # Elegir tipo de gráfico según la cantidad de datos
            if len(symbol_data) > 1:
                plt.plot(symbol_data['timestamp'], symbol_data['currentPrice'], color='steelblue', linewidth=2)
            else:
                plt.scatter(symbol_data['timestamp'], symbol_data['currentPrice'], color='firebrick', s=100)

            plt.title(f"Precio de {symbol} (Últimos {days} días)")
            plt.xlabel("Fecha")
            plt.ylabel("Precio (USD)")
            plt.grid(True, linestyle='--', alpha=0.7)

            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=120, bbox_inches='tight')
            buffer.seek(0)
            figures[symbol] = buffer
            plt.close()

        if not figures:
            raise HTTPException(status_code=404, detail="No se pudo generar ningún gráfico")

        return StreamingResponse(figures[next(iter(figures))], media_type="image/png")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
