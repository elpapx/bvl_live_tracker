from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from typing import List, Optional
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import io
import uvicorn

app = FastAPI(title="Stock API", version="1.0")


class StockPriceExtractor:
    """Extrae datos de precios de acciones desde archivos CSV"""

    def __init__(self, symbols=None):
        self.data_dir = self._get_data_directory()
        self.csv_files = self.get_csv_files()
        self.symbols = symbols if symbols else None

    def _get_data_directory(self):
        current_dir = Path(__file__).resolve().parent
        data_dir = current_dir / "data"
        if not data_dir.exists():
            raise FileNotFoundError(f"No se encontró la carpeta 'data' en: {data_dir}")
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
                print(f"Error procesando {file.name}: {e}")
        return latest_prices

    def get_all_prices(self):
        all_data = []
        for file in self.csv_files:
            try:
                df = pd.read_csv(file, usecols=['symbol', 'timestamp', 'currentPrice'], parse_dates=['timestamp'])
                if self.symbols:
                    df = df[df['symbol'].isin(self.symbols)]
                all_data.append(df)
            except Exception as e:
                print(f"Error procesando {file.name}: {e}")
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame(columns=['symbol', 'timestamp', 'currentPrice'])


@app.get("/plot-prices/")
def plot_prices(
        symbols: List[str] = Query(["BAP", "ILF", "BRK-B"], description="Símbolos a graficar"),
        days: int = Query(30, description="Número de días a visualizar")
):
    """
    Genera gráficos del 'currentPrice' para cada símbolo solicitado.
    """
    try:
        # Obtener datos
        extractor = StockPriceExtractor(symbols=symbols)
        all_data = extractor.get_all_prices()

        if all_data.empty:
            raise HTTPException(status_code=404, detail="No se encontraron datos para los símbolos especificados")

        # Filtrar por días
        min_date = pd.Timestamp.now() - pd.Timedelta(days=days)
        filtered_data = all_data[all_data['timestamp'] >= min_date]

        if filtered_data.empty:
            raise HTTPException(status_code=404, detail=f"No hay datos en los últimos {days} días")

        # Generar gráficos
        plt.style.use('seaborn')
        figures = {}

        for symbol in symbols:
            symbol_data = filtered_data[filtered_data['symbol'] == symbol]
            if symbol_data.empty:
                continue

            plt.figure(figsize=(10, 5))
            plt.plot(symbol_data['timestamp'], symbol_data['currentPrice'], 'b-', linewidth=2)
            plt.title(f"Evolución de Precio: {symbol}")
            plt.xlabel("Fecha")
            plt.ylabel("Precio (USD)")
            plt.grid(True)

            # Guardar imagen en buffer
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight')
            buffer.seek(0)
            figures[symbol] = buffer
            plt.close()

        if not figures:
            raise HTTPException(status_code=404, detail="No se pudo generar ningún gráfico")

        # Devolver el primer gráfico (puedes modificar para devolver múltiples)
        return StreamingResponse(figures[symbols[0]], media_type="image/png")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ... (otros endpoints que ya tenías)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)