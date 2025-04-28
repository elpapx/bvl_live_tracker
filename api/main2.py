# ---- IMPORT LIBRARIES --------
# ------------------------------
import os
import time
import threading
import json
import logging
from logging.handlers import RotatingFileHandler
import heapq

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from datetime import datetime
from fastapi import FastAPI, HTTPException, BackgroundTasks, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.security import APIKeyHeader
from starlette.responses import FileResponse, JSONResponse, HTMLResponse
from starlette.requests import Request
from typing import Dict, List, Optional, Tuple, Any
from pydantic import BaseModel
from io import BytesIO
from fastapi import BackgroundTasks
from fastapi import APIRouter, Query
from datetime import datetime, timedelta
from typing import Dict, List
import pandas as pd
import logging
from pydantic import BaseModel
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, FileResponse
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import os

# ---- CONFIGURACIÓN INICIAL --------
# ------------------------------

# Precios originales del portfolio
ORIGINAL_PRICES = {
    "BAP": 184.88,  # Credicorp
    "BRK-B": 479.20,  # Berkshire Hathaway B
    "ILF": 24.10  # iShares Latin America 40 ETF
}

# Datos fijos del portfolio
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

# ---- CONFIGURACIÓN DE LOGGING MEJORADA --------
# ------------------------------

# Configurar directorio de logs
log_dir = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(log_dir, exist_ok=True)

# Configurar logging con rotación de archivos
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

# Loggers específicos
logger = logging.getLogger(__name__)
cache_logger = logging.getLogger('cache')
perf_logger = logging.getLogger('performance')

# ---- CONFIGURACIÓN DE CACHÉ MEJORADA --------
# ------------------------------

# Parámetros de la caché
MAX_CACHE_SIZE = 10  # Número máximo de DataFrames en caché
CACHE_ITEM_TTL = 300  # 5 minutos en segundos


class CacheItem:
    def __init__(self, df, last_modified):
        self.df = df
        self.last_load_time = datetime.now()
        self.last_modified = last_modified
        self.size = df.memory_usage(deep=True).sum() if df is not None else 0
        self.access_count = 0  # Para política LRU

    def __lt__(self, other):
        return self.access_count < other.access_count


# Variables globales para la caché
dataframes_cache = {}
cache_lock = threading.Lock()
cache_size = 0  # Tamaño total en bytes


# ---- MODELOS PYDANTIC --------
# ------------------------------

class StockData(BaseModel):
    symbol: str
    currentPrice: float
    previousClose: float
    open: float
    dayLow: float
    dayHigh: float
    dividendYield: Optional[float] = None
    financialCurrency: str
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
    available_periods: List[str] = ["1d", "1w", "1m", "3m"]
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

class StockAPIException(HTTPException):
    """Custom exception for stock API errors"""
    def __init__(self, status_code: int, detail: str, code: str = None):
        super().__init__(status_code=status_code, detail=detail)
        self.code = code



# ---- FUNCIONES AUXILIARES MEJORADAS --------
# ------------------------------

def clear_cache_if_needed():
    """Limpia la caché si excede el tamaño máximo usando política LRU"""
    global cache_size, dataframes_cache

    if len(dataframes_cache) <= MAX_CACHE_SIZE:
        return

    cache_logger.info(f"Limpiando caché (actual: {len(dataframes_cache)} items)")

    # Ordenar items por contador de accesos (LRU)
    sorted_items = sorted(dataframes_cache.items(), key=lambda x: x[1].access_count)

    # Eliminar los menos usados hasta estar bajo el límite
    while len(dataframes_cache) > MAX_CACHE_SIZE and sorted_items:
        symbol, item = sorted_items.pop(0)
        cache_size -= item.size
        del dataframes_cache[symbol]
        cache_logger.debug(f"Removido {symbol} de caché (tamaño: {item.size / 1024:.2f} KB)")


def get_project_root():
    """Devuelve la ruta relativa a la raíz del proyecto"""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def get_csv_path(symbol: str):
    """Obtiene la ruta del archivo CSV basado en el símbolo"""
    root_path = get_project_root()

    if symbol.upper() == "BRK-B":
        filename = "brk-b_stock_data.csv"
    elif symbol.upper() == "ILF":
        filename = "ilf_etf_data.csv"
    else:
        filename = f"{symbol.lower()}_{'etf_data' if 'ETF' in symbol.upper() else 'stock_data'}.csv"

    return os.path.join(root_path, "data", filename)


def get_file_last_modified(file_path: str) -> float:
    """Obtiene el timestamp de última modificación del archivo"""
    try:
        return os.path.getmtime(file_path) if os.path.exists(file_path) else 0
    except Exception as e:
        logger.error(f"Error al obtener la fecha de modificación: {str(e)}")
        return 0


def load_dataframe(symbol: str, force_reload: bool = False) -> pd.DataFrame:
    """Carga el DataFrame desde el CSV con manejo de caché mejorado"""
    global dataframes_cache, cache_size

    file_path = get_csv_path(symbol)
    current_modified_time = get_file_last_modified(file_path)

    with cache_lock:
        # Verificar si el item en caché está expirado
        cache_item = dataframes_cache.get(symbol)

        if cache_item:
            # Verificar TTL y modificaciones del archivo
            cache_expired = (datetime.now() - cache_item.last_load_time).total_seconds() > CACHE_ITEM_TTL
            file_modified = current_modified_time > cache_item.last_modified

            if not force_reload and not cache_expired and not file_modified:
                cache_item.access_count += 1
                cache_logger.debug(f"Usando caché para {symbol} (accesos: {cache_item.access_count})")
                return cache_item.df

        # Cargar datos del archivo
        perf_logger.info(f"Cargando datos para {symbol} {'(forced)' if force_reload else ''}")
        start_time = time.time()

        try:
            if not os.path.exists(file_path):
                logger.error(f"Archivo CSV no encontrado: {file_path}")
                return None

            df = pd.read_csv(file_path)

            # Procesamiento del DataFrame
            columns_to_drop = [col for col in ['symbol', 'Symbol', 'Ticker'] if col in df.columns]
            df = df.drop(columns=columns_to_drop)

            required_cols = ['timestamp', 'currentPrice']
            for col in required_cols:
                if col not in df.columns:
                    logger.error(f"Columna requerida '{col}' no encontrada en {file_path}")
                    return None

            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df = df.dropna(subset=['timestamp'])

            if 'volumen' not in df.columns:
                volume_col = next((col for col in df.columns if 'vol' in col.lower()), None)
                if volume_col:
                    df['volumen'] = df[volume_col]
                else:
                    df['volumen'] = 0

            numeric_cols = ['currentPrice', 'open', 'dayHigh', 'dayLow', 'volumen',
                            'marketCap', 'trailingPE', 'dividendYield']
            existing_numeric_cols = [col for col in numeric_cols if col in df.columns]
            for col in existing_numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            if 'fiftyTwoWeekRange' in df.columns:
                df['fiftyTwoWeekRange'] = df['fiftyTwoWeekRange'].astype(str)
                df.loc[df['fiftyTwoWeekRange'] == 'nan', 'fiftyTwoWeekRange'] = None

            df = df.ffill().bfill()
            df = df.replace([np.inf, -np.inf], np.nan)

            if df.empty:
                logger.error(f"DataFrame vacío después de limpieza para {symbol}")
                return None

            df = df.sort_values('timestamp')

            # Calcular tamaño en memoria
            df_size = df.memory_usage(deep=True).sum()
            perf_logger.info(
                f"DataFrame {symbol} cargado en {time.time() - start_time:.2f}s - Tamaño: {df_size / 1024:.2f} KB")

            # Crear nuevo ítem de caché
            new_cache_item = CacheItem(df, current_modified_time)
            new_cache_item.access_count = 1

            # Actualizar caché
            if symbol in dataframes_cache:
                cache_size -= dataframes_cache[symbol].size

            dataframes_cache[symbol] = new_cache_item
            cache_size += df_size

            # Limpiar caché si es necesario
            clear_cache_if_needed()

            return df

        except Exception as e:
            logger.error(f"Error al cargar datos de {symbol}: {str(e)}", exc_info=True)
            return None


def load_historical_data(symbol: str):
    """Carga datos históricos desde archivos CSV incluyendo volumen"""
    root_path = get_project_root()
    if symbol.upper() == "BRK-B":
        hist_filename = "brk-b_historical.csv"
    else:
        hist_filename = f"{symbol.lower()}_historical.csv"

    hist_file_path = os.path.join(root_path, "historical_data", hist_filename)

    if not os.path.exists(hist_file_path):
        return None

    try:
        df = pd.read_csv(hist_file_path)
        df['timestamp'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
        df = df.dropna(subset=['timestamp'])

        df = df.rename(columns={
            'Open': 'currentPrice',
            'Volume': 'volumen'
        })

        return df[['timestamp', 'currentPrice', 'volumen']]
    except Exception as e:
        logger.error(f"Error cargando datos históricos para {symbol}: {str(e)}")
        return None


def background_update_all_dataframes():
    """Actualiza todos los dataframes en segundo plano"""
    try:
        symbols = list_available_stocks_internal()
        for symbol in symbols:
            load_dataframe(symbol, force_reload=True)
        logger.info(f"Actualización en segundo plano completada para {len(symbols)} símbolos")
    except Exception as e:
        logger.error(f"Error durante la actualización en segundo plano: {str(e)}")


def calculate_percentage_change(price_series):
    """Calcula el cambio porcentual entre el primer y último precio de una serie"""
    if len(price_series) < 2:
        return 0.0

    first_price = price_series.iloc[0]
    last_price = price_series.iloc[-1]

    if first_price == 0:
        return 0.0

    return ((last_price - first_price) / first_price) * 100


def list_available_stocks_internal():
    """Lista todos los símbolos disponibles (versión interna)"""
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
        logger.error(f"Error al listar acciones: {str(e)}")
        return []


def get_latest_data(symbol: str) -> Dict:
    """Obtiene los datos más recientes para un símbolo"""
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
        if latest_data['volumen'] is not None:
            latest_data['volumen'] = int(latest_data['volumen'])
        else:
            latest_data['volumen'] = 0
    except (ValueError, TypeError):
        latest_data['volumen'] = 0

    return latest_data


def get_historical_data(symbol: str, days: int = 30) -> List[Dict]:
    """Obtiene datos históricos para un símbolo"""
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
    """Limpia los datos para asegurar que sean serializables en JSON"""
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


# ---- CONFIGURACIÓN FASTAPI --------
# ------------------------------

app = FastAPI(
    title="BVL Live Tracker API",
    description="API para consultar datos de acciones en tiempo real",
    version="1.0.0"
)

# Configuración de archivos estáticos
static_dir = os.path.join(os.path.dirname(__file__), 'static')
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---- ENDPOINTS DE MONITOREO MEJORADOS --------
# ------------------------------

@app.get("/monitor/cache")
def monitor_cache():
    """Endpoint para monitorear el estado de la caché"""
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
    """Endpoint para ver los logs recientes"""
    log_file = os.path.join(log_dir, 'api.log')

    if not os.path.exists(log_file):
        raise HTTPException(status_code=404, detail="Archivo de log no encontrado")

    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            all_lines = f.readlines()
            recent_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines

        return HTMLResponse(content="<pre>" + "".join(recent_lines) + "</pre>")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al leer logs: {str(e)}")


# ---- ENDPOINTS PRINCIPALES --------
# ------------------------------

@app.get("/")
def read_root():
    return {"message": "BVL Live Tracker API v1.0"}


@app.get("/stocks", response_model=List[str])
def list_available_stocks():
    """Lista todos los símbolos disponibles"""
    symbols = list_available_stocks_internal()
    if not symbols:
        raise HTTPException(status_code=500, detail="Error al obtener la lista de acciones")
    return symbols


@app.get("/stocks/{symbol}", response_model=StockData)
def get_stock_data(symbol: str):
    """Obtiene los datos más recientes para un símbolo específico"""
    available_symbols = list_available_stocks_internal()
    if symbol.upper() not in available_symbols:
        raise HTTPException(
            status_code=404,
            detail=f"Símbolo no válido. Opciones disponibles: {', '.join(available_symbols)}"
        )

    data = get_latest_data(symbol)
    if data is None:
        raise HTTPException(status_code=404, detail=f"Datos no encontrados para {symbol}")

    data = clean_json_data(data)
    return data


@app.get("/stocks/{symbol}/history")
def get_stock_history(symbol: str, days: int = 30):
    """Obtiene el historial de datos para un símbolo"""
    data = get_historical_data(symbol, days)

    if not data:
        raise HTTPException(status_code=404, detail=f"Datos históricos no encontrados para {symbol}")

    data = clean_json_data(data)
    return data


@app.get("/stocks/{symbol}/chart/{field}")
def get_stock_chart(symbol: str, field: str, days: int = 30):
    """Genera datos para un gráfico de un campo específico"""
    valid_fields = ['currentPrice', 'previousClose', 'open', 'dayLow', 'dayHigh',
                    'dividendYield', 'volume']

    if field not in valid_fields:
        raise HTTPException(status_code=400, detail=f"Campo inválido. Opciones: {', '.join(valid_fields)}")

    data = get_historical_data(symbol, days)

    if not data:
        raise HTTPException(status_code=404, detail=f"Datos históricos no encontrados para {symbol}")

    chart_data = []
    for item in data:
        if item.get(field) is not None:
            chart_data.append({"timestamp": item["timestamp"], field: item.get(field)})

    chart_data.sort(key=lambda x: x["timestamp"] if x["timestamp"] is not None else "")
    return chart_data


@app.get("/compare")
def compare_stocks(symbols: str, field: str = "currentPrice", days: int = 30):
    """Compara múltiples símbolos en un campo específico"""
    symbol_list = symbols.split(",")
    valid_fields = ['currentPrice', 'previousClose', 'open', 'dayLow', 'dayHigh',
                    'dividendYield', 'volumen']

    if field not in valid_fields:
        raise HTTPException(status_code=400, detail=f"Campo inválido. Opciones: {', '.join(valid_fields)}")

    comparison_data = {}

    for symbol in symbol_list:
        data = get_historical_data(symbol, days)
        if data:
            symbol_data = []
            for item in data:
                if item.get(field) is not None:
                    symbol_data.append({"timestamp": item["timestamp"], field: item.get(field)})

            symbol_data.sort(key=lambda x: x["timestamp"] if x["timestamp"] is not None else "")
            comparison_data[symbol] = symbol_data

    if not comparison_data:
        raise HTTPException(status_code=404, detail="No se encontraron datos para comparar")

    comparison_data = clean_json_data(comparison_data)
    return comparison_data


@app.get("/portfolio/profitability", response_model=List[ProfitabilityData])
def get_portfolio_profitability():
    """Calcula la rentabilidad del portafolio basado en precios originales y actuales"""
    result = []

    symbol_names = {
        "BAP": "Credicorp",
        "BRK-B": "Berkshire Hathaway B",
        "ILF": "iShares Latin America 40 ETF"
    }

    for symbol, original_price in ORIGINAL_PRICES.items():
        current_data = get_latest_data(symbol)

        if current_data is None or current_data.get('currentPrice') is None:
            current_price = original_price
            profitability = 0.0
        else:
            current_price = current_data.get('currentPrice')
            profitability = ((current_price - original_price) / original_price) * 100

        result.append({
            "symbol": symbol,
            "name": symbol_names.get(symbol, symbol),
            "original_price": original_price,
            "current_price": current_price,
            "profitability_percentage": round(profitability, 2)
        })

    return result


@app.get("/portfolio/profitability/{symbol}", response_model=ProfitabilityData)
def get_symbol_profitability(symbol: str):
    """Calcula la rentabilidad para un símbolo específico"""
    if symbol not in ORIGINAL_PRICES:
        raise HTTPException(status_code=404, detail=f"No hay precio original registrado para {symbol}")

    original_price = ORIGINAL_PRICES[symbol]
    current_data = get_latest_data(symbol)

    if current_data is None:
        raise HTTPException(status_code=404, detail=f"No se encontraron datos actuales para {symbol}")

    current_price = current_data.get('currentPrice')
    if current_price is None:
        raise HTTPException(status_code=500, detail=f"No hay precio actual disponible para {symbol}")

    profitability = ((current_price - original_price) / original_price) * 100

    symbol_names = {
        "BAP": "Credicorp",
        "BRK-B": "Berkshire Hathaway B",
        "ILF": "iShares Latin America 40 ETF"
    }

    return {
        "symbol": symbol,
        "name": symbol_names.get(symbol, symbol),
        "original_price": original_price,
        "current_price": current_price,
        "profitability_percentage": round(profitability, 2)
    }


@app.get("/api/timeseries", response_model=TimeSeriesResponse)
async def get_time_series(
        symbol: str = Query("BAP", description="Símbolo a consultar (BAP, BRK-B, ILF)"),
        period: str = Query("1w", description="Periodo (1d, 1w, 1m, 3m)"),
        compare_all: bool = Query(False, description="Mostrar todos los símbolos juntos")
):
    """Obtiene series temporales para diferentes periodos"""
    periods_map = {
        "1d": timedelta(days=1),
        "1w": timedelta(weeks=1),
        "1m": timedelta(days=30),
        "3m": timedelta(days=90)
    }

    symbols_to_fetch = ["BAP", "BRK-B", "ILF"] if compare_all else [symbol]
    end_date = datetime.now()

    result = []

    for sym in symbols_to_fetch:
        try:
            df = load_dataframe(sym)
            if df is None or df.empty:
                continue

            df['timestamp'] = pd.to_datetime(df['timestamp'])
            start_date = end_date - periods_map[period]

            filtered_df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]
            filtered_df = filtered_df.sort_values('timestamp')

            series_data = [
                TimeSeriesPoint(
                    timestamp=row['timestamp'].isoformat(),
                    price=float(row['currentPrice'])
                )
                for _, row in filtered_df.iterrows()
                if pd.notna(row['currentPrice'])
            ]

            result.append(SymbolTimeSeries(
                symbol=sym,
                data=series_data,
                period=period
            ))

        except Exception as e:
            logger.error(f"Error procesando {sym}: {str(e)}")

    return TimeSeriesResponse(series=result)


@app.get("/api/timeseries-with-profitability", response_model=TimeSeriesResponse)
async def get_time_series_with_profitability(
        symbol: str = Query("BAP", description="Símbolo a consultar (BAP, BRK-B, ILF)"),
        period: str = Query("1w", description="Periodo (realtime, 1d, 1w, 1m, 3m)"),
        compare_all: bool = Query(False, description="Mostrar todos los símbolos juntos")
):
    """Obtiene series temporales con información de rentabilidad, volumen y PE Ratio"""
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

    logger.info(f"Procesando símbolos: {symbols_to_fetch}, período: {period}")
    result = []

    for sym in symbols_to_fetch:
        try:
            current_df = load_dataframe(sym)
            if current_df is None:
                current_df = pd.DataFrame()

            hist_df = load_historical_data(sym)

            if hist_df is not None:
                hist_df = hist_df[hist_df['timestamp'] < transition_date]
                logger.info(f"Datos históricos para {sym}: {len(hist_df)} registros")

            if not current_df.empty:
                current_df = current_df[current_df['timestamp'] >= transition_date]
                logger.info(f"Datos actuales para {sym}: {len(current_df)} registros")

            combined_df = pd.concat([hist_df, current_df], ignore_index=True) if hist_df is not None else current_df

            if combined_df.empty:
                logger.warning(f"No hay datos combinados para {sym}")
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

            logger.info(f"Datos filtrados para {sym} ({period}): {len(filtered_df)} registros")

            if filtered_df.empty:
                logger.warning(f"No hay datos para {sym} en el período {period}")
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
            logger.error(f"Error procesando {sym}: {str(e)}", exc_info=True)

    return TimeSeriesResponse(series=result)

@app.get("/api/timeseries-variations", response_model=TimeSeriesResponse)
async def get_time_series_variations(
        symbol: str = Query("BAP", description="Símbolo a consultar (BAP, BRK-B, ILF)"),
        period: str = Query("1w", description="Periodo (realtime, 1d, 1w, 1m, 3m)"),
        compare_all: bool = Query(False, description="Mostrar todos los símbolos juntos")
):
    """Obtiene series temporales con información de variación porcentual entre precios"""
    periods_map = {
        "realtime": timedelta(hours=6),  # Últimas 6 horas para tiempo real
        "1d": timedelta(days=1),
        "1w": timedelta(weeks=1),
        "1m": timedelta(days=30),
        "3m": timedelta(days=90)
    }

    symbols_to_fetch = ["BAP", "BRK-B", "ILF"] if compare_all else [symbol]
    end_date = datetime.now()
    transition_date = datetime(2025, 4, 5)  # Fecha de transición

    logger.info(f"Procesando símbolos: {symbols_to_fetch}, período: {period}")
    result = []

    for sym in symbols_to_fetch:
        try:
            # Cargar datos con manejo seguro de DataFrames
            current_df = load_dataframe(sym)
            current_df = pd.DataFrame() if current_df is None or current_df.empty else current_df

            hist_df = load_historical_data(sym)
            hist_df = pd.DataFrame() if hist_df is None or hist_df.empty else hist_df

            # Filtrar y combinar datasets
            hist_df = hist_df[hist_df['timestamp'] < transition_date] if not hist_df.empty else hist_df
            current_df = current_df[current_df['timestamp'] >= transition_date] if not current_df.empty else current_df

            # Combinar ambos datasets
            combined_df = pd.concat([hist_df, current_df], ignore_index=True)

            if combined_df.empty:
                logger.warning(f"No hay datos combinados para {sym}")
                continue

            # Filtrar por período
            start_date = end_date - periods_map[period]
            filtered_df = combined_df[
                (combined_df['timestamp'] >= start_date) &
                (combined_df['timestamp'] <= end_date)
                ].copy()  # Usar copy() para evitar SettingWithCopyWarning

            # Filtro adicional para tiempo real
            if period == "realtime":
                market_open = end_date.replace(hour=8, minute=0, second=0, microsecond=0)
                market_close = end_date.replace(hour=16, minute=0, second=0, microsecond=0)
                filtered_df = filtered_df[
                    (filtered_df['timestamp'] >= market_open) &
                    (filtered_df['timestamp'] <= market_close)
                    ]

            if filtered_df.empty:
                logger.warning(f"No hay datos para {sym} en el período {period}")
                continue

            # Procesamiento de datos numéricos
            numeric_cols = ['currentPrice', 'open', 'dayLow', 'dayHigh', 'volumen', 'previousClose']
            for col in numeric_cols:
                if col in filtered_df.columns:
                    filtered_df[col] = pd.to_numeric(filtered_df[col], errors='coerce')

            # Calcular variaciones
            if period == "realtime":
                # Variación punto a punto para tiempo real
                filtered_df['variation_pct'] = filtered_df['currentPrice'].pct_change() * 100
            else:
                # Variación respecto al primer punto para otros períodos
                first_valid_price = filtered_df['currentPrice'].first_valid_index()
                if first_valid_price is not None:
                    first_price = filtered_df.loc[first_valid_price, 'currentPrice']
                    filtered_df['variation_pct'] = ((filtered_df['currentPrice'] - first_price) / first_price) * 100
                else:
                    filtered_df['variation_pct'] = 0.0

            # Rellenar NaN con 0 y manejar infinitos
            filtered_df['variation_pct'] = filtered_df['variation_pct'].replace([np.inf, -np.inf], np.nan).fillna(0)

            # Obtener metadatos
            last_current_data = get_latest_data(sym) or {}

            # Construir respuesta
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

            # Calcular métricas adicionales
            original_price = ORIGINAL_PRICES.get(sym)
            last_price = filtered_df['currentPrice'].iloc[
                -1] if not filtered_df.empty and 'currentPrice' in filtered_df.columns else None
            last_variation = filtered_df['variation_pct'].iloc[-1] if not filtered_df.empty else 0.0

            # Calcular volatilidad (desviación estándar de las variaciones)
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
            logger.error(f"Error procesando {sym}: {str(e)}", exc_info=True)
            continue

    return TimeSeriesResponse(series=result)

@app.get("/portfolio/holdings/live", response_model=PortfolioHoldings)
def get_portfolio_holdings_live():
    """
    Obtiene los datos de la cartera en tiempo real, calculando los valores con los datos más recientes.
    Utiliza información fija de cantidad de acciones y precios de compra.

    Returns:
        PortfolioHoldings: Objeto con información completa de la cartera
    """
    holdings = []
    portfolio_total_value = 0
    portfolio_todays_change_value = 0
    portfolio_total_gain_loss = 0
    portfolio_previous_value = 0

    for symbol, data in PORTFOLIO_DATA.items():
        # Obtener datos actuales del símbolo
        current_data = get_latest_data(symbol)

        if current_data is None:
            logger.error(f"No se encontraron datos para {symbol}")
            continue

        # Extraer valores necesarios con manejo de errores
        current_price = current_data.get('currentPrice')
        previous_close = current_data.get('previousClose')

        # Si no hay previous_close, usar un valor estimado para evitar errores
        if previous_close is None and current_price is not None:
            logger.warning(f"Sin datos de cierre previo para {symbol}, usando precio actual")
            previous_close = current_price

        if current_price is None:
            logger.error(f"Sin precio actual para {symbol}")
            continue

        # Valores del portfolio para este símbolo
        purchase_price = data["purchase_price"]
        qty = data["qty"]

        # Calcular el cambio diario
        todays_change = current_price - previous_close
        todays_change_percent = (todays_change / previous_close) * 100 if previous_close > 0 else 0

        # Calcular el valor total actual
        total_value = current_price * qty

        # Calcular la ganancia/pérdida total
        total_gain_loss = total_value - (purchase_price * qty)
        total_gain_loss_percent = (total_gain_loss / (purchase_price * qty)) * 100 if purchase_price > 0 else 0

        # Actualizar totales del portfolio
        portfolio_total_value += total_value
        portfolio_todays_change_value += todays_change * qty
        portfolio_total_gain_loss += total_gain_loss
        portfolio_previous_value += previous_close * qty

        # Crear objeto de holding con valores redondeados para mejor presentación
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

    # Calcular porcentajes totales del portfolio
    portfolio_initial_value = portfolio_total_value - portfolio_total_gain_loss
    portfolio_todays_change_percent = (
                                                  portfolio_todays_change_value / portfolio_previous_value) * 100 if portfolio_previous_value > 0 else 0
    portfolio_total_gain_loss_percent = (
                                                    portfolio_total_gain_loss / portfolio_initial_value) * 100 if portfolio_initial_value > 0 else 0

    # Crear objeto final con valores redondeados
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
    """
    Fuerza una actualización de todos los datos en caché

    Returns:
        dict: Mensaje de confirmación de la actualización
    """
    background_tasks.add_task(background_update_all_dataframes)
    return {"message": "Actualización de datos iniciada en segundo plano"}


@app.exception_handler(StockAPIException)
async def stock_exception_handler(request: Request, exc: StockAPIException):
    """
    Maneja las excepciones personalizadas de la API

    Args:
        request: Objeto Request de la petición
        exc: Excepción StockAPIException

    Returns:
        JSONResponse: Respuesta JSON con detalles del error
    """
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": exc.code or str(exc.status_code),
                "message": exc.detail,
                "timestamp": datetime.now().isoformat(),
                "path": request.url.path
            }
        }
    )


@app.get("/health")
def health_check():
    """
    Endpoint para verificar el estado de la API

    Returns:
        dict: Estado de salud de la API incluyendo:
            - status: "ok"
            - timestamp: Fecha y hora actual
            - cached_symbols: Lista de símbolos en caché
    """
    return {
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "cached_symbols": list(dataframes_cache.keys())
    }

# Función para iniciar el bucle de actualización periódica
def start_periodic_updates():
    """
    Inicia un hilo para actualizar periódicamente todos los datos
    """
    def update_loop():
        while True:
            try:
                logger.info("Iniciando actualización periódica de datos")
                background_update_all_dataframes()
                # Esperar 60 segundos antes de la próxima actualización
                time.sleep(60)  # Cambiado de 2 a 60 segundos para reducir carga
            except Exception as e:
                logger.error(f"Error en el bucle de actualización: {str(e)}")
                # Si hay un error, esperar 10 segundos antes de reintentar
                time.sleep(10)

    # Iniciar el hilo de actualización
    update_thread = threading.Thread(target=update_loop, daemon=True)
    update_thread.start()
    logger.info("Hilo de actualización periódica iniciado")

# Añadir estos endpoints después de la definición de los endpoints existentes
# y antes de la función startup_event

@app.get("/html/stocks")
async def get_stock_list_html():
    return FileResponse(os.path.join(static_dir, "stock_list.html"))

@app.get("/html/stocks/{symbol}")
async def get_stock_detail_html(symbol: str):
    return FileResponse(os.path.join(static_dir, "stock_detail.html"))

@app.get("/html/stocks/{symbol}/history")
async def get_stock_history_html(symbol: str):
    return FileResponse(os.path.join(static_dir, "stock_history.html"))

@app.get("/html/chart")
async def get_stock_chart_html():
    """
    Sirve la página HTML para mostrar el gráfico de un símbolo.
    """
    return FileResponse(os.path.join(static_dir, "stock_chart.html"))


@app.get("/html/compare")
async def get_compare_stocks_html():
    """
    Sirve la página HTML para comparar stocks.
    """
    return FileResponse(os.path.join(static_dir, "compare_stocks.html"))


#-----------PROFITABILITY------------------

@app.get("/html/portfolio/profitability")
async def get_portfolio_profitability_html():
    """
    Sirve la página HTML para mostrar la rentabilidad del portafolio.
    """
    return FileResponse(os.path.join(static_dir, "portfolio_profitability.html"))

@app.get("/html/portfolio/profitability/{symbol}")
async def get_symbol_profitability_html(symbol: str):
    """
    Sirve la página HTML para mostrar la rentabilidad de un símbolo.
    """
    return FileResponse(os.path.join(static_dir, "symbol_profitability.html"))



#---PLOTS


@app.get("/html/stocks/{symbol}/chart/{field}")
async def get_stock_chart_html(symbol: str, field: str):
    return FileResponse(os.path.join(static_dir, "stock_chart.html"))

@app.get("/timeseries", response_class=HTMLResponse)
async def serve_timeseries_html():
    """Endpoint que sirve la interfaz de visualización"""
    file_path = os.path.join(static_dir, "timeseries.html")
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Interfaz no encontrada")
    return FileResponse(file_path)


@app.get("/html/timeseries-profitability", response_class=HTMLResponse)
async def get_timeseries_profitability_html():
    """Sirve la interfaz de análisis completo"""
    return FileResponse(os.path.join(static_dir, "timeseries-profitability.html"))


@app.get("/html/market/variations", response_class=HTMLResponse)
async def get_market_variations_html():
    """
    Sirve la página HTML para mostrar las variaciones de precio de los símbolos.
    """
    return FileResponse(os.path.join(static_dir, "market_variations.html"))


# HTML Endpoint para visualizar los datos del portfolio
@app.get("/html/portfolio/holdings", response_class=HTMLResponse)
async def get_portfolio_holdings_html():
    """
    Sirve la página HTML para mostrar las posiciones de la cartera.
    """
    return FileResponse(os.path.join(static_dir, "portfolio_holdings.html"))


@app.get("/html")
async def get_html_index():
    return FileResponse(os.path.join(static_dir, "index.html"))


# ---- MANEJADOR DE EXCEPCIONES --------
# ------------------------------

@app.exception_handler(StockAPIException)
async def stock_exception_handler(request: Request, exc: StockAPIException):
    """
    Maneja las excepciones personalizadas de la API

    Args:
        request: Objeto Request de la petición
        exc: Excepción StockAPIException

    Returns:
        JSONResponse: Respuesta JSON con detalles del error
    """
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": exc.code or str(exc.status_code),
                "message": exc.detail,
                "timestamp": datetime.now().isoformat(),
                "path": request.url.path
            }
        }
    )


# ---- FUNCIONES DE INICIO --------
# ------------------------------

def start_periodic_updates():
    """
    Inicia un hilo para actualizar periódicamente todos los datos

    El hilo ejecuta actualizaciones cada 60 segundos y maneja errores
    con reintentos cada 10 segundos en caso de fallos.
    """

    def update_loop():
        while True:
            try:
                logger.info("Iniciando actualización periódica de datos")
                background_update_all_dataframes()
                time.sleep(60)
            except Exception as e:
                logger.error(f"Error en el bucle de actualización: {str(e)}")
                time.sleep(10)

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

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)