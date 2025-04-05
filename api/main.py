import os
import time
import threading
import json
import logging

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

ORIGINAL_PRICES = {
    "BAP": 184.88,    # Credicorp
    "BRK-B": 479.20,  # Berkshire Hathaway B
    "ILF": 24.10      # iShares Latin America 40 ETF
}

# Configuración de logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Modelo para la respuesta
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


# Creación de la aplicación FastAPI
app = FastAPI(title="BVL Live Tracker API",
              description="API para consultar datos de acciones en tiempo real",
              version="1.0.0")

static_dir = os.path.join(os.path.dirname(__file__), 'static')
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Configurar CORS para permitir solicitudes desde el frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite todas las origenes (ajustar en producción)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Estructura para almacenar en caché los DataFrames junto con su timestamp de modificación
class CacheItem:
    def __init__(self, df, last_modified):
        self.df = df
        self.last_load_time = datetime.now()
        self.last_modified = last_modified


# Diccionario para almacenar en caché los DataFrames
dataframes_cache = {}

# Lock para sincronización al acceder a la caché
cache_lock = threading.Lock()

class StockAPIException(HTTPException):
    """Custom exception for stock API errors"""
    def __init__(self, status_code: int, detail: str, code: str = None):
        super().__init__(status_code=status_code, detail=detail)
        self.code = code

def get_project_root():
    """
    Devuelve la ruta relativa a la raíz del proyecto
    """
    # La raíz del proyecto está dos niveles arriba de este archivo
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def get_csv_path(symbol: str):
    """
    Obtiene la ruta del archivo CSV basado en el símbolo
    """
    root_path = get_project_root()

    # Mapear símbolo a nombre de archivo
    if symbol.upper() == "BRK-B":
        filename = "brk-b_stock_data.csv"
    elif symbol.upper() == "ILF":
        filename = "ilf_etf_data.csv"
    else:
        filename = f"{symbol.lower()}_{'etf_data' if 'ETF' in symbol.upper() else 'stock_data'}.csv"

    return os.path.join(root_path, "data", filename)


def get_file_last_modified(file_path: str) -> float:
    """
    Obtiene el timestamp de última modificación del archivo
    """
    try:
        return os.path.getmtime(file_path) if os.path.exists(file_path) else 0
    except Exception as e:
        logger.error(f"Error al obtener la fecha de modificación: {str(e)}")
        return 0


def load_dataframe(symbol: str, force_reload: bool = False) -> pd.DataFrame:
    """
    Carga el DataFrame desde el CSV y maneja la caché
    """
    global dataframes_cache

    file_path = get_csv_path(symbol)
    current_modified_time = get_file_last_modified(file_path)

    with cache_lock:
        # Verificar si debemos actualizar la caché
        needs_update = force_reload or symbol not in dataframes_cache

        if not needs_update and symbol in dataframes_cache:
            cache_item = dataframes_cache[symbol]
            if current_modified_time > cache_item.last_modified:
                logger.info(f"Archivo CSV actualizado para {symbol}, recargando datos")
                needs_update = True
            elif (datetime.now() - cache_item.last_load_time).seconds > 65:
                logger.info(f"Caché expirada para {symbol}, recargando datos")
                needs_update = True
            else:
                logger.info(f"Usando caché para {symbol}")
                return cache_item.df

        if needs_update:
            try:
                if not os.path.exists(file_path):
                    logger.error(f"Archivo CSV no encontrado: {file_path}")
                    return None

                logger.info(f"Cargando datos de {file_path}")

                # Leer CSV con manejo de errores
                df = pd.read_csv(file_path)

                # Eliminar columnas no numéricas que puedan causar problemas
                df = df.loc[:, ~df.columns.isin(['symbol', 'Symbol', 'Ticker'])]

                # Verificar columnas requeridas
                required_cols = ['timestamp', 'currentPrice']
                for col in required_cols:
                    if col not in df.columns:
                        logger.error(f"Columna requerida '{col}' no encontrada en {file_path}")
                        return None

                # Convertir timestamp a datetime
                df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                df = df.dropna(subset=['timestamp'])

                # Convertir columnas numéricas
                numeric_cols = ['currentPrice', 'open', 'dayHigh', 'dayLow', 'volumen']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                # Limpieza de datos
                df = df.ffill().bfill()
                df = df.replace([np.inf, -np.inf], np.nan)

                # Verificar que tenemos datos válidos
                if df.empty:
                    logger.error(f"DataFrame vacío después de limpieza para {symbol}")
                    return None

                # Actualizar caché
                dataframes_cache[symbol] = CacheItem(df, current_modified_time)

                return df

            except Exception as e:
                logger.error(f"Error al cargar datos de {symbol}: {str(e)}")
                return None

    return None


def background_update_all_dataframes():
    """
    Actualiza todos los dataframes en segundo plano
    """
    try:
        # Obtener lista de símbolos disponibles
        symbols = list_available_stocks_internal()

        for symbol in symbols:
            load_dataframe(symbol, force_reload=True)

        logger.info(f"Actualización en segundo plano completada para {len(symbols)} símbolos")
    except Exception as e:
        logger.error(f"Error durante la actualización en segundo plano: {str(e)}")


def calculate_percentage_change(price_series):
    """
    Calcula el cambio porcentual entre el primer y último precio de una serie
    Args:
        price_series: Serie de pandas con precios
    Returns:
        float: Cambio porcentual
    """
    if len(price_series) < 2:
        return 0.0

    first_price = price_series.iloc[0]
    last_price = price_series.iloc[-1]

    if first_price == 0:  # Evitar división por cero
        return 0.0

    return ((last_price - first_price) / first_price) * 100


def list_available_stocks_internal():
    """
    Lista todos los símbolos disponibles (versión interna)
    """
    root_path = get_project_root()
    data_dir = os.path.join(root_path, "data")

    try:
        files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
        symbols = []
        for f in files:
            if f == "brk-b_stock_data.csv":
                symbols.append("BRK-B")
            elif f.endswith("_etf_data.csv"):
                symbols.append(f.split('_')[0].upper())
            elif f.endswith("_stock_data.csv"):  # Asegura que solo procese archivos de stock
                symbols.append(f.split('_')[0].upper())
        return symbols
    except Exception as e:
        logger.error(f"Error al listar acciones: {str(e)}")
        return []

def get_latest_data(symbol: str) -> Dict:
    """
    Obtiene los datos más recientes para un símbolo
    """
    df = load_dataframe(symbol)

    if df is None or df.empty:
        return None

    # Asumimos que el último registro es el más reciente
    latest_data = df.iloc[-1].to_dict()

    # Reemplazar valores NaN con None para compatibilidad con JSON
    for key, value in latest_data.items():
        if pd.isna(value):
            latest_data[key] = None

    # IMPORTANT FIX: Always ensure symbol is set correctly from the parameter
    latest_data['symbol'] = symbol

    # Asegurar que las columnas requeridas estén presentes
    required_columns = ['currentPrice', 'previousClose', 'open', 'dayLow', 'dayHigh',
                        'dividendYield', 'financialCurrency', 'volumen', 'timestamp']

    for col in required_columns:
        if col not in latest_data:
            latest_data[col] = None

    # Fix data types
    if latest_data['financialCurrency'] is None:
        latest_data['financialCurrency'] = "USD"  # Set a default value

    # Ensure volumen is an integer
    try:
        if latest_data['volumen'] is not None:
            latest_data['volumen'] = int(latest_data['volumen'])
        else:
            latest_data['volumen'] = 0  # Default value if None
    except (ValueError, TypeError):
        # If conversion fails, set to default
        latest_data['volumen'] = 0

    return latest_data

def get_historical_data(symbol: str, days: int = 30) -> List[Dict]:
    """
    Obtiene datos históricos para un símbolo
    """
    df = load_dataframe(symbol)

    if df is None or df.empty:
        return []

    # Convertir 'timestamp' a datetime si existe
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp', ascending=False)

    # Tomar los últimos 'days' registros
    historical_df = df.head(days)

    # Convertir a lista de diccionarios
    records = historical_df.to_dict(orient='records')

    # Reemplazar valores NaN con None para compatibilidad con JSON
    for record in records:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None
            # Convertir timestamp a string si es datetime
            elif key == 'timestamp' and isinstance(value, pd.Timestamp):
                record[key] = value.isoformat()

    return records


# Función para limpiar datos no serializables en JSON
def clean_json_data(data):
    """
    Limpia los datos para asegurar que sean serializables en JSON
    """
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


# Endpoints de la API
@app.get("/")
def read_root():
    return {"message": "BVL Live Tracker API v1.0"}


@app.get("/stocks", response_model=List[str])
def list_available_stocks():
    """
    Lista todos los símbolos disponibles
    """
    symbols = list_available_stocks_internal()
    if not symbols:
        raise HTTPException(status_code=500, detail="Error al obtener la lista de acciones")
    return symbols


@app.get("/stocks/{symbol}", response_model=StockData)
def get_stock_data(symbol: str):
    """
    Obtiene los datos más recientes para un símbolo específico
    """
    data = get_latest_data(symbol)

    if data is None:
        raise HTTPException(status_code=404, detail=f"Datos no encontrados para {symbol}")

    # Asegurar que los datos son JSON serializables
    data = clean_json_data(data)

    return data


@app.get("/stocks/{symbol}/history")
def get_stock_history(symbol: str, days: int = 30):
    """
    Obtiene el historial de datos para un símbolo
    """
    data = get_historical_data(symbol, days)

    if not data:
        raise HTTPException(status_code=404, detail=f"Datos históricos no encontrados para {symbol}")

    # Asegurar que los datos son JSON serializables
    data = clean_json_data(data)

    return data


@app.get("/stocks/{symbol}/chart/{field}")
def get_stock_chart(symbol: str, field: str, days: int = 30):
    """
    Genera datos para un gráfico de un campo específico
    """
    valid_fields = ['currentPrice', 'previousClose', 'open', 'dayLow', 'dayHigh',
                    'dividendYield', 'volumen']

    if field not in valid_fields:
        raise HTTPException(status_code=400, detail=f"Campo inválido. Opciones: {', '.join(valid_fields)}")

    data = get_historical_data(symbol, days)

    if not data:
        raise HTTPException(status_code=404, detail=f"Datos históricos no encontrados para {symbol}")

    # Filtrar solo las columnas necesarias y asegurar que no hay valores NaN
    chart_data = []
    for item in data:
        if item.get(field) is not None:
            chart_data.append({"timestamp": item["timestamp"], field: item.get(field)})

    # Ordenar por fecha (ascendente para que el gráfico muestre la progresión correcta)
    chart_data.sort(key=lambda x: x["timestamp"] if x["timestamp"] is not None else "")

    return chart_data


@app.get("/compare")
def compare_stocks(symbols: str, field: str = "currentPrice", days: int = 30):
    """
    Compara múltiples símbolos en un campo específico

    Args:
        symbols: Lista de símbolos separados por coma (ej: BAP,BRK-B,ILF)
        field: Campo a comparar (default: currentPrice)
        days: Número de días de historia (default: 30)

    Returns:
        Diccionario con datos para cada símbolo
    """
    symbol_list = symbols.split(",")
    valid_fields = ['currentPrice', 'previousClose', 'open', 'dayLow', 'dayHigh',
                    'dividendYield', 'volumen']

    if field not in valid_fields:
        raise HTTPException(status_code=400, detail=f"Campo inválido. Opciones: {', '.join(valid_fields)}")

    comparison_data = {}

    for symbol in symbol_list:
        data = get_historical_data(symbol, days)
        if data:
            # Extraer solo los campos relevantes y filtrar valores nulos
            symbol_data = []
            for item in data:
                if item.get(field) is not None:
                    symbol_data.append({"timestamp": item["timestamp"], field: item.get(field)})

            # Ordenar por fecha (ascendente para que el gráfico muestre la progresión correcta)
            symbol_data.sort(key=lambda x: x["timestamp"] if x["timestamp"] is not None else "")
            comparison_data[symbol] = symbol_data

    if not comparison_data:
        raise HTTPException(status_code=404, detail="No se encontraron datos para comparar")

    # Asegurar que los datos son JSON serializables
    comparison_data = clean_json_data(comparison_data)

    return comparison_data


@app.get("/portfolio/profitability", response_model=List[ProfitabilityData])
def get_portfolio_profitability():
    """
    Calcula la rentabilidad del portafolio basado en precios originales y actuales

    Returns:
        Lista de objetos con información de rentabilidad para cada símbolo
    """
    result = []

    # Mapeo de símbolos a nombres descriptivos
    symbol_names = {
        "BAP": "Credicorp",
        "BRK-B": "Berkshire Hathaway B",
        "ILF": "iShares Latin America 40 ETF"
    }

    for symbol, original_price in ORIGINAL_PRICES.items():
        # Obtener datos actuales
        current_data = get_latest_data(symbol)

        if current_data is None or current_data.get('currentPrice') is None:
            # Si no hay datos actuales, usar el precio original para evitar errores
            current_price = original_price
            profitability = 0.0
        else:
            current_price = current_data.get('currentPrice')
            # Calcular rentabilidad: ((precio venta - precio compra) / precio compra) * 100
            profitability = ((current_price - original_price) / original_price) * 100

        # Agregar a resultados
        result.append({
            "symbol": symbol,
            "name": symbol_names.get(symbol, symbol),
            "original_price": original_price,
            "current_price": current_price,
            "profitability_percentage": round(profitability, 2)  # Redondear a 2 decimales
        })

    return result


@app.get("/portfolio/profitability/{symbol}", response_model=ProfitabilityData)
def get_symbol_profitability(symbol: str):
    """
    Calcula la rentabilidad para un símbolo específico

    Args:
        symbol: Símbolo de la acción o ETF

    Returns:
        Objeto con información de rentabilidad para el símbolo
    """
    if symbol not in ORIGINAL_PRICES:
        raise HTTPException(status_code=404, detail=f"No hay precio original registrado para {symbol}")

    original_price = ORIGINAL_PRICES[symbol]
    current_data = get_latest_data(symbol)

    if current_data is None:
        raise HTTPException(status_code=404, detail=f"No se encontraron datos actuales para {symbol}")

    current_price = current_data.get('currentPrice')
    if current_price is None:
        raise HTTPException(status_code=500, detail=f"No hay precio actual disponible para {symbol}")

    # Calcular rentabilidad
    profitability = ((current_price - original_price) / original_price) * 100

    # Mapeo de símbolos a nombres descriptivos
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
        "profitability_percentage": round(profitability, 2)  # Redondear a 2 decimales
    }


@app.get("/stocks/{symbol}/continuous-data")
def get_continuous_stock_data(
        symbol: str,
        start_date: str = "2025-02-26",
        end_date: Optional[str] = None,
        field: str = "price"
):
    """
    Obtiene datos continuos combinando datos históricos y actuales,
    mapeando campos similares para crear una serie temporal uniforme.

    Args:
        symbol: Símbolo de la acción o ETF
        start_date: Fecha de inicio en formato YYYY-MM-DD (default: 2025-02-26)
        end_date: Fecha final en formato YYYY-MM-DD (opcional)
        field: Campo a devolver ("price", "volume") - price combinará Open y currentPrice

    Returns:
        Lista de registros con datos continuos
    """
    try:
        # Validar fechas
        start_datetime = pd.to_datetime(start_date).tz_localize(None)  # Eliminar zona horaria
        if end_date:
            end_datetime = pd.to_datetime(end_date).tz_localize(None)  # Eliminar zona horaria
        else:
            # Si no se especifica, usar la fecha actual
            end_datetime = pd.to_datetime(datetime.now().strftime("%Y-%m-%d")).tz_localize(None)
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Formato de fecha inválido. Use YYYY-MM-DD. Error: {str(e)}"
        )

    # Cargar datos actuales (scraping)
    current_df = load_dataframe(symbol)
    if current_df is None or current_df.empty:
        raise HTTPException(status_code=404, detail=f"Datos actuales no encontrados para {symbol}")

    # Preparar el nombre del archivo de datos históricos
    root_path = get_project_root()
    if symbol.upper() == "BRK-B":
        hist_filename = "brk-b_historical.csv"
    else:
        hist_filename = f"{symbol.lower()}_historical.csv"

    hist_file_path = os.path.join(root_path, "data", hist_filename)

    # Verificar si existe el archivo histórico
    has_historical_data = os.path.exists(hist_file_path)
    hist_df = pd.DataFrame()  # Inicializar dataframe vacío

    # Procesar datos históricos si existen
    if has_historical_data:
        try:
            logger.info(f"Cargando datos históricos de {hist_file_path}")
            hist_df = pd.read_csv(hist_file_path)

            # Asegurar que la columna de fecha existe
            if 'Date' not in hist_df.columns:
                logger.warning(f"Columna 'Date' no encontrada en datos históricos de {symbol}")
                has_historical_data = False
            else:
                # Convertir 'Date' a datetime sin zona horaria
                hist_df['Date'] = pd.to_datetime(hist_df['Date'], utc=True).dt.tz_localize(None)

                # Renombrar columnas para consistencia
                hist_df = hist_df.rename(columns={
                    'Date': 'timestamp',
                    'Open': 'open',
                    'Close': 'close',
                    'Volume': 'volumen'
                })

                # Filtrar por fecha
                hist_df = hist_df[hist_df['timestamp'] >= start_datetime]

                # Punto de corte: 1 de abril de 2025
                transition_date = pd.to_datetime("2025-04-01").tz_localize(None)
                hist_df = hist_df[hist_df['timestamp'] < transition_date]
        except Exception as e:
            logger.error(f"Error al cargar datos históricos: {str(e)}")
            has_historical_data = False

    # Procesar datos actuales
    # Asegurar que la columna timestamp es datetime sin zona horaria
    if 'timestamp' in current_df.columns:
        # Verificar primero si el tipo de dato es datetime
        if not pd.api.types.is_datetime64_any_dtype(current_df['timestamp']):
            # Convertir explícitamente a datetime si no lo es
            current_df['timestamp'] = pd.to_datetime(current_df['timestamp'], utc=True)

        # Ahora es seguro usar el accessor .dt
        if current_df['timestamp'].dt.tz is not None:
            current_df['timestamp'] = current_df['timestamp'].dt.tz_localize(None)
    else:
        raise HTTPException(status_code=500, detail=f"El dataframe de {symbol} no contiene columna 'timestamp'")

    # Filtrar datos actuales por fecha (usando fechas sin zona horaria)
    current_df = current_df[
        (current_df['timestamp'] >= pd.to_datetime("2025-04-01").tz_localize(None)) &
        (current_df['timestamp'] <= end_datetime)
        ]

    # Crear una tabla unificada
    frames = []

    # Añadir datos históricos si existen y son relevantes
    if has_historical_data and not hist_df.empty:
        # Para datos históricos, seleccionar columnas según el campo solicitado
        if field == "price":
            # Hasta el 31 de marzo, usar 'open' como precio
            hist_df['value'] = hist_df['open']
            hist_df['source'] = 'historical'
            frames.append(hist_df[['timestamp', 'value', 'source']])
        elif field == "volume":
            hist_df['value'] = hist_df['volumen']
            hist_df['source'] = 'historical'
            frames.append(hist_df[['timestamp', 'value', 'source']])

    # Añadir datos actuales
    if not current_df.empty:
        # Para datos actuales, usar currentPrice o volumen según corresponda
        if field == "price":
            current_df['value'] = current_df['currentPrice']
            current_df['source'] = 'current'
            frames.append(current_df[['timestamp', 'value', 'source']])
        elif field == "volume":
            current_df['value'] = current_df['volumen']
            current_df['source'] = 'current'
            frames.append(current_df[['timestamp', 'value', 'source']])

    # Combinar los dataframes
    if not frames:
        raise HTTPException(
            status_code=404,
            detail=f"No hay datos disponibles para {symbol} en el rango de fechas especificado"
        )

    combined_df = pd.concat(frames)
    combined_df = combined_df.sort_values('timestamp')

    # Convertir a registros y limpiar valores NaN
    records = combined_df.to_dict(orient='records')
    clean_records = clean_json_data(records)

    # Añadir metadatos
    result = {
        "symbol": symbol,
        "field": field,
        "period": {
            "start_date": start_date,
            "end_date": end_date or datetime.now().strftime("%Y-%m-%d")
        },
        "data": clean_records
    }

    return result

@app.get("/stocks/{symbol}/advanced-chart-data", response_model=Dict[str, Any])
def get_advanced_chart_data(
        symbol: str,
        timeframe: str = "1week",
        background_tasks: BackgroundTasks = None
):
    try:
        # Validar timeframe
        timeframe_days = {
            "1day": 1,
            "1week": 7,
            "1month": 30,
            "3months": 90
        }.get(timeframe)

        if not timeframe_days:
            raise HTTPException(status_code=400, detail="Timeframe no válido")

        # Forzar recarga si es necesario
        if background_tasks:
            background_tasks.add_task(load_dataframe, symbol, force_reload=True)

        # Log the symbol we're processing
        logger.info(f"Processing advanced chart data for symbol: {symbol}")

        # Obtener datos
        df = load_dataframe(symbol)
        if df is None or df.empty:
            raise HTTPException(status_code=404, detail=f"No hay datos disponibles para {symbol}")

        # Log dataframe info for debugging
        logger.info(f"Original columns for {symbol}: {df.columns.tolist()}")
        logger.info(f"DataFrame shape for {symbol}: {df.shape}")

        # Check for NaN values in important columns
        for col in ['timestamp', 'currentPrice']:
            if col in df.columns:
                nan_count = df[col].isna().sum()
                logger.info(f"NaN count in {col} for {symbol}: {nan_count}")

        # General column mapping for all symbols
        column_mapping = {
            'Close': 'currentPrice',
            'Open': 'open',
            'High': 'dayHigh',
            'Low': 'dayLow',
            'Volume': 'volumen',
            'Date': 'timestamp',
            'close': 'currentPrice'  # Handle lowercase variants too
        }

        # Apply mapping for columns that exist
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns and new_col not in df.columns:
                df[new_col] = df[old_col]
                logger.info(f"Mapped {old_col} to {new_col} for {symbol}")

        # Ensure timestamp column exists and is properly formatted
        if 'timestamp' not in df.columns:
            if 'Date' in df.columns:
                df['timestamp'] = pd.to_datetime(df['Date'])
                logger.info(f"Created timestamp from Date for {symbol}")
            else:
                # Create a timestamp if neither column exists
                logger.warning(f"No timestamp or Date column found for {symbol}, creating synthetic dates")
                df['timestamp'] = pd.date_range(end=datetime.now(), periods=len(df))

        # Ensure currentPrice exists
        if 'currentPrice' not in df.columns:
            if 'Close' in df.columns:
                df['currentPrice'] = df['Close']
                logger.info(f"Created currentPrice from Close for {symbol}")
            elif 'close' in df.columns:
                df['currentPrice'] = df['close']
                logger.info(f"Created currentPrice from close for {symbol}")
            else:
                logger.error(f"No price column found for {symbol}")
                raise HTTPException(status_code=500, detail=f"No price data found for {symbol}")

        # Log min and max values for key columns
        for col in ['currentPrice', 'open', 'dayHigh', 'dayLow', 'volumen']:
            if col in df.columns:
                logger.info(f"{col} range for {symbol}: min={df[col].min()}, max={df[col].max()}")

        # Procesamiento de fechas
        try:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            min_date = df['timestamp'].min()
            max_date = df['timestamp'].max()
            logger.info(f"Date range for {symbol}: {min_date} to {max_date}")
        except Exception as e:
            logger.error(f"Error processing timestamps for {symbol}: {str(e)}")
            # Create a backup timestamp column
            df['timestamp'] = pd.date_range(end=datetime.now(), periods=len(df))

        df = df.sort_values('timestamp')

        # Drop rows with NaN in critical columns
        nan_before = len(df)
        df = df.dropna(subset=['timestamp', 'currentPrice'])
        nan_after = len(df)
        if nan_before != nan_after:
            logger.warning(f"Dropped {nan_before - nan_after} rows with NaN values for {symbol}")

        # Calcular métricas de rentabilidad
        if len(df) >= 2:  # Ensure we have at least two rows for pct_change
            df['daily_return'] = df['currentPrice'].pct_change() * 100
            df['cumulative_return'] = (df['currentPrice'] / df['currentPrice'].iloc[0] - 1) * 100
        else:
            # Handle case with only one data point
            logger.warning(f"Not enough data points for {symbol} to calculate returns")
            df['daily_return'] = 0
            df['cumulative_return'] = 0

        # Limitar a los últimos N días
        before_filter = len(df)
        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=timeframe_days)
        df = df[df['timestamp'] >= cutoff_date]
        after_filter = len(df)
        logger.info(f"Filtered from {before_filter} to {after_filter} rows for {symbol} by date")

        # Limpieza final
        df = df.ffill().bfill().fillna(0)

        # Log final dataframe shape
        logger.info(f"Final dataframe for {symbol} has shape {df.shape}")

        # Check if we have any data left
        if df.empty:
            logger.error(f"No data remains for {symbol} after filtering")
            raise HTTPException(status_code=404,
                                detail=f"No hay datos en el rango de tiempo seleccionado para {symbol}")

        # Ensure all numeric columns are float
        numeric_cols = ['currentPrice', 'open', 'dayHigh', 'dayLow', 'daily_return', 'cumulative_return']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # Preparar respuesta
        response = {
            "symbol": symbol.upper(),
            "timestamps": df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist(),
            "prices": {
                "close": df['currentPrice'].tolist(),
                "open": df.get('open', df['currentPrice']).tolist(),
                "high": df.get('dayHigh', df['currentPrice']).tolist(),
                "low": df.get('dayLow', df['currentPrice']).tolist(),
                "daily_returns": df['daily_return'].tolist(),
                "cumulative_returns": df['cumulative_return'].tolist()
            },
            "volume": df.get('volumen', [0] * len(df)).tolist(),
            "last_close": float(df['currentPrice'].iloc[-1]) if len(df) > 0 else 0,
            "percentage_change": calculate_percentage_change(df['currentPrice']),
            "currency": "USD",
            "timeframe": timeframe
        }

        # Log data points in response
        logger.info(f"Response for {symbol} contains {len(response['timestamps'])} data points")

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error procesando datos para {symbol}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error interno procesando datos para {symbol}")

@app.post("/refresh")
def refresh_data(background_tasks: BackgroundTasks):
    """
    Fuerza una actualización de todos los datos en caché
    """
    background_tasks.add_task(background_update_all_dataframes)
    return {"message": "Actualización de datos iniciada en segundo plano"}


@app.exception_handler(StockAPIException)
async def stock_exception_handler(request: Request, exc: StockAPIException):
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


@app.get("/html/stocks/{symbol}/chart/{field}")
async def get_stock_chart_html(symbol: str, field: str):
    return FileResponse(os.path.join(static_dir, "stock_chart.html"))


@app.get("/html/compare")
async def get_compare_stocks_html():
    """
    Sirve la página HTML para comparar stocks.
    """
    return FileResponse(os.path.join(static_dir, "compare_stocks.html"))

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

@app.get("/html/stocks/{symbol}/advanced-chart")
async def serve_advanced_chart(symbol: str):
    return FileResponse(os.path.join(static_dir, "advanced_chart.html"))

@app.get("/html")
async def get_html_index():
    return FileResponse(os.path.join(static_dir, "index.html"))


@app.on_event("startup")
def startup_event():
    """
    Evento que se ejecuta al iniciar la aplicación
    """
    # Iniciar actualizaciones periódicas
    start_periodic_updates()
    logger.info("API inicializada y actualización periódica configurada")


if __name__ == "__main__":
    import uvicorn

    # Ejecutar la aplicación con Uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)