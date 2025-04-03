import os
import pandas as pd
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime
import time
import threading
import json
from pydantic import BaseModel

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


# Creación de la aplicación FastAPI
app = FastAPI(title="BVL Live Tracker API",
              description="API para consultar datos de acciones en tiempo real",
              version="1.0.0")

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
            # Verificar si el archivo ha sido modificado desde la última carga
            if current_modified_time > cache_item.last_modified:
                logger.info(f"Archivo CSV actualizado para {symbol}, recargando datos")
                needs_update = True
            # Verificar si han pasado más de 65 segundos desde la última carga (buffer de 5s)
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

                # Leer CSV
                df = pd.read_csv(file_path)

                # Manejar valores nulos - reemplazar con el valor anterior
                df = df.ffill()

                # Reemplazar los valores infinitos y NaN restantes con None
                df = df.replace([float('inf'), float('-inf')], None)
                df = df.where(pd.notnull(df), None)

                # Actualizar caché con el nuevo DataFrame y timestamp
                dataframes_cache[symbol] = CacheItem(df, current_modified_time)

                return df
            except Exception as e:
                logger.error(f"Error al cargar datos de {symbol}: {str(e)}")
                return None

        # Si llegamos aquí, algo salió mal
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
            else:
                symbols.append(f.split('_')[0].upper())
        return symbols
    except Exception as e:
        logger.error(f"Error al listar acciones: {str(e)}")
        return []


# get_latest_data function
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

    # Asegurar que las columnas requeridas estén presentes
    required_columns = ['currentPrice', 'previousClose', 'open', 'dayLow', 'dayHigh',
                        'dividendYield', 'financialCurrency', 'volumen', 'symbol', 'timestamp']

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

    # Ordenar por fecha
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

            symbol_data.sort(key=lambda x: x["timestamp"] if x["timestamp"] is not None else "")
            comparison_data[symbol] = symbol_data

    if not comparison_data:
        raise HTTPException(status_code=404, detail="No se encontraron datos para comparar")

    # Asegurar que los datos son JSON serializables
    comparison_data = clean_json_data(comparison_data)

    return comparison_data


@app.post("/refresh")
def refresh_data(background_tasks: BackgroundTasks):
    """
    Fuerza una actualización de todos los datos en caché
    """
    background_tasks.add_task(background_update_all_dataframes)
    return {"message": "Actualización de datos iniciada en segundo plano"}


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
                time.sleep(60)
            except Exception as e:
                logger.error(f"Error en el bucle de actualización: {str(e)}")
                # Si hay un error, esperar 10 segundos antes de reintentar
                time.sleep(10)

    # Iniciar el hilo de actualización
    update_thread = threading.Thread(target=update_loop, daemon=True)
    update_thread.start()
    logger.info("Hilo de actualización periódica iniciado")


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