from fastapi import FastAPI, HTTPException, Depends
import pandas as pd
from pathlib import Path
from typing import List, Optional, Dict, Any
import time
import os
import threading
import asyncio
from fastapi.middleware.cors import CORSMiddleware
import logging
from contextlib import asynccontextmanager
import plotly.graph_objs as go
from fastapi.responses import HTMLResponse, FileResponse
from io import BytesIO

# Configuración avanzada de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("api_service.log", encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Configuración portable multiplataforma
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"

# Crear directorios necesarios con permisos adecuados
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Cache con bloqueo para concurrencia
data_cache: Dict[str, tuple] = {}
cache_lock = threading.Lock()
CACHE_TIMEOUT = 300

# Cache para gráficos
CHART_CACHE = {}
CHART_CACHE_TIMEOUT = 600  # 10 minutos

# Columnas prioritarias según especificación
PRIORITY_COLUMNS = [
    'currentPrice', 'previousClose', 'open',
    'dayLow', 'dividendYield', 'financialCurrency',
    'volumen', 'symbol', 'timestamp'
]


def detect_priority_columns(df: pd.DataFrame) -> Dict[str, str]:
    """Detecta y mapea columnas prioritarias respetando nombres originales"""
    column_map = {}
    df_columns = df.columns.str.strip()

    # Buscar coincidencias exactas primero
    for target_col in PRIORITY_COLUMNS:
        if target_col in df_columns:
            column_map[target_col] = target_col
            logger.debug(f"Columna prioritaria detectada: {target_col}")

    # Búsqueda case-insensitive para columnas restantes
    remaining_cols = [col for col in PRIORITY_COLUMNS if col not in column_map]
    for target_col in remaining_cols:
        matches = [col for col in df_columns if col.lower() == target_col.lower()]
        if matches:
            column_map[target_col] = matches[0]
            logger.debug(f"Mapeo case-insensitive: {target_col} -> {matches[0]}")

    # Validar columnas críticas
    required_cols = ['timestamp', 'symbol']
    for col in required_cols:
        if col not in column_map:
            logger.error(f"Columna requerida faltante: {col}")
            raise ValueError(f"Columna {col} no encontrada en datos")

    logger.info(f"Mapeo de columnas completado: {column_map}")
    return column_map


def load_stock_data(symbol: str) -> Optional[pd.DataFrame]:
    """Carga y normaliza datos manteniendo nombres originales de columnas"""
    symbol = symbol.upper()
    cache_key = f"{symbol}_data"

    with cache_lock:
        if cache_key in data_cache:
            cached_data, timestamp = data_cache[cache_key]
            if (time.time() - timestamp) < CACHE_TIMEOUT:
                logger.debug(f"Cache hit para {symbol}")
                return cached_data.copy()

    file_patterns = [f"{symbol}_*.csv", f"{symbol}.csv"]
    found_files = []
    for pattern in file_patterns:
        found_files.extend(DATA_DIR.glob(pattern))
        if found_files:
            break

    if not found_files:
        logger.warning(f"Archivo no encontrado para {symbol}")
        return None

    try:
        file_path = found_files[0]
        logger.info(f"Cargando {symbol} desde {file_path.name}")

        df = pd.read_csv(file_path)
        df = df.rename(columns=lambda x: x.strip())

        # Normalización de tipos de datos
        numeric_cols = ['currentPrice', 'previousClose', 'open', 'dayLow', 'volumen']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

        # Aplicar mapeo de columnas prioritarias
        column_map = detect_priority_columns(df)
        df = df.rename(columns={v: k for k, v in column_map.items()})
        df = df[[col for col in PRIORITY_COLUMNS if col in df.columns]]

        # Cache con control de errores
        try:
            with cache_lock:
                data_cache[cache_key] = (df.copy(), time.time())
        except Exception as cache_error:
            logger.error(f"Error actualizando cache: {str(cache_error)}")

        return df

    except Exception as e:
        logger.error(f"Error cargando {symbol}: {str(e)}", exc_info=True)
        return None


def generate_price_chart(df: pd.DataFrame, symbol: str, chart_type: str = 'line') -> go.Figure:
    """Genera un gráfico interactivo con estilo oscuro"""
    df = df.sort_values('timestamp', ascending=True)

    # Configuración de colores
    dark_theme = {
        'background': '#111111',
        'text': '#FFFFFF',
        'grid': '#2E2E2E',
        'line': '#00FF88',
        'marker': '#FFAA00'
    }

    # Crear traza según el tipo de gráfico
    if chart_type == 'candlestick' and all(col in df.columns for col in ['open', 'high', 'low', 'close']):
        trace = go.Candlestick(
            x=df['timestamp'],
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            increasing_line_color='#2ECC71',
            decreasing_line_color='#E74C3C',
            name='Velas'
        )
    else:
        trace = go.Scatter(
            x=df['timestamp'],
            y=df['currentPrice'],
            mode='lines+markers',
            name='Precio',
            line=dict(color=dark_theme['line'], width=2),
            marker=dict(size=6, color=dark_theme['marker']),
            hovertemplate='<b>%{x|%Y-%m-%d}</b><br>$%{y:.2f}<extra></extra>'
        )

    # Configurar layout oscuro
    layout = go.Layout(
        title=dict(
            text=f'Histórico de Precios - {symbol}',
            font=dict(color=dark_theme['text'])
        ),
        xaxis=dict(
            title='Fecha',
            gridcolor=dark_theme['grid'],
            rangeslider=dict(visible=True),
            color=dark_theme['text']
        ),
        yaxis=dict(
            title='Precio (USD)',
            gridcolor=dark_theme['grid'],
            tickprefix='$',
            color=dark_theme['text']
        ),
        plot_bgcolor=dark_theme['background'],
        paper_bgcolor=dark_theme['background'],
        font=dict(color=dark_theme['text']),
        hovermode='x unified',
        height=600
    )

    return go.Figure(data=[trace], layout=layout)

# Modificar la función lifespan de esta manera:
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manejador del ciclo de vida corregido"""
    logger.info("Iniciando API - Validando entorno...")

    # Verificar permisos de escritura en logs
    try:
        with open(LOG_DIR / "startup_test.log", "w") as f:
            f.write("Test de permisos\n")
        os.remove(LOG_DIR / "startup_test.log")
    except Exception as e:
        logger.critical(f"Error de permisos en directorio de logs: {str(e)}")
        raise

    # Precarga inicial en segundo plano
    async def preload_essentials():
        """Precarga corregida usando list_available_symbols"""
        try:
            logger.info("Iniciando precarga de datos esenciales...")

            if not DATA_DIR.exists():
                logger.warning(f"Directorio de datos no encontrado: {DATA_DIR}")
                return

            # Obtener símbolos usando el endpoint
            symbols_response = await list_available_symbols()
            symbols = [s['symbol'] for s in symbols_response.get('symbols', [])[:3]]

            if not symbols:
                logger.warning("No se encontraron símbolos para precargar")
                return

            logger.info(f"Precargando datos para: {symbols}")

            # Precarga síncrona (load_stock_data no es async)
            for symbol in symbols:
                try:
                    df = load_stock_data(symbol)
                    if df is not None:
                        logger.debug(f"Precargado {symbol}: {len(df)} registros")
                except Exception as e:
                    logger.error(f"Error precargando {symbol}: {str(e)}", exc_info=True)

            logger.info("Precarga completada")

        except Exception as e:
            logger.critical(f"Error crítico en precarga: {str(e)}", exc_info=True)

    # Ejecutar la precarga como tarea
    asyncio.create_task(preload_essentials())

    yield  # La aplicación está lista

    logger.info("Apagando API - Liberando recursos...")


app = FastAPI(
    title="Financial Data API",
    description="API para datos financieros con visualizaciones interactivas",
    lifespan=lifespan,
    docs_url="/"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)


# Dependencias API
async def validate_symbol(symbol: str) -> pd.DataFrame:
    df = load_stock_data(symbol)
    if df is None or df.empty:
        logger.error(f"Símbolo inválido o datos vacíos: {symbol}")
        raise HTTPException(404, detail="Símbolo no encontrado o datos corruptos")
    return df


# Endpoints principales
@app.get("/financial-data/{symbol}", response_model=List[Dict[str, Any]])
async def get_full_financial_data(
        symbol: str,
        limit: int = 100,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        df: pd.DataFrame = Depends(validate_symbol)
):
    """Endpoint principal para todos los datos financieros"""
    logger.info(f"Solicitud de datos completos para {symbol}")

    # Filtrar por fechas
    if start_date:
        df = df[df['timestamp'] >= pd.to_datetime(start_date)]
    if end_date:
        df = df[df['timestamp'] <= pd.to_datetime(end_date)]

    # Ordenar y limitar
    result = df.sort_values('timestamp', ascending=False).head(limit)
    return result.to_dict(orient='records')


@app.get("/financial-data/{symbol}/key-metrics", response_model=Dict[str, Any])
async def get_key_metrics(
        symbol: str,
        df: pd.DataFrame = Depends(validate_symbol)
):
    """Obtiene las métricas clave más recientes"""
    logger.info(f"Solicitud de métricas clave para {symbol}")

    latest = df.iloc[0]
    metrics = {
        'currentPrice': latest.get('currentPrice'),
        'previousClose': latest.get('previousClose'),
        'dayLow': latest.get('dayLow'),
        'dividendYield': latest.get('dividendYield'),
        'volume': latest.get('volumen')
    }

    return {k: v for k, v in metrics.items() if pd.notnull(v)}


@app.get("/financial-data/{symbol}/chart", response_class=HTMLResponse)
async def generate_price_chart_endpoint(
        symbol: str,
        chart_type: str = 'line',
        df: pd.DataFrame = Depends(validate_symbol)
):
    """Genera un gráfico interactivo de precios"""
    logger.info(f"Generando gráfico para {symbol} - Tipo: {chart_type}")

    if 'currentPrice' not in df.columns:
        raise HTTPException(400, detail="Datos de precio no disponibles")

    # Generar clave única para el caché
    cache_key = f"{symbol}_{chart_type}"

    # Verificar caché
    if cache_key in CHART_CACHE:
        cached_time, chart_html = CHART_CACHE[cache_key]
        if (time.time() - cached_time) < CHART_CACHE_TIMEOUT:
            logger.debug(f"Usando gráfico en caché para {symbol}")
            return HTMLResponse(content=chart_html)

    # Generar nuevo gráfico
    try:
        fig = generate_price_chart(df, symbol, chart_type)
        chart_html = fig.to_html(full_html=False, include_plotlyjs='cdn')

        # Actualizar caché
        CHART_CACHE[cache_key] = (time.time(), chart_html)

        return HTMLResponse(content=chart_html)

    except Exception as e:
        logger.error(f"Error generando gráfico: {str(e)}", exc_info=True)
        raise HTTPException(500, detail="Error al generar el gráfico")


@app.get("/financial-data/{symbol}/chart-image")
async def generate_price_chart_image(
        symbol: str,
        chart_type: str = 'line',
        width: int = 1200,
        height: int = 800,
        df: pd.DataFrame = Depends(validate_symbol)
):
    """Versión en imagen del gráfico (PNG)"""
    logger.info(f"Generando imagen de gráfico para {symbol}")

    try:
        fig = generate_price_chart(df, symbol, chart_type)
        img_bytes = fig.to_image(format="png", width=width, height=height)

        return FileResponse(
            BytesIO(img_bytes),
            media_type="image/png",
            filename=f"{symbol}_chart.png"
        )
    except Exception as e:
        logger.error(f"Error generando imagen: {str(e)}", exc_info=True)
        raise HTTPException(500, detail="Error al generar la imagen")


@app.get("/metadata/symbols")
async def list_available_symbols():
    """Lista de símbolos disponibles con metadatos"""
    logger.info("Generando lista de símbolos disponibles")

    symbols = []
    try:
        for csv_file in DATA_DIR.glob("*.csv"):
            try:
                symbol = csv_file.stem.split('_')[0].upper()
                stats = csv_file.stat()
                symbols.append({
                    'symbol': symbol,
                    'filename': csv_file.name,
                    'last_modified': stats.st_mtime,
                    'size_mb': round(stats.st_size / (1024 * 1024), 3)
                })
            except Exception as e:
                logger.error(f"Error procesando archivo {csv_file}: {str(e)}")
                continue

    except Exception as e:
        logger.critical(f"Error accediendo al directorio de datos: {str(e)}")
        raise HTTPException(500, detail="Error al leer archivos de datos")

    return {"symbols": symbols}


@app.get("/health")
async def health_check():
    """Endpoint de salud avanzado"""
    health_data = {
        'status': 'ok',
        'timestamp': time.time(),
        'data_dir': str(DATA_DIR),
        'cache_entries': len(data_cache),
        'active_symbols': [k.split('_')[0] for k in data_cache.keys()],
        'system_load': os.getloadavg()
    }
    return health_data


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)