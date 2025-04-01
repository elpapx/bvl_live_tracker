from fastapi import FastAPI, APIRouter, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any, Optional
import uvicorn
import functools

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("stocks-Backend")

## Configuración de Empresas ##
EMPRESA_CONFIGS = {
    "BAP": {
        "nombre": "CREDICORP LTD.",
        "nombre_csv": "CREDICORP LTD.",
        "archivo": "BAP_stockdata.csv",
        "columnas": {
            "nombre": "shortName",
            "precio": "currentPrice",
            "volumen": "volume",
            "apertura": "open",
            "minimo": "dayLow",
            "maximo": "dayHigh",
            "timestamp": "timestamp"
        },
        "estructura": "completa"
    },
    "BRK-B": {
        "nombre": "Berkshire Hathaway",
        "nombre_csv": "BERKSHIRE HATHAWAY INC.",  # Ajustar según CSV real
        "archivo": "BRK_B_stockdata.csv",
        "columnas": {
            "nombre": "shortName",
            "precio": "currentPrice",
            "volumen": "volume",
            "apertura": "open",
            "minimo": "dayLow",
            "maximo": "dayHigh",
            "timestamp": "timestamp"
        },
        "estructura": "completa"
    },
    "ILF": {
        "nombre": "iShares Latin America 40 ETF",
        "nombre_csv": "ISHARES LATIN AMERICA 40 ETF",
        "archivo": "ILF_stockdata.csv",
        "columnas": {
            "nombre": "shortName",
            "precio": "open",
            "volumen": "volume",
            "timestamp": "timestamp"
        },
        "estructura": "basica"
    }
}

# Rutas de datos actualizadas
DATA_PATHS = [
    Path(__file__).parent.parent / "scraper" / "data",  # Ruta relativa
    Path(r"/scraper/data"),  # Ruta exacta
    Path(r"E:\papx\end_to_end_ml\nb_pr\tickets_live_tracker\scraper\data")  # Alternativa
]


def encontrar_archivo(nombre_archivo: str) -> Path:
    """Busca archivos CSV considerando variaciones de nombre"""
    # Lista de variaciones posibles del nombre de archivo
    variaciones = [
        nombre_archivo,
        nombre_archivo.replace('-', '_'),
        nombre_archivo.replace('_', '-')
    ]

    for ruta_base in DATA_PATHS:
        for variacion in variaciones:
            ruta = ruta_base / variacion
            if ruta.exists():
                logger.info(f"Archivo encontrado: {ruta}")
                return ruta

    # Diagnóstico detallado
    archivos_encontrados = []
    for ruta_base in DATA_PATHS:
        if ruta_base.exists():
            archivos = list(ruta_base.glob('*stockdata.csv'))
            archivos_encontrados.extend(archivos)

    error_msg = (
        f"No se encontró {nombre_archivo} (ni variaciones) en:\n"
        f"{chr(10).join(str(p) for p in DATA_PATHS)}\n\n"
        f"Archivos CSV encontrados en estas rutas:\n"
        f"{chr(10).join(str(p) for p in archivos_encontrados)}"
    )
    raise FileNotFoundError(error_msg)


class CargadorDatosEmpresa:
    """Clase dedicada a cargar y manejar datos para una sola empresa"""

    def __init__(self, config: Dict[str, Any], codigo: str):
        self.config = config
        self.codigo = codigo
        self.nombre = config["nombre"]
        self.columnas = config["columnas"]
        self.df = self._cargar_datos()

    def _cargar_datos(self) -> pd.DataFrame:
        """Carga y valida los datos para una empresa específica"""
        try:
            ruta = encontrar_archivo(self.config["archivo"])
            timestamp_col = self.config["columnas"]["timestamp"]
            nombre_col = self.config["columnas"]["nombre"]

            # Leer CSV con manejo robusto
            df = pd.read_csv(ruta, encoding='utf-8-sig', low_memory=False)

            # Verificar columnas requeridas
            required_columns = {
                self.config["columnas"]["nombre"],
                self.config["columnas"]["precio"],
                timestamp_col
            }
            missing = required_columns - set(df.columns)
            if missing:
                raise ValueError(f"Columnas faltantes en CSV: {missing}")

            # Convertir timestamp (manejo flexible de formatos)
            df[timestamp_col] = pd.to_datetime(df[timestamp_col], format='mixed', errors='coerce')
            df = df[df[timestamp_col].notna()]

            # Filtrar por nombre de empresa (búsqueda flexible)
            nombre_buscado = self.config["nombre_csv"].upper()
            df[nombre_col] = df[nombre_col].astype(str).str.strip().str.upper()

            # Primero intenta coincidencia exacta
            df_filtrado = df[df[nombre_col] == nombre_buscado]

            # Si no hay resultados, intenta búsqueda parcial
            if df_filtrado.empty:
                df_filtrado = df[df[nombre_col].str.contains(nombre_buscado.split()[0], case=False, na=False)]

            if df_filtrado.empty:
                nombres_unicos = df[nombre_col].unique()
                logger.warning(
                    f"No se encontraron registros para '{nombre_buscado}'. "
                    f"Nombres encontrados: {nombres_unicos}\n"
                    f"Primeras filas del CSV:\n{df.head(2)}"
                )
                return pd.DataFrame()

            # Ordenar por timestamp
            df_filtrado = df_filtrado.sort_values(timestamp_col)

            logger.info(f"Datos cargados para {self.codigo}: {len(df_filtrado)} registros válidos")
            return df_filtrado

        except Exception as e:
            logger.error(f"Error cargando {self.config['archivo']}: {str(e)}", exc_info=True)
            return pd.DataFrame()

    @functools.lru_cache(maxsize=8)
    def get_historical(self, days: int = 30) -> pd.DataFrame:
        """Obtiene datos históricos con caché"""
        if self.df.empty or days <= 0:
            return pd.DataFrame()

        cutoff = datetime.now() - timedelta(days=days)
        return self.df[self.df[self.columnas["timestamp"]] >= cutoff]

    def get_realtime(self) -> Optional[Dict[str, Any]]:
        """Obtiene los datos más recientes"""
        if self.df.empty:
            return None
        return self._process_row(self.df.iloc[-1])

    def _process_row(self, row: pd.Series) -> Dict[str, Any]:
        """Convierte una fila del DataFrame a un diccionario"""
        data = {
            "precio": float(row[self.columnas["precio"]]),
            "timestamp": row[self.columnas["timestamp"]].timestamp()
        }

        # Campos opcionales
        optional_fields = {
            "volumen": "volumen",
            "apertura": "apertura",
            "minimo": "minimo",
            "maximo": "maximo"
        }

        for field, col_key in optional_fields.items():
            if col_key in self.columnas and self.columnas[col_key] in row:
                try:
                    data[field] = float(row[self.columnas[col_key]])
                except (ValueError, TypeError):
                    data[field] = None

        return data


## Sistema de enrutamiento modular ##
def crear_router_empresa(cargador: CargadorDatosEmpresa) -> APIRouter:
    """Crea un router FastAPI específico para una empresa"""
    router = APIRouter(prefix=f"/{cargador.codigo}", tags=[cargador.nombre])

    @router.get("/", summary=f"Datos para {cargador.nombre}")
    async def get_datos(dias: int = 30):
        if cargador.df.empty:
            raise HTTPException(
                status_code=503,
                detail=(
                    f"Datos no disponibles para {cargador.nombre}. "
                    f"Verifique que el archivo {cargador.config['archivo']} "
                    f"contenga registros para '{cargador.config['nombre_csv']}'"
                )
            )

        try:
            historico = cargador.get_historical(dias)
            if historico.empty:
                raise HTTPException(
                    status_code=404,
                    detail=f"No hay datos para {cargador.nombre} en los últimos {dias} días"
                )

            realtime = cargador.get_realtime()
            if not realtime:
                raise HTTPException(
                    status_code=404,
                    detail=f"No se encontraron datos recientes para {cargador.nombre}"
                )

            historical_data = [
                cargador._process_row(row)
                for _, row in historico.iterrows()
            ]

            return {
                "empresa": cargador.codigo,
                "nombre": cargador.nombre,
                "tiempo_real": realtime,
                "historico": historical_data,
                "metadata": {
                    "dias": dias,
                    "puntos_datos": len(historical_data),
                    "estructura": cargador.config["estructura"],
                    "archivo_origen": cargador.config["archivo"],
                    "nombre_en_csv": cargador.config["nombre_csv"],
                    "ultima_actualizacion": datetime.now().isoformat()
                }
            }
        except Exception as e:
            logger.error(f"Error procesando {cargador.codigo}: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=500,
                detail=f"Error al procesar datos: {str(e)}"
            )

    return router


## Aplicación principal ##
app = FastAPI(title="API de Acciones BVL", version="2.3")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# Cargadores y routers para cada empresa
cargadores = {}
for codigo, config in EMPRESA_CONFIGS.items():
    try:
        cargador = CargadorDatosEmpresa(config, codigo)
        cargadores[codigo] = cargador
        router = crear_router_empresa(cargador)
        app.include_router(router)
        logger.info(f"API creada para {codigo} - {config['nombre']} (Registros: {len(cargador.df)})")
    except Exception as e:
        logger.error(f"Error inicializando {codigo}: {str(e)}", exc_info=True)
        # Crear cargador vacío como fallback
        cargadores[codigo] = type('EmptyLoader', (), {
            'df': pd.DataFrame(),
            'get_historical': lambda days=30: pd.DataFrame(),
            'get_realtime': lambda: None,
            'nombre': config['nombre'],
            'codigo': codigo,
            'config': config
        })()


# Endpoints globales
@app.get("/health", summary="Estado del sistema")
async def health_check():
    return {
        "status": "running",
        "timestamp": datetime.now().isoformat(),
        "empresas": [
            {
                "codigo": codigo,
                "nombre": cargador.nombre,
                "disponible": not cargador.df.empty,
                "registros": len(cargador.df),
                "ruta": f"/{codigo}",
                "archivo": cargador.config["archivo"],
                "nombre_buscado": cargador.config["nombre_csv"]
            }
            for codigo, cargador in cargadores.items()
        ]
    }


@app.get("/empresas", summary="Lista de empresas disponibles")
async def listar_empresas():
    return [
        {
            "codigo": codigo,
            "nombre": config["nombre"],
            "estructura": config["estructura"],
            "ruta": f"/{codigo}",
            "archivo": config["archivo"],
            "nombre_en_csv": config["nombre_csv"],
            "datos_disponibles": not cargadores[codigo].df.empty
        }
        for codigo, config in EMPRESA_CONFIGS.items()
    ]


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)