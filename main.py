"""
Backend FastAPI para BVL - Versión para 3 empresas (2 similares, 1 diferente)
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any
import uvicorn

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("stocks-Backend")

# Configuración de las 3 empresas
EMPRESAS = {
    # Empresas con estructura similar
    "BAP": {
        "nombre": "CREDICORP LTD.",
        "archivo": "BAP_stockdata.csv",
        "columnas": {
            "nombre": "shortName",
            "precio": "currentPrice",
            "volumen": "volume",
            "apertura": "open",
            "minimo": "dayLow",
            "maximo": "dayHigh",
            "timestamp": "timestamp"
        }
    },
    "BRK-B": {
        "nombre": "Berkshire Hathaway",
        "archivo": "BRK-B_stockdata.csv",
        "columnas": {
            "nombre": "shortName",
            "precio": "currentPrice",
            "volumen": "volume",
            "apertura": "open",
            "minimo": "dayLow",
            "maximo": "dayHigh",
            "timestamp": "timestamp"
        }
    },
    # Empresa con estructura diferente
    "ILF": {
        "nombre": "iShares Latin America 40 ETF",
        "archivo": "ILF_stockdata.csv",
        "columnas": {
            "nombre": "shortName",
            "precio": "open",
            "volumen": "volume",
            "timestamp": "timestamp"
            # No tiene apertura, mínimo ni máximo
        }
    }
}

def encontrar_archivo(nombre_archivo: str) -> Path:
    """Busca el archivo CSV en ubicaciones posibles"""
    posibles_rutas = [
        Path(__file__).parent.parent.parent / "scraper" / "data" / nombre_archivo,
        Path(r"E:\papx\end_to_end_ml\nb_pr\tickets_live_tracker\scraper\data") / nombre_archivo,
        Path(__file__).parent.parent / "scraper" / "data" / nombre_archivo
    ]

    for ruta in posibles_rutas:
        if ruta.exists():
            logger.info(f"Archivo encontrado: {ruta}")
            return ruta

    raise FileNotFoundError(f"No se encontró {nombre_archivo} en: {posibles_rutas}")

class CargadorDatos:
    def __init__(self, config_empresa: Dict[str, Any]):
        self.config = config_empresa
        self.df = self._cargar_datos()

    def _cargar_datos(self) -> pd.DataFrame:
        """Carga datos validando las columnas requeridas"""
        try:
            ruta_archivo = encontrar_archivo(self.config["archivo"])
            df = pd.read_csv(ruta_archivo, parse_dates=[self.config["columnas"]["timestamp"]])

            # Verificar columnas mínimas requeridas
            columnas_requeridas = {
                self.config["columnas"]["nombre"],
                self.config["columnas"]["precio"],
                self.config["columnas"]["timestamp"]
            }

            if not columnas_requeridas.issubset(df.columns):
                faltantes = columnas_requeridas - set(df.columns)
                raise ValueError(f"Columnas faltantes: {faltantes}")

            # Limpiar nombre de empresa
            col_nombre = self.config["columnas"]["nombre"]
            df[col_nombre] = df[col_nombre].str.strip().str.upper()

            return df.sort_values(self.config["columnas"]["timestamp"])

        except Exception as e:
            logger.error(f"Error cargando {self.config['archivo']}: {str(e)}")
            raise

# Inicialización de FastAPI
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# Cargar datos para cada empresa
cargadores = {}
for codigo, config in EMPRESAS.items():
    try:
        cargadores[codigo] = CargadorDatos(config)
        logger.info(f"Datos cargados para {codigo} - {config['nombre']}")
    except Exception as e:
        logger.error(f"Error inicializando {codigo}: {str(e)}")
        # Crear dataframe vacío como fallback
        cargadores[codigo] = type('EmptyLoader', (), {'df': pd.DataFrame()})()

# Endpoints
@app.get("/health")
async def health_check():
    status = {
        "status": "running",
        "empresas": {}
    }
    for codigo in EMPRESAS:
        status["empresas"][codigo] = {
            "cargado": not cargadores[codigo].df.empty,
            "datos": len(cargadores[codigo].df) if not cargadores[codigo].df.empty else 0,
            "nombre": EMPRESAS[codigo]["nombre"]
        }
    return status

@app.get("/empresas")
async def listar_empresas():
    """Devuelve las empresas configuradas"""
    return [
        {"codigo": codigo, "nombre": config["nombre"]}
        for codigo, config in EMPRESAS.items()
    ]

@app.get("/datos/{empresa}")
async def obtener_datos_empresa(empresa: str, dias: int = 30):
    """Endpoint único para cualquier empresa"""
    if empresa not in EMPRESAS:
        raise HTTPException(
            status_code=404,
            detail=f"Empresa no configurada. Opciones: {list(EMPRESAS.keys())}"
        )

    cargador = cargadores[empresa]
    config = EMPRESAS[empresa]
    col = config["columnas"]

    if cargador.df.empty:
        raise HTTPException(
            status_code=503,
            detail=f"Datos no disponibles para {empresa}"
        )

    try:
        # Filtrar datos de la empresa
        datos_empresa = cargador.df[cargador.df[col["nombre"]] == config["nombre"]]

        if datos_empresa.empty:
            raise HTTPException(
                status_code=404,
                detail=f"No hay datos para {config['nombre']}"
            )

        # Último registro
        ultimo = datos_empresa.iloc[-1]

        # Datos en tiempo real
        realtime = {
            "precio": float(ultimo[col["precio"]]),
            "timestamp": ultimo[col["timestamp"]].timestamp()
        }

        # Añadir campos opcionales si existen
        if "volumen" in col and col["volumen"] in datos_empresa.columns:
            realtime["volumen"] = float(ultimo[col["volumen"]])
        if "apertura" in col and col["apertura"] in datos_empresa.columns:
            realtime["apertura"] = float(ultimo[col["apertura"]])
        if "minimo" in col and col["minimo"] in datos_empresa.columns:
            realtime["minimo"] = float(ultimo[col["minimo"]])
        if "maximo" in col and col["maximo"] in datos_empresa.columns:
            realtime["maximo"] = float(ultimo[col["maximo"]])

        # Datos históricos (últimos N días)
        fecha_corte = datetime.now() - timedelta(days=dias)
        historico = datos_empresa[datos_empresa[col["timestamp"]] >= fecha_corte]

        # Construir respuesta según columnas disponibles
        datos_historicos = []
        for _, fila in historico.iterrows():
            dato = {
                "precio": float(fila[col["precio"]]),
                "timestamp": fila[col["timestamp"]].timestamp()
            }

            # Añadir campos opcionales si existen
            if "volumen" in col and col["volumen"] in fila:
                dato["volumen"] = float(fila[col["volumen"]])
            if "apertura" in col and col["apertura"] in fila:
                dato["apertura"] = float(fila[col["apertura"]])
            if "minimo" in col and col["minimo"] in fila:
                dato["minimo"] = float(fila[col["minimo"]])
            if "maximo" in col and col["maximo"] in fila:
                dato["maximo"] = float(fila[col["maximo"]])

            datos_historicos.append(dato)

        return {
            "empresa": empresa,
            "nombre_completo": config["nombre"],
            "estructura": "completa" if "apertura" in col else "basica",
            "tiempo_real": realtime,
            "historico": datos_historicos,
            "metadata": {
                "dias": dias,
                "puntos_datos": len(datos_historicos),
                "desde": datetime.fromtimestamp(datos_historicos[-1]["timestamp"]).strftime("%Y-%m-%d"),
                "hasta": datetime.fromtimestamp(datos_historicos[0]["timestamp"]).strftime("%Y-%m-%d")
            }
        }

    except Exception as e:
        logger.error(f"Error procesando {empresa}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error procesando datos: {str(e)}"
        )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)