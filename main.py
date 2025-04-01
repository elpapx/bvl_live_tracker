"""
Backend FastAPI para BVL - Versión mejorada para 3 empresas
"""
from fastapi import FastAPI, APIRouter, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any, Optional
import uvicorn
import functools
import os

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
        "nombre_csv": "BERKSHIRE HATHAWAY",  # Uso de valor más genérico para la búsqueda
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
        "estructura": "completa",
        "busqueda_parcial": True  # Nueva opción para búsqueda parcial del nombre
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

# Configuración de rutas - Mejorada con detección automática
BASE_PATH = Path(__file__).parent
DATA_PATHS = [
    BASE_PATH.parent / "scraper" / "data",  # Ruta relativa desde main.py
    Path(r"E:\papx\end_to_end_ml\nb_pr\bvl_live_tracker1\scraper\data"),  # Ruta absoluta exacta
    Path(r"E:\papx\end_to_end_ml\nb_pr\tickets_live_tracker\scraper\data"),  # Ruta alternativa
    # Añadir detección automática relativa a la ubicación del script
    Path(os.path.dirname(os.path.abspath(__file__))) / "scraper" / "data"
]


def encontrar_archivo(nombre_archivo: str) -> Path:
    """Función mejorada para encontrar archivos con detección de errores"""
    # Primero verifica rutas exactas
    for ruta_base in DATA_PATHS:
        if not ruta_base.exists():
            continue

        ruta = ruta_base / nombre_archivo
        if ruta.exists():
            logger.info(f"Archivo encontrado en ubicación exacta: {ruta}")
            return ruta

    # Si no se encuentra, verifica variaciones de nombre
    variaciones = [
        nombre_archivo,
        nombre_archivo.replace('-', '_'),
        nombre_archivo.replace('_', '-')
    ]

    for ruta_base in DATA_PATHS:
        if not ruta_base.exists():
            continue

        for variacion in variaciones:
            ruta = ruta_base / variacion
            if ruta.exists():
                logger.info(f"Archivo encontrado (con variación): {ruta}")
                return ruta

    # Búsqueda recursiva como última opción
    for ruta_base in DATA_PATHS:
        if not ruta_base.exists():
            continue

        # Busca recursivamente en subdirectorios
        for item in ruta_base.glob('**/*_stockdata.csv'):
            if nombre_archivo.replace('-', '_') in str(item) or nombre_archivo.replace('_', '-') in str(item):
                logger.info(f"Archivo encontrado (búsqueda recursiva): {item}")
                return item

    # Diagnóstico detallado
    archivos_encontrados = []
    for ruta_base in DATA_PATHS:
        if ruta_base.exists():
            archivos = list(ruta_base.glob('*_stockdata.csv'))
            archivos_encontrados.extend(archivos)

    error_msg = (
        f"No se encontró {nombre_archivo} (ni variaciones) en:\n"
        f"{chr(10).join(str(p) for p in DATA_PATHS)}\n\n"
        f"Archivos CSV encontrados en estas rutas:\n"
        f"{chr(10).join(str(p) for p in archivos_encontrados)}"
    )
    raise FileNotFoundError(error_msg)


def mostrar_contenido_archivo(ruta: Path, max_lineas: int = 5) -> str:
    """Muestra las primeras líneas de un archivo para diagnóstico"""
    try:
        with open(ruta, 'r', encoding='utf-8-sig') as f:
            lineas = [next(f, None) for _ in range(max_lineas)]
            return "\n".join([l for l in lineas if l is not None])
    except Exception as e:
        return f"Error al leer archivo: {str(e)}"


class CargadorDatosEmpresa:
    def __init__(self, config: Dict[str, Any], codigo: str):
        self.config = config
        self.codigo = codigo
        self.nombre = config["nombre"]
        self.columnas = config["columnas"]
        self.df = self._cargar_datos()

    def _cargar_datos(self) -> pd.DataFrame:
        try:
            ruta = encontrar_archivo(self.config["archivo"])
            timestamp_col = self.config["columnas"]["timestamp"]
            nombre_col = self.config["columnas"]["nombre"]

            # Diagnóstico pre-carga
            logger.info(f"Intentando cargar {ruta} para {self.codigo}")
            contenido_muestra = mostrar_contenido_archivo(ruta)
            logger.info(f"Muestra del archivo {ruta}:\n{contenido_muestra}")

            df = pd.read_csv(ruta, encoding='utf-8-sig', low_memory=False)

            # Mostrar todas las columnas disponibles
            logger.info(f"Columnas disponibles en CSV: {list(df.columns)}")

            # Verificar columnas requeridas
            required_columns = {
                self.config["columnas"]["nombre"],
                self.config["columnas"]["precio"],
                timestamp_col
            }
            missing = required_columns - set(df.columns)
            if missing:
                raise ValueError(f"Columnas faltantes en CSV: {missing}")

            # Convertir timestamp
            df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')
            df = df[df[timestamp_col].notna()]

            # Filtrar por nombre de empresa (con opción de búsqueda parcial)
            nombre_buscado = self.config["nombre_csv"].upper()
            df[nombre_col] = df[nombre_col].astype(str).str.strip().str.upper()

            # Mostrar nombres disponibles para diagnóstico
            nombres_unicos = df[nombre_col].unique()
            logger.info(f"Nombres encontrados en {self.codigo}: {nombres_unicos}")

            if self.config.get("busqueda_parcial", False):
                # Búsqueda parcial (contiene el nombre)
                df = df[df[nombre_col].str.contains(nombre_buscado)]
                if df.empty:
                    # Intento adicional con partes del nombre
                    partes_nombre = nombre_buscado.split()
                    if len(partes_nombre) > 1:
                        for parte in partes_nombre:
                            if len(parte) > 3:  # Solo buscar palabras significativas
                                temp_df = df[df[nombre_col].str.contains(parte)]
                                if not temp_df.empty:
                                    df = temp_df
                                    logger.info(f"Encontrados registros usando parte del nombre: '{parte}'")
                                    break
            else:
                # Búsqueda exacta (igual al nombre)
                df = df[df[nombre_col] == nombre_buscado]

            if df.empty:
                logger.warning(
                    f"No se encontraron registros para '{nombre_buscado}'. Nombres encontrados: {nombres_unicos}")

                # Sugerencia de posibles coincidencias
                sugerencias = []
                for nombre in nombres_unicos:
                    if any(parte in nombre for parte in nombre_buscado.split()) or \
                            any(parte in nombre_buscado for parte in nombre.split()):
                        sugerencias.append(nombre)

                if sugerencias:
                    logger.info(f"Posibles coincidencias para '{nombre_buscado}': {sugerencias}")

                return pd.DataFrame()

            df = df.sort_values(timestamp_col)
            logger.info(f"Datos cargados para {self.codigo}: {len(df)} registros válidos")
            return df

        except Exception as e:
            logger.error(f"Error cargando {self.config['archivo']}: {str(e)}", exc_info=True)
            return pd.DataFrame()

    def actualizar_configuracion(self, nueva_config: Dict[str, Any]) -> bool:
        """Permite actualizar la configuración y recargar datos"""
        try:
            self.config.update(nueva_config)
            self.df = self._cargar_datos()
            return not self.df.empty
        except Exception as e:
            logger.error(f"Error actualizando configuración: {str(e)}")
            return False

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

    @router.post("/actualizar-config", summary=f"Actualiza configuración para {cargador.nombre}")
    async def actualizar_config(nueva_config: Dict[str, Any]):
        """Permite actualizar la configuración de la empresa en tiempo de ejecución"""
        try:
            exito = cargador.actualizar_configuracion(nueva_config)
            return {
                "empresa": cargador.codigo,
                "nombre": cargador.nombre,
                "exito": exito,
                "datos_disponibles": not cargador.df.empty,
                "registros": len(cargador.df),
                "config_actual": cargador.config
            }
        except Exception as e:
            logger.error(f"Error actualizando config {cargador.codigo}: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=500,
                detail=f"Error al actualizar configuración: {str(e)}"
            )

    return router


## Aplicación principal ##
app = FastAPI(title="API de Acciones BVL", version="3.0")

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
            'config': config,
            'actualizar_configuracion': lambda nueva_config: False
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
                "archivo": cargador.config["archivo"]
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


@app.post("/fix-brk", summary="Corregir configuración para BRK-B")
async def fix_brk_config():
    """Endpoint especial para probar diferentes configuraciones para BRK-B"""
    if "BRK-B" not in cargadores:
        raise HTTPException(
            status_code=404,
            detail="Empresa BRK-B no encontrada"
        )

    # Leer el archivo CSV para extraer nombres posibles
    try:
        ruta = encontrar_archivo("BRK_B_stockdata.csv")
        df = pd.read_csv(ruta, encoding='utf-8-sig', low_memory=False)
        nombre_col = EMPRESA_CONFIGS["BRK-B"]["columnas"]["nombre"]

        if nombre_col not in df.columns:
            return {"error": f"Columna {nombre_col} no encontrada", "columnas_disponibles": list(df.columns)}

        nombres = df[nombre_col].astype(str).str.strip().str.upper().unique().tolist()

        # Intentar varias configuraciones
        configs_a_probar = []

        # Usar los nombres encontrados
        for nombre in nombres:
            if nombre and len(nombre) > 3:  # Evitar nombres vacíos o muy cortos
                configs_a_probar.append({"nombre_csv": nombre})

        # Si no hay nombres, probar con variantes genéricas
        if not configs_a_probar:
            configs_a_probar = [
                {"nombre_csv": "BERKSHIRE"},
                {"nombre_csv": "BERKSHIRE HATHAWAY"},
                {"nombre_csv": "BRK"},
                {"nombre_csv": "BRK-B"},
                {"nombre_csv": "BRK.B"}
            ]

        # Probar cada configuración
        resultados = []
        for config in configs_a_probar:
            config["busqueda_parcial"] = True
            exito = cargadores["BRK-B"].actualizar_configuracion(config)
            resultados.append({
                "config": config,
                "exito": exito,
                "registros": len(cargadores["BRK-B"].df)
            })

            # Si encontramos una que funcione, terminamos
            if exito:
                break

        return {
            "empresa": "BRK-B",
            "nombres_encontrados": nombres,
            "resultados": resultados,
            "config_final": cargadores["BRK-B"].config,
            "datos_disponibles": not cargadores["BRK-B"].df.empty,
            "registros": len(cargadores["BRK-B"].df)
        }

    except Exception as e:
        logger.error(f"Error en fix-brk: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error al corregir BRK-B: {str(e)}"
        )


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)