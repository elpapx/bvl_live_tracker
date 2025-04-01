import requests
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import numpy as np
import argparse
import sys
from typing import Dict, List, Any, Optional


class BVLStockPlotter:
    """Clase para visualizar datos de acciones desde la API de BVL"""

    def __init__(self, base_url: str = "http://localhost:8000"):
        """Inicializa el plotter con la URL base de la API"""
        self.base_url = base_url
        self.empresas = self._fetch_empresas()

        # Configuración de estilos para matplotlib
        plt.style.use('seaborn-v0_8-darkgrid')
        self.colors = {
            "BAP": "#1f77b4",  # Azul
            "BRK-B": "#ff7f0e",  # Naranja
            "ILF": "#2ca02c"  # Verde
        }

    def _fetch_empresas(self) -> List[Dict[str, Any]]:
        """Obtiene la lista de empresas disponibles"""
        try:
            response = requests.get(f"{self.base_url}/empresas")
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error obteniendo lista de empresas: {e}")
            return []

    def get_empresa_names(self) -> List[str]:
        """Retorna los códigos de las empresas disponibles"""
        return [empresa["codigo"] for empresa in self.empresas]

    def fetch_data(self, codigo: str, dias: int = 30) -> Optional[Dict[str, Any]]:
        """Obtiene datos para una empresa específica"""
        try:
            response = requests.get(f"{self.base_url}/{codigo}", params={"dias": dias})
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error obteniendo datos para {codigo}: {e}")
            if hasattr(e.response, 'text'):
                print(f"Respuesta: {e.response.text}")
            return None

    def prepare_dataframe(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Convierte los datos de la API en un DataFrame para graficar"""
        if not data or "historico" not in data or not data["historico"]:
            return pd.DataFrame()

        df = pd.DataFrame(data["historico"])

        # Convertir timestamp a datetime
        df["fecha"] = pd.to_datetime(df["timestamp"], unit='s')

        # Ordenar por fecha
        df = df.sort_values("fecha")

        return df

    def plot_precio(self, codigo: str, dias: int = 30, save_path: Optional[str] = None):
        """Genera un gráfico de precios para una empresa"""
        data = self.fetch_data(codigo, dias)
        if not data:
            print(f"No se pudieron obtener datos para {codigo}")
            return

        df = self.prepare_dataframe(data)
        if df.empty:
            print(f"No hay datos históricos para {codigo}")
            return

        fig, ax = plt.subplots(figsize=(12, 6))

        # Gráfico de precio
        ax.plot(df["fecha"], df["precio"],
                color=self.colors.get(codigo, "blue"),
                linewidth=2,
                label=f"Precio ({data['nombre']})")

        # Añadir punto para el valor actual
        ultimo_precio = df["precio"].iloc[-1]
        ax.scatter(df["fecha"].iloc[-1], ultimo_precio,
                   color='red', s=80, zorder=5)

        # Añadir texto con el último precio
        ax.annotate(f"${ultimo_precio:.2f}",
                    (df["fecha"].iloc[-1], ultimo_precio),
                    xytext=(10, 10), textcoords='offset points',
                    fontsize=12, fontweight='bold')

        # Configuración del eje X
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
        plt.xticks(rotation=45)

        # Añadir cuadrícula
        ax.grid(True, linestyle='--', alpha=0.7)

        # Añadir título y etiquetas
        ax.set_title(f"Evolución del Precio - {data['nombre']} ({codigo})", fontsize=16)
        ax.set_xlabel("Fecha", fontsize=12)
        ax.set_ylabel("Precio ($)", fontsize=12)

        # Añadir metadatos
        ax.text(0.01, 0.01,
                f"Periodo: Últimos {dias} días\nPrecio actual: ${ultimo_precio:.2f}\nFecha actualización: {data['metadata']['ultima_actualizacion'][:10]}",
                transform=ax.transAxes, fontsize=10,
                bbox=dict(facecolor='white', alpha=0.8))

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path)
            print(f"Gráfico guardado en {save_path}")
        else:
            plt.show()

        plt.close()

    def plot_comparacion(self, codigos: List[str], dias: int = 30, metrica: str = "precio",
                         save_path: Optional[str] = None):
        """Genera un gráfico comparativo entre varias empresas"""
        if not codigos:
            print("Debe especificar al menos un código de empresa")
            return

        fig, ax = plt.subplots(figsize=(14, 7))

        # Para normalizar los valores
        normalizar = len(codigos) > 1

        for codigo in codigos:
            data = self.fetch_data(codigo, dias)
            if not data:
                print(f"No se pudieron obtener datos para {codigo}")
                continue

            df = self.prepare_dataframe(data)
            if df.empty:
                print(f"No hay datos históricos para {codigo}")
                continue

            if metrica not in df.columns:
                print(f"La métrica '{metrica}' no está disponible para {codigo}")
                continue

            # Normalizar valores si se comparan múltiples empresas
            valores = df[metrica]
            if normalizar:
                valores = (valores / valores.iloc[0]) * 100

            # Graficar
            ax.plot(df["fecha"], valores,
                    color=self.colors.get(codigo, None),
                    linewidth=2,
                    label=f"{data['nombre']} ({codigo})")

            # Añadir punto para el último valor
            ultimo_valor = valores.iloc[-1]
            ax.scatter(df["fecha"].iloc[-1], ultimo_valor,
                       color=self.colors.get(codigo, None), s=80, zorder=5)

        # Configuración del eje X
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
        plt.xticks(rotation=45)

        # Añadir leyenda
        ax.legend(loc='best')

        # Añadir título y etiquetas
        if normalizar:
            ax.set_title(f"Comparación de {metrica.capitalize()} (Base 100)", fontsize=16)
            ax.set_ylabel(f"{metrica.capitalize()} normalizado (%)", fontsize=12)
        else:
            ax.set_title(f"Comparación de {metrica.capitalize()}", fontsize=16)
            ax.set_ylabel(f"{metrica.capitalize()}", fontsize=12)

        ax.set_xlabel("Fecha", fontsize=12)

        # Añadir cuadrícula
        ax.grid(True, linestyle='--', alpha=0.7)

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path)
            print(f"Gráfico guardado en {save_path}")
        else:
            plt.show()

        plt.close()

    def plot_dashboard(self, codigos: List[str], dias: int = 30, save_path: Optional[str] = None):
        """Genera un dashboard completo con múltiples gráficos"""
        if not codigos:
            codigos = self.get_empresa_names()

        # Determinar layout según cantidad de empresas
        n_empresas = len(codigos)
        if n_empresas <= 2:
            rows, cols = 1, n_empresas
        else:
            rows, cols = (n_empresas + 1) // 2, 2

        # Crear figura y subplots
        fig = plt.figure(figsize=(15, 5 * rows))
        fig.suptitle("Dashboard de Acciones BVL", fontsize=20, y=0.98)

        # Crear una comparativa en la parte superior
        ax_comp = plt.subplot2grid((rows + 1, cols), (0, 0), colspan=cols)

        # Obtener datos y graficar comparativa
        for codigo in codigos:
            data = self.fetch_data(codigo, dias)
            if not data:
                continue

            df = self.prepare_dataframe(data)
            if df.empty:
                continue

            # Normalizar para comparar
            normalized = (df["precio"] / df["precio"].iloc[0]) * 100

            # Graficar línea normalizada
            ax_comp.plot(df["fecha"], normalized,
                         color=self.colors.get(codigo, None),
                         linewidth=2,
                         label=f"{data['nombre']} ({codigo})")

        ax_comp.set_title("Comparación de Rendimiento (Base 100)", fontsize=14)
        ax_comp.set_ylabel("Precio Normalizado (%)", fontsize=12)
        ax_comp.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
        ax_comp.tick_params(axis='x', rotation=45)
        ax_comp.legend()
        ax_comp.grid(True, linestyle='--', alpha=0.7)

        # Graficar cada empresa individual
        for i, codigo in enumerate(codigos):
            row = (i // cols) + 1  # +1 porque la primera fila es la comparativa
            col = i % cols

            # Crear subplot
            ax = plt.subplot2grid((rows + 1, cols), (row, col))

            data = self.fetch_data(codigo, dias)
            if not data:
                ax.text(0.5, 0.5, f"No hay datos para {codigo}",
                        ha='center', va='center', fontsize=12)
                continue

            df = self.prepare_dataframe(data)
            if df.empty:
                ax.text(0.5, 0.5, f"No hay datos históricos para {codigo}",
                        ha='center', va='center', fontsize=12)
                continue

            # Graficar precio
            ax.plot(df["fecha"], df["precio"],
                    color=self.colors.get(codigo, "blue"),
                    linewidth=2)

            # Añadir punto para el valor actual
            ultimo_precio = df["precio"].iloc[-1]
            ax.scatter(df["fecha"].iloc[-1], ultimo_precio,
                       color='red', s=50, zorder=5)

            # Añadir etiqueta con último precio
            ax.annotate(f"${ultimo_precio:.2f}",
                        (df["fecha"].iloc[-1], ultimo_precio),
                        xytext=(5, 5), textcoords='offset points',
                        fontsize=10, fontweight='bold')

            # Configurar eje X
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
            ax.tick_params(axis='x', rotation=45)

            # Añadir título
            ax.set_title(f"{data['nombre']} ({codigo})", fontsize=12)

            # Añadir cuadrícula
            ax.grid(True, linestyle='--', alpha=0.7)

        plt.tight_layout()
        plt.subplots_adjust(top=0.93)

        if save_path:
            plt.savefig(save_path)
            print(f"Dashboard guardado en {save_path}")
        else:
            plt.show()

        plt.close()


def main():
    """Función principal para ejecutar el plotter desde línea de comandos"""
    parser = argparse.ArgumentParser(description="Visualizador de datos BVL")
    parser.add_argument("--url", default="http://localhost:8000", help="URL base de la API")
    parser.add_argument("--empresas", nargs="+", help="Códigos de empresas a graficar")
    parser.add_argument("--dias", type=int, default=30, help="Días de histórico")
    parser.add_argument("--tipo", choices=["precio", "comparacion", "dashboard"],
                        default="dashboard", help="Tipo de gráfico")
    parser.add_argument("--guardar", help="Ruta para guardar el gráfico")
    parser.add_argument("--metrica", default="precio",
                        help="Métrica a comparar (precio, volumen, etc.)")

    args = parser.parse_args()

    plotter = BVLStockPlotter(args.url)

    # Si no se especifican empresas, usar todas las disponibles
    if not args.empresas:
        args.empresas = plotter.get_empresa_names()
        print(f"Usando todas las empresas disponibles: {', '.join(args.empresas)}")

    if args.tipo == "precio" and len(args.empresas) == 1:
        plotter.plot_precio(args.empresas[0], args.dias, args.guardar)
    elif args.tipo == "comparacion":
        plotter.plot_comparacion(args.empresas, args.dias, args.metrica, args.guardar)
    elif args.tipo == "dashboard":
        plotter.plot_dashboard(args.empresas, args.dias, args.guardar)
    else:
        print("Opción no válida o incompatible con la cantidad de empresas")


if __name__ == "__main__":
    main()