import yfinance as yf
import pandas as pd
import time
import os

# Directorio base del proyecto (donde se ejecuta el script)
base_dir = os.path.dirname(os.path.abspath(__file__))  # Ruta del script actual
csv_filename = os.path.join(base_dir, "data", "bap_stock_data.csv")  # Guardar en /data

# Crear la carpeta "data" si no existe
os.makedirs(os.path.dirname(csv_filename), exist_ok=True)

# Lista de claves a extraer
columns = [
    "currentPrice", "previousClose", "open", "dayLow", "dayHigh",
    "bid", "dividendYield", "earningsGrowth", "revenueGrowth",
    "grossMargins", "ebitdaMargins", "operatingMargins", "financialCurrency",
    "returnOnAssets", "returnOnEquity", "bookValue", "priceToBook"
]

# Duración total en minutos (7 horas = 420 minutos)
total_runtime_minutes = 420
update_interval_minutes = 5
iterations = total_runtime_minutes // update_interval_minutes  # Número total de iteraciones

# Función para extraer datos y guardarlos en el CSV
def fetch_and_store_data():
    stock = yf.Ticker("BAP")
    stock_info = stock.info

    # Extraer solo las claves deseadas
    extracted_data = {key: stock_info.get(key, None) for key in columns}
    extracted_data["symbol"] = "BAP"  # Agregar símbolo de la acción
    extracted_data["timestamp"] = pd.Timestamp.now()  # Agregar tiempo de captura

    # Convertir a DataFrame
    df = pd.DataFrame([extracted_data])

    # Guardar en CSV sin borrar datos previos
    if os.path.exists(csv_filename):
        df.to_csv(csv_filename, mode='a', header=False, index=False)
    else:
        df.to_csv(csv_filename, mode='w', header=True, index=False)

    print(f"✅ Datos guardados en {csv_filename} - {extracted_data['timestamp']}")

# Ejecutar la actualización cada 5 minutos por 7 horas
if __name__ == "__main__":
    for _ in range(iterations):
        fetch_and_store_data()
        time.sleep(update_interval_minutes * 60)  # Esperar 5 minutos (300 segundos)

    print("🚀 Finalizó la recolección de datos después de 7 horas.")
