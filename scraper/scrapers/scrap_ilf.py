import yfinance as yf
import pandas as pd
import time
import os

# --- Configuración de rutas ABSOLUTAS para evitar errores ---
current_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(current_dir, "data")  # Carpeta "data" en la MISMA ubicación que el script
csv_filename = os.path.join(data_dir, "ilf_etf_data.csv")

# Crear carpeta "data" si no existe
os.makedirs(data_dir, exist_ok=True)

# Columnas a extraer
columns = [
    "previousClose", "open", "dayLow", "dayHigh",
    "volume", "regularMarketVolume", "averageVolume", "averageVolume10days",
    "bid", "ask", "dividendYield"
]


def fetch_and_store_data():
    try:
        etf = yf.Ticker("ILF")

        # 1. Precio actual (método confiable)
        current_price = etf.history(period="1d")["Close"].iloc[-1]

        # 2. Otras métricas
        etf_info = etf.info
        extracted_data = {key: etf_info.get(key, "N/A") for key in columns}  # "N/A" si no existe

        # 3. Agregar datos adicionales
        extracted_data.update({
            "currentPrice": current_price,
            "symbol": "ILF",
            "timestamp": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")  # Formato legible
        })

        # 4. Guardar en CSV
        df = pd.DataFrame([extracted_data])
        if not os.path.exists(csv_filename):
            df.to_csv(csv_filename, mode="w", header=True, index=False)
        else:
            df.to_csv(csv_filename, mode="a", header=False, index=False)

        print(f"✅ Datos guardados: {current_price:.2f} | Ruta: {csv_filename}")

    except Exception as e:
        print(f"❌ Error grave: {str(e)}")


if __name__ == "__main__":
    total_minutes = 1  # Solo para prueba (cambia a 420 después)
    interval_minutes = 1
    iterations = total_minutes // interval_minutes

    print("Iniciando recolección...")
    for _ in range(iterations):
        fetch_and_store_data()
        time.sleep(interval_minutes * 60)

    print("🚀 Proceso completado. Verifica el archivo CSV.")