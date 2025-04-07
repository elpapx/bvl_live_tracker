import yfinance as yf

# Descargar datos históricos de BRK-B para 2025
brk_b = yf.Ticker("BRK-B")
datos_2025 = brk_b.history(start="2025-01-01", end="2025-04-05")

# Guardar en CSV
datos_2025.to_csv("brk-b_historical.csv")
print("Datos descargados y guardados en 'ilf_historical.csv'")