import yfinance as yf

# Descargar datos históricos de BRK-B para 2025
brk_b = yf.Ticker("ILF")
datos_2025 = brk_b.history(start="2025-01-01", end="2025-12-31")

# Guardar en CSV
datos_2025.to_csv("ilf_historical.csv")
print("Datos descargados y guardados en 'brk-b_historical.csv'")