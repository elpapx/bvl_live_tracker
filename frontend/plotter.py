from plot import BVLStockPlotter

# Create plotter
plotter = BVLStockPlotter(base_url="http://localhost:8000")

# Get available companies
companies = plotter.get_empresa_names()
print(f"Available companies: {companies}")

# Create a dashboard
plotter.plot_dashboard(companies, dias=1)

# Create price chart for BAP
plotter.plot_precio("BAP", dias=1, save_path="bap_chart.png")

# Compare BAP and ILF
plotter.plot_comparacion(["BAP", "ILF"], dias=1, metrica="precio")