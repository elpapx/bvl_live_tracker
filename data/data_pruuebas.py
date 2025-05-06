import psycopg2
import pandas as pd
from tabulate import tabulate
import matplotlib.pyplot as plt
import os
from datetime import datetime

# Configuración de la conexión a PostgreSQL
DB_CONFIG = {
    "host": "98.85.189.191",
    "port": 5432,
    "database": "bvl_monitor",
    "user": "bvl_user",
    "password": "179fae82"
}


def connect_to_db():
    """Conectar a la base de datos PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Conexión establecida con éxito a la base de datos PostgreSQL.")
        return conn
    except Exception as e:
        print(f"Error al conectar a la base de datos: {str(e)}")
        return None





def get_date_distribution_detailed(conn, start_date="2025-01-01", end_date="2025-04-30"):
    """Obtener distribución detallada de registros por fecha en el rango especificado"""
    try:
        cursor = conn.cursor()

        query = """
                SELECT
                    DATE (timestamp) as date, symbol, COUNT (*) as count
                FROM stock_data
                WHERE timestamp >= %s \
                  AND timestamp <= %s
                GROUP BY DATE (timestamp), symbol
                ORDER BY DATE (timestamp), symbol \
                """
        cursor.execute(query, (start_date, end_date))
        results = cursor.fetchall()
        cursor.close()

        # Convertir a DataFrame
        df = pd.DataFrame(results, columns=['date', 'symbol', 'count'])

        # Crear tabla dinámica para mejor visualización
        pivot_df = df.pivot_table(
            index='date',
            columns='symbol',
            values='count',
            fill_value=0
        ).reset_index()

        return pivot_df

    except Exception as e:
        print(f"Error al obtener distribución detallada de fechas: {str(e)}")
        return None


def check_data_completeness(conn, start_date="2025-01-01", end_date="2025-04-30"):
    """Verificar la completitud de los datos en el rango especificado"""
    try:
        # Construir rango de fechas esperado
        date_range = pd.date_range(start=start_date, end=end_date)
        expected_dates = set(date_range.date)

        cursor = conn.cursor()

        # Obtener fechas reales en la base de datos
        query = """
                SELECT DISTINCT DATE (timestamp) as date
                FROM stock_data
                WHERE timestamp >= %s \
                  AND timestamp <= %s
                ORDER BY date \
                """
        cursor.execute(query, (start_date, end_date))
        actual_dates = set([row[0] for row in cursor.fetchall()])

        cursor.close()

        # Encontrar fechas faltantes
        missing_dates = expected_dates - actual_dates

        return {
            "expected_date_count": len(expected_dates),
            "actual_date_count": len(actual_dates),
            "missing_dates": sorted(list(missing_dates))
        }

    except Exception as e:
        print(f"Error al verificar completitud de datos: {str(e)}")
        return None


def get_sample_data_by_month(conn, month, year=2025, limit=5):
    """Obtener una muestra de datos para un mes específico"""
    try:
        cursor = conn.cursor()

        # Construir rango de fechas para el mes
        if month == 12:
            next_month = 1
            next_year = year + 1
        else:
            next_month = month + 1
            next_year = year

        start_date = f"{year}-{month:02d}-01"
        end_date = f"{next_year}-{next_month:02d}-01"

        query = f"""
            SELECT timestamp, symbol, current_price, previous_close, day_low, day_high, volume
            FROM stock_data
            WHERE timestamp >= %s AND timestamp < %s
            ORDER BY timestamp DESC
            LIMIT {limit}
        """
        cursor.execute(query, (start_date, end_date))

        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()

        cursor.close()

        # Convertir resultados a DataFrame
        df = pd.DataFrame(results, columns=columns)
        return df

    except Exception as e:
        print(f"Error al obtener datos de muestra para el mes {month}: {str(e)}")
        return None


def generate_monthly_price_chart(conn, symbols, start_date="2025-01-01", end_date="2025-04-30", save_dir="charts"):
    """Generar gráfico de precios mensuales para los símbolos especificados"""
    # Crear directorio para gráficos si no existe
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    try:
        cursor = conn.cursor()

        plt.figure(figsize=(12, 8))

        for symbol in symbols:
            # Obtener datos diarios (un registro por día para evitar ruido)
            query = """
                    WITH daily_data AS (SELECT
                        DATE (timestamp) as date \
                       , symbol \
                       , AVG (current_price) as avg_price
                    FROM stock_data
                    WHERE symbol = %s \
                      AND timestamp >= %s \
                      AND timestamp <= %s
                    GROUP BY DATE (timestamp), symbol
                        )
                    SELECT date, avg_price
                    FROM daily_data
                    ORDER BY date \
                    """
            cursor.execute(query, (symbol, start_date, end_date))
            results = cursor.fetchall()

            if results:
                # Convertir a DataFrame
                df = pd.DataFrame(results, columns=['date', 'price'])
                plt.plot(df['date'], df['price'], label=symbol)

        cursor.close()

        plt.title(f'Precios Diarios (Enero - Abril 2025)')
        plt.xlabel('Fecha')
        plt.ylabel('Precio')
        plt.grid(True)
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()

        # Guardar gráfico
        chart_path = os.path.join(save_dir, f'price_history_jan_apr_2025.png')
        plt.savefig(chart_path)
        print(f"Gráfico guardado en: {chart_path}")

        # También mostrar el gráfico
        plt.show()

    except Exception as e:
        print(f"Error al generar gráfico de precios mensuales: {str(e)}")


def analyze_business_days(conn, start_date="2025-01-01", end_date="2025-04-30"):
    """Analizar la cobertura de días hábiles en el rango de fechas"""
    try:
        # Obtener las fechas reales en la base de datos
        cursor = conn.cursor()
        query = """
                SELECT DISTINCT DATE (timestamp) as date
                FROM stock_data
                WHERE timestamp >= %s \
                  AND timestamp <= %s
                ORDER BY date \
                """
        cursor.execute(query, (start_date, end_date))
        actual_dates = set([row[0] for row in cursor.fetchall()])
        cursor.close()

        # Crear rango de fechas completo
        date_range = pd.date_range(start=start_date, end=end_date)
        all_dates = set(date_range.date)

        # Identificar días de fin de semana (0=lunes, 6=domingo)
        weekend_dates = set([d for d in date_range.date if d.weekday() >= 5])

        # Calcular días hábiles esperados
        business_days = all_dates - weekend_dates

        # Calcular días hábiles con datos
        business_days_with_data = business_days.intersection(actual_dates)

        # Calcular días hábiles sin datos
        business_days_without_data = business_days - business_days_with_data

        return {
            "total_days": len(all_dates),
            "weekend_days": len(weekend_dates),
            "business_days": len(business_days),
            "business_days_with_data": len(business_days_with_data),
            "business_days_without_data": len(business_days_without_data),
            "coverage_percentage": len(business_days_with_data) / len(business_days) * 100 if business_days else 0,
            "missing_business_days": sorted(list(business_days_without_data))
        }

    except Exception as e:
        print(f"Error al analizar días hábiles: {str(e)}")
        return None


def main():
    print("=== Verificación de Datos PostgreSQL: Enero a Abril 2025 ===")

    # Fechas a analizar
    start_date = "2025-01-01"
    end_date = "2025-04-30"

    # Conectar a la base de datos
    conn = connect_to_db()
    if not conn:
        return

    try:
        # Obtener y mostrar información para el rango de fechas
        print(f"\n=== Información de Datos entre {start_date} y {end_date} ===")
        date_range_data = get_date_range_data(conn, start_date, end_date)

        if date_range_data:
            print(f"Total de registros en el período: {date_range_data['total_count']}")

            print("\nRegistros por símbolo:")
            for symbol, count in date_range_data['symbol_counts']:
                print(f"  {symbol}: {count} registros")

            print("\nRegistros por mes:")
            for year, month, count in date_range_data['month_counts']:
                month_name = datetime(int(year), int(month), 1).strftime('%B')
                print(f"  {month_name} {int(year)}: {count} registros")

        # Verificar completitud de datos
        print("\n=== Análisis de Completitud de Datos ===")
        completeness = check_data_completeness(conn, start_date, end_date)

        if completeness:
            print(f"Días esperados en el período: {completeness['expected_date_count']}")
            print(f"Días con datos: {completeness['actual_date_count']}")
            print(f"Días sin datos: {completeness['expected_date_count'] - completeness['actual_date_count']}")

            if completeness['missing_dates']:
                print("\nPrimeras 10 fechas sin datos:")
                for date in sorted(completeness['missing_dates'])[:10]:
                    print(f"  {date}")
                if len(completeness['missing_dates']) > 10:
                    print(f"  ... y {len(completeness['missing_dates']) - 10} fechas más.")

        # Analizar días hábiles
        print("\n=== Análisis de Días Hábiles ===")
        business_days_analysis = analyze_business_days(conn, start_date, end_date)

        if business_days_analysis:
            print(f"Total de días en el período: {business_days_analysis['total_days']}")
            print(f"Días de fin de semana: {business_days_analysis['weekend_days']}")
            print(f"Días hábiles: {business_days_analysis['business_days']}")
            print(f"Días hábiles con datos: {business_days_analysis['business_days_with_data']}")
            print(f"Días hábiles sin datos: {business_days_analysis['business_days_without_data']}")
            print(f"Cobertura de días hábiles: {business_days_analysis['coverage_percentage']:.2f}%")

            if business_days_analysis['missing_business_days']:
                print("\nPrimeros 10 días hábiles sin datos:")
                for date in sorted(business_days_analysis['missing_business_days'])[:10]:
                    print(f"  {date} ({date.strftime('%A')})")
                if len(business_days_analysis['missing_business_days']) > 10:
                    print(f"  ... y {len(business_days_analysis['missing_business_days']) - 10} días más.")

        # Mostrar distribución detallada de registros por fecha
        print("\n=== Distribución Detallada de Registros por Fecha ===")
        date_dist = get_date_distribution_detailed(conn, start_date, end_date)
        if date_dist is not None and not date_dist.empty:
            print(tabulate(date_dist.head(10), headers='keys', tablefmt='psql', showindex=False))
            print(f"... mostrando las primeras 10 fechas de un total de {len(date_dist)} días.")

        # Mostrar ejemplos de datos para cada mes
        months = [1, 2, 3, 4]  # Enero a Abril

        for month in months:
            month_name = datetime(2025, month, 1).strftime('%B')
            print(f"\n=== Muestra de Datos: {month_name} 2025 ===")
            month_data = get_sample_data_by_month(conn, month, 2025, 3)

            if month_data is not None and not month_data.empty:
                print(tabulate(month_data, headers='keys', tablefmt='psql', showindex=False))
            else:
                print(f"No hay datos para {month_name} 2025")

        # Obtener símbolos disponibles
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT symbol FROM stock_data WHERE timestamp >= %s AND timestamp <= %s ORDER BY symbol",
            (start_date, end_date))
        symbols = [row[0] for row in cursor.fetchall()]
        cursor.close()

        # Generar gráfico para los símbolos disponibles
        if symbols:
            print("\n=== Generando Gráfico de Precios (Enero-Abril 2025) ===")
            generate_monthly_price_chart(conn, symbols, start_date, end_date)

    except Exception as e:
        print(f"Error durante la ejecución: {str(e)}")

    finally:
        # Cerrar conexión
        if conn:
            conn.close()
            print("\nConexión a la base de datos cerrada.")


if __name__ == "__main__":
    main()