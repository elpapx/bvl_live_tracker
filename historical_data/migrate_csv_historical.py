import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
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

# Lista de archivos CSV históricos a migrar
CSV_FILES = [
    {"path": "bap_historical.csv", "symbol": "BAP"},
    {"path": "brk-b_historical.csv", "symbol": "BRK-B"},
    {"path": "ilf_historical.csv", "symbol": "ILF"}
]


def get_existing_columns(conn):
    """Obtener las columnas existentes en la tabla stock_data"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
                       SELECT column_name
                       FROM information_schema.columns
                       WHERE table_name = 'stock_data'
                       ORDER BY ordinal_position
                       """)
        columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return columns
    except Exception as e:
        print(f"Error al obtener columnas existentes: {str(e)}")
        return []


def ensure_stock_data_table_exists(conn):
    """Verificar que la tabla stock_data existe o crearla si es necesario"""
    try:
        cursor = conn.cursor()

        # Verificar si la tabla existe
        cursor.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'stock_data')")
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            # Crear la tabla si no existe
            print("Creando tabla stock_data")
            cursor.execute("""
                           CREATE TABLE stock_data
                           (
                               id                   SERIAL PRIMARY KEY,
                               symbol               VARCHAR(10) NOT NULL,
                               timestamp            TIMESTAMP   NOT NULL,
                               current_price        NUMERIC(10, 2),
                               previous_close       NUMERIC(10, 2),
                               open                 NUMERIC(10, 2),
                               day_low              NUMERIC(10, 2),
                               day_high             NUMERIC(10, 2),
                               bid                  NUMERIC(10, 2),
                               dividend_yield       NUMERIC(8, 4),
                               volume               BIGINT,
                               fifty_two_week_range VARCHAR(30),
                               market_cap           BIGINT,
                               trailing_pe          NUMERIC(10, 2),
                               earnings_growth      NUMERIC(8, 4),
                               revenue_growth       NUMERIC(8, 4),
                               gross_margins        NUMERIC(8, 4),
                               ebitda_margins       NUMERIC(8, 4),
                               operating_margins    NUMERIC(8, 4),
                               financial_currency   VARCHAR(10),
                               return_on_assets     NUMERIC(8, 4),
                               return_on_equity     NUMERIC(8, 4),
                               book_value           NUMERIC(10, 2),
                               price_to_book        NUMERIC(8, 4),
                               UNIQUE (symbol, timestamp)
                           );

                           CREATE INDEX idx_stock_data_symbol ON stock_data (symbol);
                           CREATE INDEX idx_stock_data_timestamp ON stock_data (timestamp);
                           CREATE INDEX idx_stock_data_symbol_timestamp ON stock_data (symbol, timestamp);
                           """)
        else:
            # Verificar si existen los índices necesarios
            cursor.execute(
                "SELECT COUNT(*) FROM pg_indexes WHERE tablename = 'stock_data' AND indexname = 'idx_stock_data_symbol'")
            if cursor.fetchone()[0] == 0:
                print("Creando índice idx_stock_data_symbol")
                cursor.execute("CREATE INDEX idx_stock_data_symbol ON stock_data(symbol)")

            cursor.execute(
                "SELECT COUNT(*) FROM pg_indexes WHERE tablename = 'stock_data' AND indexname = 'idx_stock_data_timestamp'")
            if cursor.fetchone()[0] == 0:
                print("Creando índice idx_stock_data_timestamp")
                cursor.execute("CREATE INDEX idx_stock_data_timestamp ON stock_data(timestamp)")

            cursor.execute(
                "SELECT COUNT(*) FROM pg_indexes WHERE tablename = 'stock_data' AND indexname = 'idx_stock_data_symbol_timestamp'")
            if cursor.fetchone()[0] == 0:
                print("Creando índice idx_stock_data_symbol_timestamp")
                cursor.execute("CREATE INDEX idx_stock_data_symbol_timestamp ON stock_data(symbol, timestamp)")

        conn.commit()
        cursor.close()
    except Exception as e:
        conn.rollback()
        print(f"Error al verificar/crear tabla: {str(e)}")
        raise


def migrate_historical_csv_to_postgres(csv_file, symbol, existing_columns):
    """Migrar un archivo CSV histórico a PostgreSQL"""
    print(f"Procesando archivo histórico: {csv_file}")

    try:
        # Leer el archivo CSV
        df = pd.read_csv(csv_file)
        print(f"Leídos {len(df)} registros desde {csv_file}")

        # Convertir la columna Date a timestamp
        df['timestamp'] = pd.to_datetime(df['Date'])

        # Mapeo de columnas de CSV histórico a columnas de base de datos
        # Aquí mapeamos "Open" a "current_price" como solicitaste
        column_mapping = {
            'Open': 'current_price',
            'Close': 'previous_close',
            'High': 'day_high',
            'Low': 'day_low',
            'Volume': 'volume',
            'Dividends': 'dividend_yield'
        }

        # Aplicar el mapeo de columnas
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns and new_col in existing_columns:
                df[new_col] = df[old_col]

        # Añadir columna symbol
        df['symbol'] = symbol

        # Filtrar solo las columnas que existen en la tabla de la base de datos
        filtered_columns = ['symbol', 'timestamp']
        for col in column_mapping.values():
            if col in existing_columns and col not in filtered_columns:
                filtered_columns.append(col)

        # Crear un DataFrame con solo las columnas existentes en la BD
        filtered_df = df[filtered_columns].copy()

        # Conectar a PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Preparar datos para inserción
        data = []
        for _, row in filtered_df.iterrows():
            # Convertir NaN a None para PostgreSQL
            record = []
            for col in filtered_columns:
                if pd.isna(row.get(col)):
                    record.append(None)
                else:
                    record.append(row.get(col))
            data.append(tuple(record))

        # Generar la consulta INSERT
        columns_str = ", ".join(filtered_columns)

        # Crear la cláusula de actualización para ON CONFLICT
        update_parts = []
        for col in filtered_columns:
            if col not in ["symbol", "timestamp"]:  # No actualizar las claves
                update_parts.append(f"{col} = EXCLUDED.{col}")

        if update_parts:
            on_conflict_clause = f"ON CONFLICT (symbol, timestamp) DO UPDATE SET {', '.join(update_parts)}"
        else:
            on_conflict_clause = "ON CONFLICT (symbol, timestamp) DO NOTHING"

        insert_query = f"""
                       INSERT INTO stock_data ({columns_str})
                       VALUES %s 
                       {on_conflict_clause}
                       """

        # Usar execute_values para inserción en bloque más eficiente
        execute_values(cursor, insert_query, data)
        conn.commit()

        print(f"Migración exitosa: {len(data)} registros insertados/actualizados para {symbol}")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"Error al migrar {csv_file}: {str(e)}")
        return False


def main():
    """Función principal"""
    print(f"Iniciando migración de datos históricos: {datetime.now()}")

    # Conectar para verificar la tabla y sus columnas
    conn = psycopg2.connect(**DB_CONFIG)
    ensure_stock_data_table_exists(conn)
    existing_columns = get_existing_columns(conn)
    conn.close()

    print(f"Columnas existentes en la tabla: {existing_columns}")

    successful = 0
    failed = 0

    for file_info in CSV_FILES:
        csv_path = file_info["path"]
        symbol = file_info["symbol"]

        if os.path.exists(csv_path):
            result = migrate_historical_csv_to_postgres(csv_path, symbol, existing_columns)
            if result:
                successful += 1
            else:
                failed += 1
        else:
            print(f"Archivo no encontrado: {csv_path}")
            failed += 1

    print(f"Migración de datos históricos completada: {datetime.now()}")
    print(f"Total exitosos: {successful}, Total fallidos: {failed}")


if __name__ == "__main__":
    main()