import psycopg2

DB_CONFIG = {
    "host": "98.85.189.191",
    "port": 5432,
    "database": "bvl_monitor",
    "user": "bvl_user",
    "password": "179fae82"
}

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    print(f"Conexión exitosa. Versión de PostgreSQL: {version[0]}")
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error de conexión: {str(e)}")
