import psycopg2

# Parámetros de conexión
host = "localhost"
port = "5433"
dbname = "northwind"
user = "postgres"
password = "XXXXXXXX"

try:
    conexion = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )
    print("✅ Conexión exitosa a PostgreSQL")

    # Crear un cursor
    cursor = conexion.cursor()

    # Ejecutar una consulta de prueba
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    print("Versión de PostgreSQL:", version)

    # Cerrar conexiones
    cursor.close()
    conexion.close()

except Exception as error:
    print("❌ Error conectando a PostgreSQL:")
    print(str(error).encode('utf-8', errors='replace').decode('utf-8'))
