#Ejercicio 1

import psycopg2

# Parámetros de conexión
host = "localhost"
port = "5433"
dbname = "northwind"
user = "postgres"
password = "Cacafuti01*0123"

try:
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )
    print("✅ Conexión exitosa a PostgreSQL\n")

    cursor = conn.cursor()

    # Versión PostgreSQL
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    print("Versión de PostgreSQL:", version[0], "\n")

    esquema = 'public'

    # TABLAS
    cursor.execute(f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{esquema}'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """)
    tables = cursor.fetchall()
    print(f"📋 Tablas en el esquema '{esquema}':\n")
    for table in tables:
        print(table[0])
    print()

    # RELACIONES
    cursor.execute(f"""
        SELECT
            tc.table_name AS tabla_origen,
            kcu.column_name AS columna_origen,
            ccu.table_name AS tabla_referenciada,
            ccu.column_name AS columna_referenciada
        FROM
            information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = '{esquema}'
        ORDER BY tabla_origen, columna_origen;
    """)
    relations = cursor.fetchall()
    print(f"🔗 Relaciones (claves foráneas) en el esquema '{esquema}':\n")
    for r in relations:
        print(f"{r[0]}.{r[1]} -> {r[2]}.{r[3]}")
    print()


#Ejercicio 2

       # 1. Empleados que han gestionado al menos un pedido (sin duplicados)
    cursor.execute("""
        SELECT DISTINCT
            e.employee_id,
            e.first_name,
            e.last_name,
            e.city,
            e.country
        FROM employees e
        JOIN orders o ON e.employee_id = o.employee_id
        ORDER BY e.employee_id;
    """)
    empleados = cursor.fetchall()
    print("👨‍💼 Empleados que han gestionado al menos un pedido:\n")
    for emp in empleados:
        print(f"Empleado ID: {emp[0]}, Nombre: {emp[1]} {emp[2]}, Ciudad: {emp[3]}, País: {emp[4]}")
    print()

    # 2. Productos
    cursor.execute("""
        SELECT product_id, supplier_id, product_name, unit_price, units_in_stock, units_on_order, discontinued
        FROM products
        ORDER BY product_id;
    """)
    productos = cursor.fetchall()
    print("📦 Productos disponibles:\n")
    for p in productos:
        print(f"ID: {p[0]}, Proveedor: {p[1]}, Nombre: {p[2]}, Precio: {p[3]}, Stock: {p[4]}, Pedidas: {p[5]}, Descontinuado: {bool(p[6])}")
    print()

    # 3. Productos descontinuados
    cursor.execute("""
        SELECT product_name, units_in_stock
        FROM products
        WHERE discontinued = 1;
    """)
    desc = cursor.fetchall()
    print("🛑 Productos descontinuados:\n")
    for d in desc:
        print(f"Producto: {d[0]}, Stock restante: {d[1]}")
    print()

    # 4. Proveedores
    cursor.execute("""
        SELECT supplier_id, company_name, city, country
        FROM suppliers
        ORDER BY supplier_id;
    """)
    proveedores = cursor.fetchall()
    print("🏢 Proveedores:\n")
    for prov in proveedores:
        print(f"ID: {prov[0]}, Compañía: {prov[1]}, Ciudad: {prov[2]}, País: {prov[3]}")
    print()

    # 5. Pedidos
    cursor.execute("""
        SELECT order_id, customer_id, ship_via, order_date, required_date, shipped_date
        FROM orders
        ORDER BY order_id;
    """)
    pedidos = cursor.fetchall()
    print("📑 Pedidos:\n")
    for ped in pedidos:
        print(f"Pedido #{ped[0]}, Cliente: {ped[1]}, Transportista: {ped[2]}, Pedido: {ped[3]}, Requerido: {ped[4]}, Enviado: {ped[5]}")
    print()

    # 6. Número de pedidos
    cursor.execute("SELECT COUNT(*) FROM orders;")
    total_pedidos = cursor.fetchone()[0]
    print(f"📦 Total de pedidos: {total_pedidos}\n")

    # 7. Clientes
    cursor.execute("""
        SELECT customer_id, company_name, city, country
        FROM customers
        ORDER BY customer_id;
    """)
    clientes = cursor.fetchall()
    print("👥 Clientes:\n")
    for c in clientes:
        print(f"ID: {c[0]}, Compañía: {c[1]}, Ciudad: {c[2]}, País: {c[3]}")
    print()

    # 8. Transportistas
    cursor.execute("""
        SELECT shipper_id, company_name
        FROM shippers
        ORDER BY shipper_id;
    """)
    shippers = cursor.fetchall()
    print("🚚 Empresas de transporte:\n")
    for s in shippers:
        print(f"ID: {s[0]}, Compañía: {s[1]}")
    print()

    # 9. Relaciones de reporte entre empleados
    cursor.execute("""
        SELECT 
            e.employee_id AS empleado_id, 
            e.first_name || ' ' || e.last_name AS empleado,
            m.employee_id AS jefe_id,
            m.first_name || ' ' || m.last_name AS jefe
        FROM employees e
        LEFT JOIN employees m ON e.reports_to = m.employee_id
        ORDER BY e.employee_id;
    """)
    reportes = cursor.fetchall()
    print("📊 Relaciones de reporte entre empleados:\n")
    for r in reportes:
        jefe = r[3] if r[3] else "Sin jefe directo"
        print(f"Empleado: {r[1]} (ID: {r[0]}) → Reporta a: {jefe}")
    print()

except Exception as error:
    print("❌ Error conectando o consultando en PostgreSQL:", error)


#Ejercicio 3




finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()