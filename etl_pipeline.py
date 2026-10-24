import os
import re
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 1. CONFIGURACIÓN Y VARIABLES DE ENTORNO
load_dotenv()

SERVER = os.getenv("DB_SERVER", "localhost")
DRIVER = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
DATABASE_ORIGEN = os.getenv("DB_ORIGEN", "DataRetail360_Origen")
DATABASE_DW = os.getenv("DB_DW", "DataRetail360_DW")

db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASSWORD")

if db_user and db_pass:
    conn_str_origen = f"mssql+pyodbc://{db_user}:{db_pass}@{SERVER}/{DATABASE_ORIGEN}?driver={DRIVER}"
    conn_str_dw = f"mssql+pyodbc://{db_user}:{db_pass}@{SERVER}/{DATABASE_DW}?driver={DRIVER}"
else:
    conn_str_origen = f"mssql+pyodbc://@{SERVER}/{DATABASE_ORIGEN}?driver={DRIVER}&trusted_connection=yes"
    conn_str_dw = f"mssql+pyodbc://@{SERVER}/{DATABASE_DW}?driver={DRIVER}&trusted_connection=yes"

engine_origen = create_engine(conn_str_origen)
engine_dw = create_engine(conn_str_dw)

PATH_FUENTES = r"C:\ETL\Fuentes"
PATH_BRONZE = r"C:\ETL\Bronze"
PATH_SILVER = r"C:\ETL\Silver"
PATH_GOLD = r"C:\ETL\Gold"

# 2. PREPARACIÓN DEL DATA WAREHOUSE
def preparar_estructura_dw():
    print("Verificando estructura en DataRetail360_DW...")
    with engine_dw.begin() as conn:
        # Verificar y agregar RUT al dim_cliente
        check_rut = conn.execute(text("""
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'dim_cliente' AND COLUMN_NAME = 'rut'
        """)).fetchone()
        if not check_rut:
            print("-> Agregando columna 'rut' a dim_cliente...")
            conn.execute(text("ALTER TABLE dbo.dim_cliente ADD rut VARCHAR(12) NULL;"))

        # Verificar y agregar cantidad_ventas para SCD2 (dim_cliente)
        check_ventas = conn.execute(text("""
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'dim_cliente' AND COLUMN_NAME = 'cantidad_ventas'
        """)).fetchone()
        if not check_ventas:
            print("-> Agregando columna 'cantidad_ventas' a dim_cliente (Requerimiento SCD2)...")
            conn.execute(text("ALTER TABLE dbo.dim_cliente ADD cantidad_ventas INT DEFAULT 0;"))
            
        print("✓ Estructura de dimensiones verificada y lista.")

# 3. CAPA BRONCE (EXTRACCIÓN RAW)
def ejecutar_extraccion_bronze():
    print("\n--- INICIANDO EXTRACCIÓN Y CAPA BRONCE ---")
    os.makedirs(PATH_BRONZE, exist_ok=True)

    print("Extrayendo tablas desde DataRetail360_Origen...")
    df_orig_fecha = pd.read_sql("SELECT * FROM dbo.dim_fecha", engine_origen)
    df_orig_producto = pd.read_sql("SELECT * FROM dbo.dim_producto", engine_origen)
    df_orig_sucursal = pd.read_sql("SELECT * FROM dbo.dim_sucursal", engine_origen)
    df_orig_canal = pd.read_sql("SELECT * FROM dbo.dim_canal", engine_origen)
    df_orig_cliente = pd.read_sql("SELECT * FROM dbo.dim_cliente", engine_origen)
    df_orig_fact = pd.read_sql("SELECT * FROM dbo.fact_ventas", engine_origen)

    path_json = os.path.join(PATH_FUENTES, "clientes.json")
    print(f"Leyendo archivo JSON: {path_json}")
    df_json_clientes = pd.read_json(path_json)

    path_csv = os.path.join(PATH_FUENTES, "ventas.csv")
    print(f"Leyendo archivo CSV: {path_csv}")
    df_csv_ventas = pd.read_csv(path_csv)

    print("Guardando datos crudos en la Capa Bronce...")
    df_orig_fecha.to_parquet(os.path.join(PATH_BRONZE, "bronze_dim_fecha.parquet"), index=False)
    df_orig_producto.to_parquet(os.path.join(PATH_BRONZE, "bronze_dim_producto.parquet"), index=False)
    df_orig_sucursal.to_parquet(os.path.join(PATH_BRONZE, "bronze_dim_sucursal.parquet"), index=False)
    df_orig_canal.to_parquet(os.path.join(PATH_BRONZE, "bronze_dim_canal.parquet"), index=False)
    df_orig_cliente.to_parquet(os.path.join(PATH_BRONZE, "bronze_dim_cliente.parquet"), index=False)
    df_orig_fact.to_parquet(os.path.join(PATH_BRONZE, "bronze_fact_ventas.parquet"), index=False)
    df_json_clientes.to_parquet(os.path.join(PATH_BRONZE, "bronze_clientes_json.parquet"), index=False)
    df_csv_ventas.to_parquet(os.path.join(PATH_BRONZE, "bronze_ventas_csv.parquet"), index=False)

    return {
        "fecha": df_orig_fecha,
        "producto": df_orig_producto,
        "sucursal": df_orig_sucursal,
        "canal": df_orig_canal,
        "cliente_origen": df_orig_cliente,
        "fact_origen": df_orig_fact,
        "clientes_json": df_json_clientes,
        "ventas_csv": df_csv_ventas
    }

# 4. CAPA PLATA (TRANSFORMACION Y CALIDAD)
def ejecutar_capa_silver(datos_brutos):
    print("\n--- INICIANDO CAPA PLATA Y TRANSFORMACIONES CRÍTICAS ---")
    os.makedirs(PATH_SILVER, exist_ok=True)

    df_cliente_orig = datos_brutos["cliente_origen"].copy()
    df_json_clientes = datos_brutos["clientes_json"].copy()
    df_producto_orig = datos_brutos["producto"].copy()
    df_canal_orig = datos_brutos["canal"].copy()
    df_fecha_orig = datos_brutos["fecha"].copy()
    df_sucursal_orig = datos_brutos["sucursal"].copy()
    df_fact_orig = datos_brutos["fact_origen"].copy()

    lista_rechazos = []

    # 1. DEDUPLICACIÓN DE CLIENTES
    print("Deduplicando clientes por RUT...")
    df_clientes_totales = pd.concat([df_cliente_orig, df_json_clientes], ignore_index=True)
    df_clientes_totales["rut"] = df_clientes_totales["rut"].astype(str).str.strip().str.upper()
    df_clientes_silver = df_clientes_totales.drop_duplicates(subset=["rut"], keep="last").copy()

    # 2. VALIDACIÓN DE FECHAS
    print("Validando fechas...")
    registros_fecha_limpios = []
    fechas_validas = set()
    for _, row in df_fecha_orig.iterrows():
        fecha_str = str(row.get("fecha", "")).strip()
        fecha_key = row.get("fecha_key")
        try:
            datetime.strptime(fecha_str, "%Y-%m-%d")
            registros_fecha_limpios.append(row)
            fechas_validas.add(fecha_key)
        except ValueError:
            lista_rechazos.append({
                "origen": "dim_fecha", "campo_erroneo": "fecha",
                "valor_original": fecha_str, "motivo_rechazo": "Formato inválido o fecha inexistente"
            })
    df_fecha_silver = pd.DataFrame(registros_fecha_limpios)

    # 3. HOMOLOGACIÓN DE SKUS
    print("Validando y homologando SKUs...")
    registros_prod_limpios = []
    skus_validos = set()
    for _, row in df_producto_orig.iterrows():
        sku_val = str(row.get("sku", "")).strip().upper()
        row["sku"] = sku_val
        if re.match(r"^[A-Z]{2,3}-\d{3}$", sku_val):
            registros_prod_limpios.append(row)
            skus_validos.add(row.get("producto_key"))
        else:
            lista_rechazos.append({
                "origen": "dim_producto", "campo_erroneo": "sku",
                "valor_original": sku_val, "motivo_rechazo": "SKU no homologado (AA-000)"
            })
    df_producto_silver = pd.DataFrame(registros_prod_limpios)

    # 4. VALIDACIÓN DE SUCURSALES
    print("Validando sucursales...")
    registros_suc_limpios = []
    sucursales_validas = set()
    for _, row in df_sucursal_orig.iterrows():
        suc = str(row.get("sucursal", "")).strip().upper()
        if suc in ["SIN_ASIGNAR", "SIN ASIGNAR", ""]:
            lista_rechazos.append({
                "origen": "dim_sucursal", "campo_erroneo": "sucursal",
                "valor_original": suc, "motivo_rechazo": "Tienda Nula o Sin Asignar"
            })
        else:
            registros_suc_limpios.append(row)
            sucursales_validas.add(row.get("sucursal_key"))
    df_sucursal_silver = pd.DataFrame(registros_suc_limpios)

    # 5. VALIDACIÓN DE CANALES
    print("Validando canales...")
    df_canal_orig["canal"] = df_canal_orig["canal"].astype(str).str.strip()
    df_canal_orig["codigo_canal"] = df_canal_orig["codigo_canal"].astype(str).str.strip()
    mask_canal_invalido = df_canal_orig["canal"].isin(["", "NAN", "NONE", "nan"]) | df_canal_orig["canal"].isnull() | df_canal_orig["codigo_canal"].isnull()
    
    for _, row in df_canal_orig[mask_canal_invalido].iterrows():
        lista_rechazos.append({
            "origen": "dim_canal", "campo_erroneo": "canal",
            "valor_original": str(row.get("canal")), "motivo_rechazo": "Canal nulo o vacío"
        })
    df_canal_silver = df_canal_orig[~mask_canal_invalido].copy()
    canales_validos = set(df_canal_silver["canal_key"])

    # 6. VALIDACIÓN DE HECHOS
    print("Validando tabla de hechos...")
    registros_fact_limpios = []
    for _, row in df_fact_orig.iterrows():
        num_boleta = row.get("num_boleta")
        monto_bruto = row.get("monto_bruto", 0)
        es_devolucion = row.get("es_devolucion", 0)
        
        if monto_bruto < 0 and es_devolucion == 0:
            lista_rechazos.append({"origen": "fact_ventas", "campo_erroneo": "monto_bruto", "valor_original": str(monto_bruto), "motivo_rechazo": f"Monto negativo en {num_boleta}"})
            continue
        if row.get("fecha_key") not in fechas_validas:
            lista_rechazos.append({"origen": "fact_ventas", "campo_erroneo": "fecha_key", "valor_original": str(row.get("fecha_key")), "motivo_rechazo": f"Fecha inválida en {num_boleta}"})
            continue
        if row.get("producto_key") not in skus_validos:
            lista_rechazos.append({"origen": "fact_ventas", "campo_erroneo": "producto_key", "valor_original": str(row.get("producto_key")), "motivo_rechazo": f"SKU inválido en {num_boleta}"})
            continue
        if row.get("canal_key") not in canales_validos:
            lista_rechazos.append({"origen": "fact_ventas", "campo_erroneo": "canal_key", "valor_original": str(row.get("canal_key")), "motivo_rechazo": f"Canal nulo en {num_boleta}"})
            continue
        if row.get("sucursal_key") not in sucursales_validas:
            lista_rechazos.append({"origen": "fact_ventas", "campo_erroneo": "sucursal_key", "valor_original": str(row.get("sucursal_key")), "motivo_rechazo": f"Tienda nula en {num_boleta}"})
            continue
            
        registros_fact_limpios.append(row)

    df_fact_silver = pd.DataFrame(registros_fact_limpios)
    df_rechazos = pd.DataFrame(lista_rechazos)

    print(f"✓ Transacciones limpias: {len(df_fact_silver)} | Enviados a Rechazos: {len(df_rechazos)}")

    # Guardar en parquet
    df_clientes_silver.to_parquet(os.path.join(PATH_SILVER, "silver_dim_cliente.parquet"), index=False)
    df_producto_silver.to_parquet(os.path.join(PATH_SILVER, "silver_dim_producto.parquet"), index=False)
    df_canal_silver.to_parquet(os.path.join(PATH_SILVER, "silver_dim_canal.parquet"), index=False)
    df_fecha_silver.to_parquet(os.path.join(PATH_SILVER, "silver_dim_fecha.parquet"), index=False)
    df_sucursal_silver.to_parquet(os.path.join(PATH_SILVER, "silver_dim_sucursal.parquet"), index=False)
    df_fact_silver.to_parquet(os.path.join(PATH_SILVER, "silver_fact_ventas.parquet"), index=False)
    if not df_rechazos.empty:
        df_rechazos.to_parquet(os.path.join(PATH_SILVER, "silver_tabla_rechazos.parquet"), index=False)

    return {
        "cliente": df_clientes_silver,
        "producto": df_producto_silver,
        "canal": df_canal_silver,
        "fecha": df_fecha_silver,
        "sucursal": df_sucursal_silver,
        "fact_ventas": df_fact_silver,
        "rechazos": df_rechazos,
        "orig_prod_map": dict(zip(df_producto_orig["producto_key"], df_producto_orig["sku"])),
        "orig_suc_map": dict(zip(df_sucursal_orig["sucursal_key"], df_sucursal_orig["codigo_sucursal"])),
        "orig_can_map": dict(zip(df_canal_orig["canal_key"], df_canal_orig["codigo_canal"])),
        "orig_cli_map": dict(zip(df_cliente_orig["cliente_key"], df_cliente_orig["cliente_id"]))
    }