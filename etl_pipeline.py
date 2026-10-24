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
        check_col = conn.execute(text("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'dim_cliente' AND COLUMN_NAME = 'rut'
        """)).fetchone()

        if not check_col:
            print("-> Agregando columna 'rut' a la tabla dbo.dim_cliente en el DW...")
            conn.execute(text("ALTER TABLE dbo.dim_cliente ADD rut VARCHAR(12) NULL;"))
            print("✓ Columna 'rut' agregada exitosamente al DW.")
        else:
            print("✓ La columna 'rut' ya existe en dim_cliente del DW.")

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