import os
import re
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 1. CONFIGURACIÓN Y VARIABLES DE ENTORNO
load_dotenv()

SERVER = os.getenv("DB_SERVER")
DRIVER = os.getenv("DB_DRIVER")
DATABASE_ORIGEN = os.getenv("DB_ORIGEN")
DATABASE_DW = os.getenv("DB_DW")

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
        # Verificar y agregar RUT asegurando el esquema dbo
        check_rut = conn.execute(text("""
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'dim_cliente' AND COLUMN_NAME = 'rut'
        """)).fetchone()
        
        if not check_rut:
            print("-> Agregando columna 'rut' a dim_cliente...")
            conn.execute(text("ALTER TABLE dbo.dim_cliente ADD rut VARCHAR(12) NULL;"))

        # Verificar y agregar cantidad_ventas para SCD2 asegurando el esquema dbo
        check_ventas = conn.execute(text("""
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'dim_cliente' AND COLUMN_NAME = 'cantidad_ventas'
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

    # 1. VALIDACIÓN Y DEDUPLICACIÓN DE CLIENTES (Incluye rechazo de RUT nulo/vacío)
    print("Validando y deduplicando clientes por RUT...")
    df_clientes_totales = pd.concat([df_cliente_orig, df_json_clientes], ignore_index=True)
    
    # Estandarizar RUT a string para validación
    df_clientes_totales["rut_clean"] = df_clientes_totales["rut"].astype(str).str.strip().str.upper()
    
    # Identificar RUTs inválidos (nulos, vacíos o "NAN")
    mask_rut_invalido = df_clientes_totales["rut_clean"].isin(["", "NAN", "NONE", "NULL"]) | df_clientes_totales["rut"].isnull()
    
    # Enviar a tabla de rechazos
    for _, row in df_clientes_totales[mask_rut_invalido].iterrows():
        lista_rechazos.append({
            "origen": "dim_cliente", 
            "campo_erroneo": "rut",
            "valor_original": str(row.get("rut")), 
            "motivo_rechazo": "RUT nulo, vacío o inválido (NAN)"
        })
    
    # Filtrar solo clientes válidos y deduplicar
    df_clientes_validos = df_clientes_totales[~mask_rut_invalido].copy()
    df_clientes_validos["rut"] = df_clientes_validos["rut_clean"]
    df_clientes_silver = df_clientes_validos.drop_duplicates(subset=["rut"], keep="last").copy()
    df_clientes_silver.drop(columns=["rut_clean"], inplace=True, errors="ignore")

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

# 5. CAPA ORO Y DATA WAREHOUSE (STAGING Y SCD2)
def ejecutar_carga_oro_y_dw(datos_plata, engine_dw):
    print("\n--- INICIANDO CARGA A CAPA ORO Y DATA WAREHOUSE (OPTIMIZADO) ---")
    os.makedirs(PATH_GOLD, exist_ok=True)

    df_cliente = datos_plata["cliente"]
    df_producto = datos_plata["producto"]
    df_canal = datos_plata["canal"]
    df_fecha = datos_plata["fecha"]
    df_sucursal = datos_plata["sucursal"]
    df_fact = datos_plata["fact_ventas"]
    df_rechazos = datos_plata["rechazos"]

    inicio_proceso = datetime.now()
    filas_extraidas = len(df_fact) + len(df_rechazos)
    filas_cargadas = 0

    try:
        with engine_dw.begin() as conn: # Única transacción segura
            # 1. CARGA RECHAZOS
            if not df_rechazos.empty:
                print("Registrando anomalías en dbo.tabla_rechazos...")
                rechazos_insert = df_rechazos.to_dict(orient="records")
                conn.execute(text("""
                    INSERT INTO dbo.tabla_rechazos (origen, campo_erroneo, valor_original, motivo_rechazo)
                    VALUES (:origen, :campo_erroneo, :valor_original, :motivo_rechazo)
                """), rechazos_insert)

            # 2. SINCRONIZAR DIMENSIONES BÁSICAS
            print("Sincronizando dimensiones Fecha, Producto, Sucursal y Canal...")
            for _, row in df_fecha.iterrows():
                if not conn.execute(text("SELECT 1 FROM dbo.dim_fecha WHERE fecha_key = :fk"), {"fk": int(row["fecha_key"])}).fetchone():
                    conn.execute(text("INSERT INTO dbo.dim_fecha (fecha_key, fecha, dia, mes, nombre_mes, trimestre, anio, es_feriado) VALUES (:fecha_key, :fecha, :dia, :mes, :nombre_mes, :trimestre, :anio, :es_feriado)"), row.to_dict())

            for _, row in df_producto.iterrows():
                if not conn.execute(text("SELECT 1 FROM dbo.dim_producto WHERE sku = :sku"), {"sku": str(row["sku"])}).fetchone():
                    conn.execute(text("INSERT INTO dbo.dim_producto (sku, producto, categoria, marca) VALUES (:sku, :producto, :categoria, :marca)"), row.to_dict())

            for _, row in df_sucursal.iterrows():
                if not conn.execute(text("SELECT 1 FROM dbo.dim_sucursal WHERE codigo_sucursal = :cs"), {"cs": str(row["codigo_sucursal"])}).fetchone():
                    conn.execute(text("INSERT INTO dbo.dim_sucursal (codigo_sucursal, sucursal, comuna, region, latitud, longitud) VALUES (:codigo_sucursal, :sucursal, :comuna, :region, :latitud, :longitud)"), row.to_dict())

            for _, row in df_canal.iterrows():
                if not conn.execute(text("SELECT 1 FROM dbo.dim_canal WHERE codigo_canal = :cc"), {"cc": str(row["codigo_canal"])}).fetchone():
                    conn.execute(text("INSERT INTO dbo.dim_canal (codigo_canal, canal, tipo_canal) VALUES (:codigo_canal, :canal, :tipo_canal)"), row.to_dict())

            # 3.0 BACKFILL DE RUT (clientes existentes cuya columna rut fue recreada o está vacía)
            print("Rellenando RUT faltante en dim_cliente...")
            backfill_rut = [
                {"rut": str(c["rut"]), "cid": int(c["cliente_id"])}
                for _, c in df_cliente.iterrows()
                if pd.notna(c.get("cliente_id")) and str(c.get("rut")) not in ("NAN", "NONE", "")
            ]
            if backfill_rut:
                conn.execute(text("""
                    UPDATE dbo.dim_cliente
                    SET rut = :rut
                    WHERE cliente_id = :cid AND rut IS NULL
                """), backfill_rut)

            # 3.1 LÓGICA SCD TIPO 2 PARA CLIENTES
            print("Procesando SCD Tipo 2 para dim_cliente...")
            for _, cliente in df_cliente.iterrows():
                c_id, c_rut = int(cliente.get("cliente_id")), str(cliente.get("rut"))
                c_comuna, c_region, c_segmento = str(cliente.get("comuna", "")), str(cliente.get("region", "")), str(cliente.get("segmento", ""))
                
                res = conn.execute(text("SELECT cliente_key, comuna, region, segmento FROM dbo.dim_cliente WHERE rut = :rut AND es_actual = 1"), {"rut": c_rut}).fetchone()
                
                if res:
                    old_key, old_comuna, old_region, old_segmento = res
                    if old_comuna != c_comuna or old_region != c_region or old_segmento != c_segmento:
                        print(f"-> SCD2: Actualizando historial para cliente RUT {c_rut}")
                        conn.execute(text("UPDATE dbo.dim_cliente SET valido_hasta = GETDATE(), es_actual = 0 WHERE cliente_key = :key"), {"key": old_key})
                        conn.execute(text("INSERT INTO dbo.dim_cliente (cliente_id, rut, nombre, comuna, region, segmento, valido_desde, es_actual, cantidad_ventas) VALUES (:cliente_id, :rut, :nombre, :comuna, :region, :segmento, GETDATE(), 1, 0)"), cliente.to_dict())
                else:
                    if not conn.execute(text("SELECT 1 FROM dbo.dim_cliente WHERE cliente_id = :cid"), {"cid": c_id}).fetchone():
                        conn.execute(text("INSERT INTO dbo.dim_cliente (cliente_id, rut, nombre, comuna, region, segmento, valido_desde, es_actual, cantidad_ventas) VALUES (:cliente_id, :rut, :nombre, :comuna, :region, :segmento, '2024-01-01', 1, 0)"), cliente.to_dict())

            # 4. EXTRACCIÓN DE CLAVES DEL DW EN MEMORIA
            print("Obteniendo mapas de dimensiones del DW para asociar transacciones...", flush=True)
            df_prod_dw = pd.read_sql(text("SELECT producto_key, sku FROM dbo.dim_producto"), conn)
            dw_sku_to_key = dict(zip(df_prod_dw["sku"], df_prod_dw["producto_key"]))

            df_suc_dw = pd.read_sql(text("SELECT sucursal_key, codigo_sucursal FROM dbo.dim_sucursal"), conn)
            dw_cod_to_suc_key = dict(zip(df_suc_dw["codigo_sucursal"], df_suc_dw["sucursal_key"]))

            df_can_dw = pd.read_sql(text("SELECT canal_key, codigo_canal FROM dbo.dim_canal"), conn)
            dw_cod_to_can_key = dict(zip(df_can_dw["codigo_canal"], df_can_dw["canal_key"]))

            df_cli_dw = pd.read_sql(text("SELECT cliente_key, cliente_id FROM dbo.dim_cliente WHERE es_actual = 1"), conn)
            dw_id_to_cli_key = dict(zip(df_cli_dw["cliente_id"], df_cli_dw["cliente_key"]))

            boletas_existentes_q = conn.execute(text("SELECT num_boleta FROM dbo.fact_ventas WHERE num_boleta IS NOT NULL")).fetchall()
            boletas_existentes = set([row[0] for row in boletas_existentes_q])

            # 5. POBLAR TABLA STAGING Y FACT_VENTAS
            total_ventas = len(df_fact)
            print(f"Preparando inserción de {total_ventas} ventas en lote...", flush=True)
            
            fact_inserts = []
            staging_inserts = []
            
            for _, venta in df_fact.iterrows():
                n_boleta = venta.get("num_boleta")
                if n_boleta in boletas_existentes:
                    continue  

                sku_val = datos_plata["orig_prod_map"].get(venta.get("producto_key"), "")
                cod_suc_val = datos_plata["orig_suc_map"].get(venta.get("sucursal_key"), "")
                cod_can_val = datos_plata["orig_can_map"].get(venta.get("canal_key"), "")
                cli_id_val = datos_plata["orig_cli_map"].get(venta.get("cliente_key"), 999)

                fk_prod = dw_sku_to_key.get(sku_val, 999)
                fk_suc = dw_cod_to_suc_key.get(cod_suc_val, 999)
                fk_can = dw_cod_to_can_key.get(cod_can_val, 1)
                fk_cli = dw_id_to_cli_key.get(cli_id_val, 999)

                fact_inserts.append({
                    "fecha_key": int(venta.get("fecha_key")),
                    "producto_key": fk_prod,
                    "sucursal_key": fk_suc,
                    "canal_key": fk_can,
                    "cliente_key": fk_cli,
                    "num_boleta": n_boleta,
                    "cantidad": int(venta.get("cantidad", 0)),
                    "monto_bruto": float(venta.get("monto_bruto", 0.0)),
                    "monto_neto": float(venta.get("monto_neto", 0.0)),
                    "es_devolucion": int(venta.get("es_devolucion", 0))
                })

                staging_inserts.append({
                    "fecha_key": int(venta.get("fecha_key")),
                    "producto_key": fk_prod,
                    "sucursal_key": fk_suc,
                    "canal_key": fk_can,
                    "cliente_key": fk_cli,
                    "num_boleta": n_boleta,
                    "cantidad": int(venta.get("cantidad", 0)),
                    "monto_neto": float(venta.get("monto_neto", 0.0))
                })

            if fact_inserts:
                # 5.1 Carga en Staging (Idempotencia y Auditoría transitoria)
                print(f"Limpiando y poblando {len(staging_inserts)} registros en dbo.fact_ventas_staging...")
                conn.execute(text("TRUNCATE TABLE dbo.fact_ventas_staging;"))
                conn.execute(text("""
                    INSERT INTO dbo.fact_ventas_staging (fecha_key, producto_key, sucursal_key, canal_key, cliente_key, num_boleta, cantidad, monto_neto)
                    VALUES (:fecha_key, :producto_key, :sucursal_key, :canal_key, :cliente_key, :num_boleta, :cantidad, :monto_neto)
                """), staging_inserts)
                
                # 5.2 Carga Final en Hechos
                print(f"Insertando {len(fact_inserts)} registros nuevos en dbo.fact_ventas...")
                conn.execute(text("""
                    INSERT INTO dbo.fact_ventas (fecha_key, producto_key, sucursal_key, canal_key, cliente_key, num_boleta, cantidad, monto_bruto, monto_neto, es_devolucion)
                    VALUES (:fecha_key, :producto_key, :sucursal_key, :canal_key, :cliente_key, :num_boleta, :cantidad, :monto_bruto, :monto_neto, :es_devolucion)
                """), fact_inserts)
                filas_cargadas = len(fact_inserts)
            else:
                print("No hay transacciones nuevas para insertar (Pipeline Idempotente).")

            # 6. ACTUALIZACIÓN DEL CONTEO DE VENTAS (SCD2)
            print("Calculando y actualizando 'cantidad_ventas' por versión de cliente en dim_cliente...")
            conn.execute(text("""
                UPDATE dbo.dim_cliente 
                SET cantidad_ventas = COALESCE((
                    SELECT COUNT(*) 
                    FROM dbo.fact_ventas f 
                    WHERE f.cliente_key = dbo.dim_cliente.cliente_key
                ), 0);
            """))

            # 7. REGISTRO DE AUDITORÍA
            conn.execute(text("""
                INSERT INTO dbo.etl_log_ejecucion (nombre_paquete, fecha_inicio, fecha_fin, filas_extraidas, filas_cargadas, filas_rechazadas, estado, mensaje_error)
                VALUES ('ETL_DataRetail360_Master', :f_inicio, GETDATE(), :f_ext, :f_car, :f_rec, 'EXITOSO', 'Staging y Conteo SCD2 aplicados correctamente')
            """), {"f_inicio": inicio_proceso, "f_ext": filas_extraidas, "f_car": filas_cargadas, "f_rec": len(df_rechazos)})

        print("✓ ¡Pipeline completado con éxito! Staging, RUT Backfill y SCD2 funcionando perfectamente.")

    except Exception as e:
        print(f"❌ Error crítico: {e}")
        with engine_dw.connect() as conn_err:
            conn_err.execute(text("""
                INSERT INTO dbo.etl_log_ejecucion (nombre_paquete, fecha_inicio, fecha_fin, estado, mensaje_error)
                VALUES ('ETL_DataRetail360_Master', :f_inicio, GETDATE(), 'ERROR', :msg)
            """), {"f_inicio": inicio_proceso, "msg": str(e)[:4000]})
            conn_err.commit()
        raise e

# 6. EJECUCIÓN PRINCIPAL
if __name__ == "__main__":
    preparar_estructura_dw()
    d_bronz = ejecutar_extraccion_bronze()
    d_silver = ejecutar_capa_silver(d_bronz)
    ejecutar_carga_oro_y_dw(d_silver, engine_dw)