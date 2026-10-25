# ETL_DataRetail360 — Pipeline ETL · DataRetail 360

> Proyecto académico para el curso IDY1102 (Duoc UC) - Estructuras y Visualización de Datos.
> Diseño e Implementación de Pipeline ETL y Data Warehouse bajo Arquitectura Medallion.

---

## 1. Descripción

Este pipeline ETL extrae, limpia e integra datos fragmentados de transacciones y clientes desde fuentes heterogéneas (SQL Server, CSV y JSON). Su propósito es centralizar la información aplicando estrictas reglas de calidad en memoria (Pandas) y cargarla de manera idempotente hacia un Data Warehouse modelado en esquema estrella (SQL Server). Esto permite a la organización contar con una "única versión de la verdad" lista para el consumo en Business Intelligence (BI) y asistentes de Inteligencia Artificial.

## 2. Arquitectura

Fuentes (SQL / CSV / JSON) ─► ETL (Python · Pandas) ─► Data Warehouse (Esquema Estrella)
       [Capa Bronce]              [Capa Plata]                  [Capa Oro]

* **Fuentes:** Base de datos relacional (`DataRetail360_Origen`), archivo de transacciones (`ventas.csv`), y catálogo de clientes externo (`clientes.json`).
* **Transformaciones:** Deduplicación de clientes por RUT, homologación de códigos SKU (Regex), limpieza de nulos, normalización de fechas (ISO) y control de cambios históricos geográficos/segmentación (SCD Tipo 2).
* **Destino:** SQL Server (`DataRetail360_DW`) — Tablas dimensionales (`dim_fecha`, `dim_producto`, `dim_sucursal`, `dim_canal`, `dim_cliente`) y tabla central de hechos (`fact_ventas`).

## 3. Requisitos

* Python 3.9+
* Librerías: `pandas`, `sqlalchemy`, `pyodbc`, `python-dotenv`, `pyarrow` (para formato Parquet).
* Motor de Base de Datos: Microsoft SQL Server + SQL Server Management Studio 22 (SSMS).
* Driver ODBC: `ODBC Driver 17 for SQL Server`.

## 4. Instalación

```
git clone https://github.com/TOXICSNNZ/ETL_DataRetail360.git
cd ETL_DataRetail360
pip install pandas sqlalchemy pyodbc python-dotenv pyarrow
```

## 5. Configuración


Se debe crear un archivo `.env` en la raíz del proyecto con la siguiente estructura:

```
DB_SERVER="Nombre del servidor"
DB_DRIVER="Driver de la BBDD utilizada"
DB_ORIGEN="Nombre de la BBDD origen/producción"
DB_DW="Nombre de la BBDD destino/Data Warehouse"
DB_USER="Tu usuario"
DB_PASSWORD="Tu contraseña"
```

## 6. Uso

```
python etl_pipeline.py
```

Al ejecutarse, el sistema procesa los datos secuencialmente:

1. **Bronce:** Extrae los orígenes crudos y los respalda localmente en formato `.parquet`.
2. **Plata:** Ejecuta la limpieza, filtra errores y mapea las transacciones en memoria RAM.
3. **Oro:** Realiza el cruce de *Surrogate Keys*, inserta los datos nuevos en la tabla Staging y pobla las tablas finales.

Los resultados técnicos del procesamiento (filas afectadas, tiempos) se pueden consultar directamente ejecutando `SELECT * FROM dbo.etl_log_ejecucion;` en SQL Server Management Studio.

## 7. Estructura del proyecto

```
C:\
├── C:\ETL\Fuentes/      # Directorio local con archivos crudos (ventas.csv, clientes.json)
├── C:\ETL\Bronze/       # Respaldo de ingesta inmutable (.parquet)
├── C:\ETL\Silver/       # Datos procesados y limpios (.parquet)
├── C:\ETL\Gold/         # Metadatos listos para el DW (.parquet)

\ETL_DataRetail360
├── etl_pipeline.py      # Script principal del pipeline ETL
├── .env                 # Variables de entorno y credenciales (NO se versiona)
├── .gitignore           # Archivos ignorados por Git
├── requirements.txt     # Dependencias de Python
└── README.md            # Documentación del repositorio
```

*(Nota: Las carpetas de datos transitorios se generan automáticamente fuera del control de versiones por seguridad).*

## 8. Calidad de datos

| Regla | Dimensión de calidad | Acción |
| --- | --- | --- |
| Sin clientes duplicados | Unicidad | Deduplicar evaluando el RUT estandarizado (`keep="last"`). |
| RUT nulo o vacío | Completitud | Rellenar por *Backfill* con `cliente_id` o desviar a rechazos. |
| SKU homologado | Consistencia | Filtrar por patrón Regex estricto (`^[A-Z]{2,3}-\d{3}$`). |
| Montos ≥ 0 | Validez | Enviar montos negativos no marcados como devolución a rechazos. |
| Fechas de transacción | Validez | Verificar la existencia de la fecha en el calendario y forzar ISO. |

## 9. Logging y manejo de errores

* **Tolerancia a fallos:** El pipeline no se detiene si un dato viene corrupto. Las filas inválidas (por errores en SKU, montos, canales o sucursales) se excluyen de la carga y se registran detalladamente en `dbo.tabla_rechazos` (Dead-Letter table) para auditoría de Gobierno de Datos.
* **Trazabilidad:** Cada ejecución deja un registro completo en `dbo.etl_log_ejecucion` con su fecha de inicio/fin, cantidad de filas extraídas, cargadas, rechazadas y estado (EXITOSO o ERROR).
* **Idempotencia:** El pipeline utiliza una tabla temporal (`fact_ventas_staging`) que se limpia con un `TRUNCATE` al inicio de la inserción de hechos. Re-ejecutar el script no duplicará datos ya cargados.

## 10. Pruebas

Para validar localmente la conexión al motor SQL o los dataframes (requiere instalación previa de `pytest`):

```
pytest tests/
```

## 11. Equipo

* **Sebastian Soto**
* **Franco González**
* **Manuel Lazo**

## 12. Licencia / uso académico

Proyecto académico para el curso IDY1102 - Estructuras y Visualización de Datos (Duoc UC). Los datos procesados corresponden al caso de estudio ficticio DataRetail 360 y cumplen con principios de anonimización académica.
