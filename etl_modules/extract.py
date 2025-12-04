from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
)
from pyspark.sql import SparkSession
from datetime import datetime
import pandas as pd

from core.config import (
    SOURCE_HOST, SOURCE_PORT, SOURCE_DB, SOURCE_USER, SOURCE_PASSWORD, 
    SOURCE_TABLE, SOURCE_SCHEMA
)
from core.config import SUPABASE_URL, SUPABASE_KEY
from supabase import create_client

# Definición del esquema de la DB Origen
DB_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("device_name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("altitude", DoubleType(), True),
    StructField("speed", DoubleType(), True),
    StructField("battery", IntegerType(), True),
    StructField("signal", IntegerType(), True),
    StructField("sim_operator", StringType(), True),
    StructField("network_type", StringType(), True),
    StructField("timestamp", TimestampType(), True), 
    StructField("device_id", StringType(), True),
])


# ============================================================
#   1) EXTRACCIÓN DESDE POSTGRES 
# ============================================================
def extract_data_postgres(spark: SparkSession, last_run_timestamp=None):
    """Extrae datos de la tabla de origen vía JDBC (Postgres)."""
    jdbc_url = f"jdbc:postgresql://{SOURCE_HOST}:{SOURCE_PORT}/{SOURCE_DB}"
    
    connection_properties = {
        "user": SOURCE_USER,
        "password": SOURCE_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    # Consulta base para seleccionar todos los datos
    sql_base = f"SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}"
    
    if last_run_timestamp:
        # Formatear el timestamp: PostgreSQL entiende el formato ISO
        # Utilizamos la columna 'timestamp' de la DB Origen
        ts_str = last_run_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
        
        # Añadir filtro incremental: solo registros MÁS NUEVOS que la última carga
        query = f"({sql_base} WHERE timestamp > '{ts_str}') AS data_alias"
        print(f"➡️ Aplicando filtro incremental: WHERE timestamp > '{ts_str}'")
    else:
        # Primera ejecución o tabla destino vacía: Carga Full (sin filtro)
        query = f"({sql_base}) AS data_alias"
        print("➡️ Carga inicial: Extrayendo todos los registros.")

    df_spark = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query) # Usar la consulta SQL con el filtro
        .options(**connection_properties)
        .schema(DB_SCHEMA)
        .load()
    )
    return df_spark

# ============================================================
#   2) EXTRACCIÓN DESDE SUPABASE (API REST)
# ============================================================
def extract_data_supabase(spark: SparkSession, last_run_timestamp=None, table_name="locations"):
    """Extrae datos desde Supabase usando la API (URL + KEY)."""

    # Crear cliente Supabase
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Filtro incremental
    if last_run_timestamp:
        ts_str = last_run_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
        print(f"➡️ [Supabase] Filtro incremental: timestamp > '{ts_str}'")

        response = (
            supabase.table(table_name)
            .select("*")
            .gt("timestamp", ts_str)
            .execute()
        )
    else:
        print("➡️ [Supabase] Carga inicial completa.")
        response = supabase.table(table_name).select("*").execute()

    data = response.data

    if not data:
        print("⚠️ [Supabase] No se encontraron registros.")
        # DF vacío con esquema correcto
        return spark.createDataFrame([], schema=DB_SCHEMA)

    # Supabase → pandas
    pdf = pd.DataFrame(data)
    
    # Asegurar que la columna timestamp es del tipo datetime
    pdf['timestamp'] = pd.to_datetime(pdf['timestamp'])

    # pandas → Spark con esquema consistente
    df_spark = spark.createDataFrame(pdf, schema=DB_SCHEMA)

    print(f"✅ [Supabase] Filas extraídas: {df_spark.count()}")
    return df_spark

# ============================================================
#   2) EXTRACCIÓN DESDE SUPABASE (API REST) - CON PAGINACIÓN
# ============================================================
def extract_data_supabase_paginated(spark: SparkSession, last_run_timestamp=None, table_name="locations"):
    """Extrae datos desde Supabase usando la API (URL + KEY) con paginación."""

    # Tamaño del bloque de datos por llamada. Supabase (PostgREST) tiene un límite por defecto.
    PAGE_SIZE = 5000 
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    all_data = []  # Lista para almacenar todos los registros de todas las páginas
    offset = 0     # Posición inicial para el desplazamiento
    
    print("➡️ [Supabase] Iniciando extracción paginada...")

    # Configuración del filtro incremental (si existe)
    if last_run_timestamp:
        ts_str = last_run_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
        print(f"➡️ [Supabase] Aplicando filtro incremental: timestamp > '{ts_str}'")
        base_query = supabase.table(table_name).select("*").gt("timestamp", ts_str)
    else:
        print("➡️ [Supabase] Carga inicial completa (FULL LOAD).")
        base_query = supabase.table(table_name).select("*")
        
    # --- Bucle de Paginación ---
    while True:
        # Añadir LIMIT y OFFSET a la consulta base
        response = (
            base_query
            .limit(PAGE_SIZE)
            .offset(offset)
            .execute()
        )
        
        page_data = response.data
        
        if not page_data:
            # Si no hay datos en esta página, hemos llegado al final.
            break
        
        # Almacenar los datos de esta página y avanzar el desplazamiento
        all_data.extend(page_data)
        
        print(f"  ➡️ Página cargada. Total de registros hasta ahora: {len(all_data)}")
        
        # Preparar el OFFSET para la siguiente página
        offset += PAGE_SIZE
        
        # Opcional: Si el número de registros en la página es menor que el límite, también es el final.
        # Esto puede evitar una llamada API innecesaria, aunque el 'if not page_data' ya lo maneja.
        if len(page_data) < PAGE_SIZE:
             break

    # --- Conversión Final ---
    if not all_data:
        print("⚠️ [Supabase] No se encontraron registros en ninguna página.")
        return spark.createDataFrame([], schema=DB_SCHEMA)

    # Supabase (JSON) → pandas
    pdf = pd.DataFrame(all_data)
    
    # Asegurar que la columna timestamp es del tipo datetime (tu arreglo anterior)
    pdf['timestamp'] = pd.to_datetime(pdf['timestamp'])

    # pandas → Spark con esquema consistente
    df_spark = spark.createDataFrame(pdf, schema=DB_SCHEMA)

    print(f"✅ [Supabase] Extracción paginada completada. Filas totales: {df_spark.count()}")
    return df_spark


# ============================================================
#   3) FUNCIÓN PRINCIPAL QUE USA TU ETL
# ============================================================
def extract_data(spark: SparkSession, last_run_timestamp=None, origin="supabase"):
    """
    origin="supabase" -> leer desde Supabase (recomendado)
    origin="postgres" -> leer desde Postgres (origen anterior)
    """
    if origin == "supabase":
        return extract_data_supabase_paginated(spark, last_run_timestamp)
    elif origin == "postgres":
        return extract_data_postgres(spark, last_run_timestamp)
    else:
        raise ValueError("Origen inválido. Use: 'supabase' o 'postgres'.")