from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
)
# from core.config import (
#     SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DB, SUPABASE_USER, 
#     SUPABASE_PASSWORD, SUPABASE_TABLE, SUPABASE_SCHEMA
# )

from core.config import (
    SOURCE_HOST, SOURCE_PORT, SOURCE_DB, SOURCE_USER, SOURCE_PASSWORD, 
    SOURCE_TABLE, SOURCE_SCHEMA
)
from datetime import datetime


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

def extract_data(spark, last_run_timestamp=None):
    """Extrae datos de la tabla de origen (Postgres local o Supabase)"""
    
    # jdbc_url = f"jdbc:postgresql://{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"
    jdbc_url = f"jdbc:postgresql://{SOURCE_HOST}:{SOURCE_PORT}/{SOURCE_DB}"
    
    connection_properties = {
        "user": SOURCE_USER,
        "password": SOURCE_PASSWORD,
        "driver": "org.postgresql.Driver",
        # "sslmode": "require" # Crucial para Supabase
    }

    # Consulta base para seleccionar todos los datos
    sql_base = f"SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}"
    
    if last_run_timestamp:
        # Formatear el timestamp: PostgreSQL entiende el formato ISO
        # Utilizamos la columna 'timestamp' de la DB Origen
        ts_str = last_run_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
        
        # Añadir filtro incremental: solo registros MÁS NUEVOS que la última carga
        query = f"({sql_base} WHERE timestamp > '{ts_str}') AS data_alias"
        print(f"-> Aplicando filtro incremental: WHERE timestamp > '{ts_str}'")
    else:
        # Primera ejecución o tabla destino vacía: Carga Full (sin filtro)
        query = f"({sql_base}) AS data_alias"
        print("-> Carga inicial: Extrayendo todos los registros.")

    df_spark = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query) # Usar la consulta SQL con el filtro
        .options(**connection_properties)
        .schema(DB_SCHEMA)
        .load()
    )
    return df_spark