from core.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, TARGET_TABLE
from pyspark.sql.functions import max, col
from pyspark.sql.types import TimestampType

def get_max_timestamp_from_target(spark):
    """Obtiene la marca de tiempo más reciente (fecha_hora) de la tabla destino (Render)."""
    
    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    connection_properties = {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver",
    }
    
    # Intentamos leer el máximo timestamp de la tabla destino
    try:
        # Usamos una subconsulta SQL en lugar de cargar toda la tabla para mayor eficiencia
        query = f"(SELECT MAX(fecha_hora) AS max_ts FROM {TARGET_TABLE}) AS max_ts_query"
        
        max_ts_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", query)
            .options(**connection_properties)
            .load()
        )
        
        result = max_ts_df.first()
        
        # Si hay un resultado válido, retornamos el timestamp
        if result and result["max_ts"]:
            return result["max_ts"]
        else:
            return None # Si la tabla está vacía
            
    except Exception as e:
        # Si la tabla aún no existe (primera ejecución), retornamos None
        if "relation" in str(e).lower() and "does not exist" in str(e).lower():
            print("ℹ️ INFO: La tabla destino no existe. Se realizará una carga inicial completa.")
            return None
        else:
            # En caso de otro error, propagar
            raise e

def load_data(spark_df):
    """Carga el DataFrame transformado a la base de datos Render (Target) en modo APPEND."""
    
    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    connection_properties = {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver",
    }
    
    spark_df.write.jdbc(
        url=jdbc_url,
        table=TARGET_TABLE,
        mode="append", # O "overwrite" si se desea reemplazar toda la tabla
        properties=connection_properties,
    )
    print(f"✅ ÉXITO: {spark_df.count()} registros cargados a PostgreSQL en la tabla '{TARGET_TABLE}'.")