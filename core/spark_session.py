import os
from pyspark.sql import SparkSession
from .config import JDBC_DRIVER_PATH

def create_spark_session():
    # Establecer variables de entorno necesarias para la JVM
    # Esto es crucial en Linux, como ya lo experimentaste.
    # Asume que JAVA_HOME y SPARK_HOME están en el PATH o definidos globalmente

    # Para ser extremadamente seguros, si se ejecuta con Python directamente:
    # os.environ['PYSPARK_PYTHON'] = ... (ruta a tu .venv)
    # os.environ['PYSPARK_DRIVER_PYTHON'] = ... (ruta a tu .venv)
    
    spark = (
        SparkSession.builder
        .appName("ETL_QoS_Automation")
        .master("local[*]")
        
        # Configuración de estabilidad y JDBC
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.jars", JDBC_DRIVER_PATH)
        .config("spark.driver.extraClassPath", JDBC_DRIVER_PATH)
        
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark