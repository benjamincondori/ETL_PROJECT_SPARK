import os
from pyspark.sql import SparkSession
from .config import JDBC_DRIVER_PATH

def create_spark_session():
    """Crea y retorna una SparkSession configurada para el proyecto ETL."""
        
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