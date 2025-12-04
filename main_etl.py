import sys
import os
import logging

# Añadir la ruta del proyecto para importar módulos (necesario en algunos entornos)
# sys.path.append(os.getcwd())

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_DIR)

# Importar módulos ETL
from core.spark_session import create_spark_session
from etl_modules.extract import extract_data
from etl_modules.transform import transform_data
from etl_modules.load import load_data, get_max_timestamp_from_target

# Configurar logging (opcional, pero buena práctica)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_full_etl():
    """Ejecuta el flujo completo de ETL: Extract -> Transform -> Load."""
    spark = None
    try:
        logger.info("Iniciando proceso ETL (Incremental)...")
        
        # 1. Crear sesión de Spark
        spark = create_spark_session()
        logger.info("Sesión de Spark inicializada.")
        
        # 2. PREPARACIÓN: Obtener la marca de tiempo de la última ejecución
        last_ts = get_max_timestamp_from_target(spark)
        logger.info(f"Última marca de tiempo cargada en destino: {last_ts}")
        
        # 3. Extract: Leer de la DB Origen con filtro incremental
        # df_raw = extract_data(spark, last_ts)
        df_raw = extract_data(spark, last_ts, origin="supabase")
        logger.info(f"Extracción completa. Filas nuevas leídas: {df_raw.count()}")
        
        # Validación: Si no hay filas nuevas, detener el proceso
        if df_raw.count() == 0:
            logger.info("No se encontraron nuevos registros. Proceso finalizado.")
            return
        
        # 3. Transform: Aplicar lógica de negocio
        df_transformed = transform_data(df_raw, spark)
        logger.info(f"Transformación completa. Registros listos para cargar: {df_transformed.count()}")
        
        # 4. Load: Escribir en Render
        load_data(df_transformed)
        logger.info("Carga de datos a Render completada exitosamente.")

    except Exception as e:
        logger.error(f"Error crítico en el proceso ETL: {e}", exc_info=True)
        sys.exit(1) 

    finally:
        if spark:
            spark.stop()
            logger.info("Sesión de Spark finalizada.")

if __name__ == "__main__":
    run_full_etl()