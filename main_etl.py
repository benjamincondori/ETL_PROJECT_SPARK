import sys
import os
import logging
import time

# Añadir la ruta del proyecto para importar módulos (necesario en algunos entornos)
# sys.path.append(os.getcwd())

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_DIR)

# Importar módulos ETL
from core.spark_session import create_spark_session
from etl_modules.extract import extract_data
from etl_modules.transform import transform_data
from etl_modules.load import load_data, get_max_timestamp_from_target

# Forzar logs a usar tu hora local
os.environ["TZ"] = "America/La_Paz"
time.tzset()

# Configurar logging (opcional, pero buena práctica)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_full_etl():
    """Ejecuta el flujo completo de ETL: Extract -> Transform -> Load."""
    spark = None
    try:
        logger.info("🚀 Iniciando proceso ETL (Incremental)...")
        
        # 1. Crear sesión de Spark
        logger.info("🔥 Inicializando sesión de Spark...")
        spark = create_spark_session()
        logger.info("🔥 Sesión de Spark inicializada.")
        
        # 2. PREPARACIÓN: Obtener la marca de tiempo de la última ejecución
        logger.info("⏱️ Obteniendo última marca de tiempo...")
        last_ts = get_max_timestamp_from_target(spark)
        logger.info(f"📌 Última marca de tiempo cargada en destino: {last_ts}")
        
        # 3. Extract: Leer de la DB Origen con filtro incremental
        logger.info("📥 Extrayendo datos desde Supabase...")
        df_raw = extract_data(spark, last_ts, origin="supabase")
        records_extracted = df_raw.count()
        logger.info(f"📥 Extracción completa: {records_extracted} registros nuevos.")
        
        # Validación: Si no hay filas nuevas, detener el proceso
        if records_extracted == 0:
            logger.info("⚠️ No existen registros nuevos. Proceso ETL finalizado.")
            return
        
        # 3. Transform: Aplicar lógica de negocio
        logger.info("🔄 Iniciando transformación de datos...")
        df_transformed = transform_data(df_raw, spark)
        logger.info(f"🔄 Transformación completada: {df_transformed.count()} registros listos para cargar.")
        
        # 4. Load: Escribir en PostgreSQL
        logger.info("📤 Cargando datos en PostgreSQL...")
        load_data(df_transformed)
        logger.info("✅ ÉXITO: Datos cargados correctamente en PostgreSQL.")

    except Exception as e:
        logger.error(f"❌ Error crítico en el proceso ETL: {e}", exc_info=True)
        sys.exit(1) 

    finally:
        if spark:
            spark.stop()
            logger.info("🏁 Sesión de Spark finalizada. ETL completado.")

if __name__ == "__main__":
    run_full_etl()