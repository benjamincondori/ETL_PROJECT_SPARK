import pandas as pd
from sqlalchemy import create_engine, types # <-- Importamos 'types'
import os
import sys

# Agrega la ruta del proyecto al path para importar core.config
# sys.path.append(os.getcwd())
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_DIR)

# Importar configuraciones de la DB local que usamos como origen de prueba
try:
    from core.config import SOURCE_HOST, SOURCE_PORT, SOURCE_DB, SOURCE_USER, SOURCE_PASSWORD
    from core.config import BASE_DIR, SOURCE_TABLE, CSV_FILE_PATH 
except ImportError:
    print("ERROR: Asegúrate de que el archivo core/config.py exista y esté configurado.")
    sys.exit(1)


def load_csv_to_db(csv_path, table_name):
    """
    Lee el CSV con Pandas y lo carga a la base de datos de prueba usando SQLAlchemy,
    forzando los tipos para que coincidan con el esquema de PySpark.
    """
    print(f"--- INICIANDO CARGA INICIAL DE CSV A POSTGRES ---")
    print(f"Base de datos de destino: {SOURCE_DB} en {SOURCE_HOST}")

    # 1. Crear URL de conexión
    DB_URL = f"postgresql://{SOURCE_USER}:{SOURCE_PASSWORD}@{SOURCE_HOST}:{SOURCE_PORT}/{SOURCE_DB}"
    
    # -----------------------------------------------------------------
    # 2. DEFINICIÓN EXPLÍCITA DE TIPOS DE POSTGRESQL (SOLUCIÓN DEL ERROR)
    # -----------------------------------------------------------------
    # Forzamos tipos INT y TIMESTAMP para la coherencia con PySpark
    SQL_DTYPE = {
        'id': types.Integer,
        'device_name': types.TEXT,
        'latitude': types.Float(precision=53),
        'longitude': types.Float(precision=53),
        'altitude': types.Float(precision=53),
        'speed': types.Float(precision=53),
        'battery': types.Integer,
        'signal': types.Integer,
        'sim_operator': types.TEXT,
        'network_type': types.TEXT,
        'timestamp': types.DateTime(timezone=False), # Se mapea a PostgreSQL TIMESTAMP
        'device_id': types.TEXT,
    }

    try:
        # 3. Crear motor de SQLAlchemy
        engine = create_engine(DB_URL)
        
        # 4. Leer CSV con Pandas
        # Usamos parse_dates para que Pandas reconozca el timestamp antes de cargarlo
        df = pd.read_csv(csv_path, parse_dates=['timestamp'])
        
        print(f"CSV cargado en Pandas. Filas: {len(df)}")
        
        # 5. Limpieza básica antes de la carga
        df.dropna(subset=['id'], inplace=True)
        
        # 6. Cargar en PostgreSQL
        df.to_sql(
            table_name,
            engine,
            if_exists='replace', 
            index=False,
            chunksize=5000,
            method='multi',
            dtype=SQL_DTYPE # <-- Aplicamos el mapeo de tipos
        )
        
        print(f"\n🎉 ÉXITO: {len(df)} registros cargados a la tabla '{table_name}'.")
        
    except ImportError:
        print("\nERROR: Asegúrate de que la librería psycopg2 esté instalada correctamente (pip install psycopg2).")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO AL CONECTAR O CARGAR A POSTGRES: {e}")
        print("Verifica las credenciales en core/config.py, el firewall (puerto 5432) y si el servicio de PostgreSQL está activo.")
        sys.exit(1)


if __name__ == "__main__":
    load_csv_to_db(
        csv_path=os.path.join(BASE_DIR, 'data', 'locations_rows.csv'),
        table_name=SOURCE_TABLE
    )