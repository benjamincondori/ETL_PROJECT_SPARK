import os

# --- DATOS DE CONEXIÓN SUPABASE (SOURCE) ---
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://lmqpbtuljodwklxdixjq.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "sb_publishable_JeXh7gEgHiVx1LQBCcFidA_Ki0ARx4F")

SUPABASE_HOST = os.environ.get("SUPABASE_HOST", "supabase-host.com")
SUPABASE_PORT = os.environ.get("SUPABASE_PORT", "5432")
SUPABASE_DB = os.environ.get("SUPABASE_DB", "postgres")
SUPABASE_USER = os.environ.get("SUPABASE_USER", "postgres")
SUPABASE_PASSWORD = os.environ.get("SUPABASE_PASSWORD", "TU_KEY_O_PASSWORD_SUPABASE")
SUPABASE_TABLE = os.environ.get("SUPABASE_TABLE", "device_locations")
SUPABASE_SCHEMA = os.environ.get("SUPABASE_SCHEMA", "public")

# --- DATOS DE CONEXIÓN ORIGEN (PRUEBA LOCAL: TU POSTGRES EN DIGITALOCEAN) ---
SOURCE_HOST = os.environ.get("SOURCE_HOST", "dpg-d4o2tv24d50c73a8gnqg-a.oregon-postgres.render.com")
SOURCE_PORT = os.environ.get("SOURCE_PORT", "5432")
SOURCE_DB = os.environ.get("SOURCE_DB", "locations_8boa")
SOURCE_USER = os.environ.get("SOURCE_USER", "locations_8boa_user") 
SOURCE_PASSWORD = os.environ.get("SOURCE_PASSWORD", "8F4Ce8CIpwu33KAcmCTRE4u4s4HUagNR") 
SOURCE_SCHEMA = os.environ.get("SOURCE_SCHEMA", "public")
SOURCE_TABLE = os.environ.get("SOURCE_TABLE", "locations") 

# --- DATOS DE CONEXIÓN DIGITAL OCEAN POSTGRESQL (TARGET) ---
DB_HOST = os.environ.get("DB_HOST", "45.55.76.198")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "db_locations")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "benjamin")
TARGET_TABLE = os.environ.get("TARGET_TABLE", "locations")

# --- RUTAS DE ARCHIVOS Y DRIVERS (Asumiendo que se ejecuta desde la raíz del proyecto) ---
BASE_DIR = os.getcwd() 
CSV_FILE_PATH = os.path.join(BASE_DIR, "data", "locations_rows.csv")
GEOJSON_FILE_PATH = os.path.join(BASE_DIR, "data", "limites_oficiales_scz.geojson")
JDBC_DRIVER_PATH = os.path.join(BASE_DIR, "drivers", "postgresql-42.7.8.jar")

# --- PARÁMETROS DE TRANSFORMACIÓN (CONSTANTES) ---
ALTITUD_BAJA_MAX = 400
ALTITUD_MEDIA_MAX = 800
COBERTURA_MEDIA_MIN_DBM = -95
COBERTURA_ALTA_MIN_DBM = -75