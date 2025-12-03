import os
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# -------------------------
# SUPABASE API REST (SOURCE)
# -------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# -------------------------
# SUPABASE JDBC (solo si luego lo usas)
# -------------------------
SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_PORT = os.getenv("SUPABASE_PORT", "5432")
SUPABASE_DB = os.getenv("SUPABASE_DB", "postgres")
SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "locations")
SUPABASE_SCHEMA = os.getenv("SUPABASE_SCHEMA", "public")

# -------------------------
# POSTGRES ORIGEN (Render / Supabase / otro)
# -------------------------
SOURCE_HOST = os.getenv("SOURCE_HOST")
SOURCE_PORT = os.getenv("SOURCE_PORT", "5432")
SOURCE_DB = os.getenv("SOURCE_DB")
SOURCE_USER = os.getenv("SOURCE_USER")
SOURCE_PASSWORD = os.getenv("SOURCE_PASSWORD")
SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "public")
SOURCE_TABLE = os.getenv("SOURCE_TABLE", "locations")

# -------------------------
# POSTGRES DESTINO (DigitalOcean)
# -------------------------
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
TARGET_TABLE = os.getenv("TARGET_TABLE", "locations")

# -------------------------
# Rutas
# -------------------------
BASE_DIR = os.getcwd()
CSV_FILE_PATH = os.path.join(BASE_DIR, "data", "locations_rows.csv")
GEOJSON_FILE_PATH = os.path.join(BASE_DIR, "data", "limites_oficiales_scz.geojson")
JDBC_DRIVER_PATH = os.path.join(BASE_DIR, "drivers", "postgresql-42.7.8.jar")

# -------------------------
# Constantes de transformación
# -------------------------
ALTITUD_BAJA_MAX = 400
ALTITUD_MEDIA_MAX = 800
COBERTURA_MEDIA_MIN_DBM = -95
COBERTURA_ALTA_MIN_DBM = -75