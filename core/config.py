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
# BASE_DIR = os.getcwd()
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_FILE_PATH = os.path.join(BASE_DIR, "data", "locations_rows.csv")
GEOJSON_FILE_PATH = os.path.join(BASE_DIR, "data", "limites_oficiales_scz.geojson")
JDBC_DRIVER_PATH = os.path.join(BASE_DIR, "drivers", "postgresql-42.7.8.jar")

# -------------------------
# Constantes de transformación
# -------------------------

# Valores típicos de altitud en metros
ALTITUD_BAJA_MAX = 400 # Altitud <= 400m
ALTITUD_MEDIA_MAX = 800 # Altitud entre 401m y 800m
# > 800m se considera Alta

# Valores típicos de señal en dBm
COBERTURA_MEDIA_MIN_DBM = -95 # -95 dBm
COBERTURA_ALTA_MIN_DBM = -75 # -75 dBm
# > -75 dBm se considera Alta

# Valores típicos de porcentaje (0-100)
BATERIA_CRITICA_MAX = 10  # 0% - 10%
BATERIA_BAJA_MAX = 30 # 11% - 30%   
BATERIA_MEDIA_MAX = 70 # 31% - 70% 
# 71% - 100% no necesita constante (es el resto)

# Valores de velocidad en KPH
VELOCIDAD_LENTA_MAX = 20 # Velocidad <= 20 KPH (Caminando/Parado)
VELOCIDAD_NORMAL_MAX = 60 # Velocidad entre 20 y 60 KPH (Ciudad/Bicicleta)
# > 60 KPH se considera Rápida