import json
import os
from shapely.geometry import Point, shape
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType

# -------------------------------------------------------------
# CARGAR GEOJSON DE ZONAS (SHAPELY EN DRIVER)
# -------------------------------------------------------------
def cargar_zonas_desde_geojson(geojson_path):
    if not os.path.exists(geojson_path):
        print(f"⚠️ Advertencia: No se encontró el GEOJSON en: {geojson_path}. Devolviendo lista vacía.")
        return []

    print(f"⚙️ Cargando polígonos desde GEOJSON: {geojson_path} ...")

    try:
        with open(geojson_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"❌ Error al cargar GEOJSON: {e}")
        return []

    zonas = []
    for feature in data.get("features", []):
        props = feature.get("properties", {})
        nombre_zona = props.get("name") or props.get("zona") or "ZONA_SIN_NOMBRE"
        geom = shape(feature["geometry"])
        zonas.append({
            "name": nombre_zona,
            "geometry": geom
        })

    print(f"✅ Zonas cargadas: {len(zonas)}")
    return zonas


# -------------------------------------------------------------
# FUNCIONES DE TRANSFORMACIÓN (UDFs para PySpark)
# -------------------------------------------------------------

def asignar_zona_func(lat, lon, zonas_broadcast):
    """Asigna una zona basada en latitud y longitud usando geometrías cargadas."""
    zonas = zonas_broadcast.value 
    if lat is None or lon is None:
        return "Desconocida"
    try:
        # Shapely espera (lon, lat)
        punto = Point(float(lon), float(lat))
    except (ValueError, TypeError):
        return "ERROR_COORDENADA" 
    
    for z in zonas:
        if z["geometry"].contains(punto):
            return z["name"]
    return "Fuera_de_zona"


def discretizar_altitud_func(altitud):
    """Clasifica la altitud en Baja, Media o Alta."""
    from core.config import ALTITUD_BAJA_MAX, ALTITUD_MEDIA_MAX
    
    if altitud is None:
        return "Baja"
    if altitud <= ALTITUD_BAJA_MAX: 
        return "Baja"
    elif altitud <= ALTITUD_MEDIA_MAX: 
        return "Media"
    else:
        return "Alta"


def discretizar_cobertura_func(signal):
    """Clasifica la señal en Alta, Media o Baja."""
    from core.config import COBERTURA_MEDIA_MIN_DBM, COBERTURA_ALTA_MIN_DBM
    
    if signal is None:
        return "Baja"
    if signal >= COBERTURA_ALTA_MIN_DBM: 
        return "Alta"
    elif signal >= COBERTURA_MEDIA_MIN_DBM: 
        return "Media"
    else:
        return "Baja"
    
    
def discretizar_velocidad_func(velocidad):
    """Clasifica la velocidad en Detenido, Lento, Normal o Rápido."""
    from core.config import VELOCIDAD_LENTA_MAX, VELOCIDAD_NORMAL_MAX
    
    if velocidad is None:
        return "Detenido"
    
    if velocidad == 0:
        return "Detenido"
    elif velocidad <= VELOCIDAD_LENTA_MAX: # 0 y <= 20
        return "Lento"
    elif velocidad <= VELOCIDAD_NORMAL_MAX: # > 20 y <= 60
        return "Normal"
    else:
        return "Rápido" # > 60
    

def discretizar_bateria_func(bateria):
    """Clasifica el nivel de batería en Crítica, Baja, Media o Alta"""
    from core.config import BATERIA_CRITICA_MAX, BATERIA_BAJA_MAX, BATERIA_MEDIA_MAX
    
    if bateria is None:
        return "Baja"
    
    if bateria <= BATERIA_CRITICA_MAX: # 0-10%
        return "Crítica"
    elif bateria <= BATERIA_BAJA_MAX: # 11-30%
        return "Baja"
    elif bateria <= BATERIA_MEDIA_MAX: # 31-70%
        return "Media"
    else: # 71-100%
        return "Alta"
    

def discretizar_compania_func(operator):
    """
    Normaliza y clasifica el operador en ENTEL, TIGO, VIVA, OTRO o SIN_SEÑAL.
    """
    if operator is None:
        return "OTRO"

    op = operator.strip().upper()

    # Operadores principales
    if "ENTEL" in op:
        return "ENTEL"
    if "TIGO" in op:
        return "TIGO"
    if "VIVA" in op or "MOVIL GSM" in op:
        return "VIVA"

    # Todo lo demás → OTRO
    return "OTRO"


discretizar_altitud_spark = udf(discretizar_altitud_func, StringType())
discretizar_cobertura_spark = udf(discretizar_cobertura_func, StringType())
discretizar_velocidad_spark = udf(discretizar_velocidad_func, StringType())
discretizar_bateria_spark = udf(discretizar_bateria_func, StringType())
discretizar_compania_spark = udf(discretizar_compania_func, StringType())


# -------------------------------------------------------------
# FUNCIÓN PRINCIPAL DE TRANSFORMACIÓN
# -------------------------------------------------------------
def transform_data(df_spark, spark_session):
    """
    Aplica todas las transformaciones UDFs al DataFrame de Spark.
    """
    from core.config import GEOJSON_FILE_PATH
    
    # 1. Cargar Zonas (Esto debe ocurrir en el Driver y luego hacerse Broadcast)
    zonas = cargar_zonas_desde_geojson(GEOJSON_FILE_PATH)
    zonas_broadcast = spark_session.sparkContext.broadcast(zonas)

    # 2. Definir y aplicar UDFs (Usando las funciones que ya tienes)
    def asignar_zona_udf_wrapper(lat, lon):
        return asignar_zona_func(lat, lon, zonas_broadcast)

    asignar_zona_spark = udf(asignar_zona_udf_wrapper, StringType())
    
    # 3. Aplicar todas las transformaciones
    df_transformado = df_spark.withColumn(
        "zona",
        asignar_zona_spark(col("latitude"), col("longitude"))
    ).withColumn(
        "altitud_categoria",
        discretizar_altitud_spark(col("altitude"))
    ).withColumn(
        "cobertura_categoria",
        discretizar_cobertura_spark(col("signal"))
    ).withColumn(
        "velocidad_categoria",
        discretizar_velocidad_spark(col("speed"))
    ).withColumn(
        "bateria_categoria",
        discretizar_bateria_spark(col("battery"))
    ).withColumn(
        "compania",
        discretizar_compania_spark(col("sim_operator"))
    )
    
    # 4. Seleccionar y renombrar
    columnas_finales = [
        # IDs
        "id", 
        
        # Coordenadas y Zona
        col("latitude").alias("latitud"),
        col("longitude").alias("longitud"),
        "zona", 
        
        # Datos de Altitud
        col("altitude").alias("altitud"),
        "altitud_categoria", 
        
        # Datos de Señal/Cobertura
        col("signal").alias("cobertura"),
        "cobertura_categoria", 
        
        # Datos de Velocidad y Batería
        col("speed").alias("velocidad"),
        "velocidad_categoria", 
        col("battery").alias("bateria"),
        "bateria_categoria",
        
        # Datos del Operador
        "compania",
        
        # Marca de Tiempo
        col("timestamp").alias("fecha_hora")
    ]
    
    df_final = df_transformado.select(*columnas_finales).fillna({
        "zona": "Desconocida", 
        "altitud_categoria": "Desconocida", 
        "cobertura_categoria": "Desconocida",
        "velocidad_categoria": "Desconocida",
        "bateria_categoria": "Desconocida",
        "compania": "Desconocida"
    })
    
    return df_final