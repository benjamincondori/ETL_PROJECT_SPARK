import json
import os
from shapely.geometry import Point, shape
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType

# from core.config import (
#     ALTITUD_BAJA_MAX, ALTITUD_MEDIA_MAX, COBERTURA_MEDIA_MIN_DBM, 
#     COBERTURA_ALTA_MIN_DBM, GEOJSON_FILE_PATH
# )


# -------------------------------------------------------------
# CARGAR GEOJSON DE ZONAS (SHAPELY EN DRIVER)
# -------------------------------------------------------------
def cargar_zonas_desde_geojson(geojson_path):
    if not os.path.exists(geojson_path):
        print(f"Advertencia: No se encontró el GEOJSON en: {geojson_path}. Devolviendo lista vacía.")
        return []

    print(f"Cargando polígonos desde GEOJSON: {geojson_path} ...")

    try:
        with open(geojson_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error al cargar GEOJSON: {e}")
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

    print(f"✓ Zonas cargadas: {len(zonas)}")
    return zonas


# -------------------------------------------------------------
# FUNCIONES DE TRANSFORMACIÓN (UDFs para PySpark)
# -------------------------------------------------------------

def asignar_zona_func(lat, lon, zonas_broadcast):
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
    return "FUERA_ZONA"

def discretizar_altitud_func(altitud):
    from core.config import ALTITUD_BAJA_MAX, ALTITUD_MEDIA_MAX
    
    if altitud is None:
        return "Desconocida"
    if altitud <= ALTITUD_BAJA_MAX: 
        return "Baja"
    elif altitud <= ALTITUD_MEDIA_MAX: 
        return "Media"
    else:
        return "Alta"


def discretizar_cobertura_func(signal):
    from core.config import COBERTURA_MEDIA_MIN_DBM, COBERTURA_ALTA_MIN_DBM
    
    if signal is None:
        return "Desconocida"
    if signal >= COBERTURA_ALTA_MIN_DBM: 
        return "Alta"
    elif signal >= COBERTURA_MEDIA_MIN_DBM: 
        return "Media"
    else:
        return "Baja"

discretizar_altitud_spark = udf(discretizar_altitud_func, StringType())
discretizar_cobertura_spark = udf(discretizar_cobertura_func, StringType())


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
    )
    
    # df_transformado = df_spark.withColumn("zona", asignar_zona_spark(col("latitude"), col("longitude")))
    # df_transformado = df_transformado.withColumn("altitud_categoria", discretizar_altitud_spark(col("altitude")))
    # df_transformado = df_transformado.withColumn("cobertura_categoria", discretizar_cobertura_spark(col("signal")))

    # 4. Seleccionar y renombrar
    columnas_finales = [
        "id", "latitude", "longitude", "zona", "altitude", 
        "altitud_categoria", "signal", "cobertura_categoria", col("timestamp").alias("fecha_hora")
    ]
    
    df_final = df_transformado.select(*columnas_finales).fillna({
        "zona": "Desconocida", "altitud_categoria": "Desconocida", "cobertura_categoria": "Desconocida"
    })
    
    return df_final