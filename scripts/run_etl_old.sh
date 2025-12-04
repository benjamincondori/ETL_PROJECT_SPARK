#!/bin/bash

# Este script se encarga de activar el entorno virtual y ejecutar la aplicación Spark.
# Es el script que se llamará desde CRON.

# -------------------------------------------------------------
# 1. AJUSTES DE RUTAS CRÍTICAS (MODIFICAR)
# -------------------------------------------------------------
PROJECT_DIR="/home/benjamin/ETL_PROJECT_SPARK" 

# -------------------------------------------------------------
# 2. CONFIGURACIÓN DE ENTORNO VIRTUAL
# -------------------------------------------------------------
VENV_ACTIVATE_SCRIPT="$PROJECT_DIR/.venv/bin/activate"

# Verificar si el script de activación existe
if [ ! -f "$VENV_ACTIVATE_SCRIPT" ]; then
    echo "ERROR: El entorno virtual no se encontró en $VENV_ACTIVATE_SCRIPT"
    exit 1
fi

# Activar el entorno virtual
source "$VENV_ACTIVATE_SCRIPT"
echo "Entorno virtual activado."

# -------------------------------------------------------------
# 2.5 EMPAQUETAR MÓDULOS PYTHON
# -------------------------------------------------------------
cd "$PROJECT_DIR" || exit 1

echo "Empaquetando módulos Python..."

# Eliminar zip anterior si existe
rm -f modules.zip

# Crear nuevo zip con los módulos
zip -q -r modules.zip etl_modules/ core/ -x "*.pyc" -x "*__pycache__*" -x "*.git*"

# Verificar que se creó correctamente
if [ ! -f "modules.zip" ]; then
    echo "ERROR: No se pudo crear modules.zip"
    exit 1
fi

echo "✓ Módulos empaquetados: $(du -h modules.zip | cut -f1)"

# -------------------------------------------------------------
# 3. EJECUCIÓN DE PYSPARK (USANDO SPARK-SUBMIT)
# -------------------------------------------------------------
# La aplicación principal de Python a ejecutar
MAIN_APP="main_etl.py"

echo "Iniciando spark-submit para $MAIN_APP..."

# Ejecutar el proceso PySpark
# --conf spark.pyspark.python: Apunta al binario de Python dentro del VENV.
# --driver-class-path: Incluye el driver JDBC.
# --py-files: Distribuye los módulos a los workers
/opt/spark/bin/spark-submit \
    --master local[*] \
    --driver-class-path "$PROJECT_DIR/drivers/postgresql-42.7.8.jar" \
    --conf spark.pyspark.python="$PROJECT_DIR/.venv/bin/python" \
    --py-files "$PROJECT_DIR/modules.zip" \
    "$PROJECT_DIR/$MAIN_APP"

# Capturar el código de salida de spark-submit
EXIT_CODE=$?

# -------------------------------------------------------------
# 4. DESACTIVACIÓN Y LIMPIEZA
# -------------------------------------------------------------
deactivate
echo "Proceso ETL finalizado con código de salida: $EXIT_CODE"

# El script saldrá con el código de spark-submit (0 para éxito, >0 para error)
exit $EXIT_CODE