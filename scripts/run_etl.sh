#!/bin/bash

# Script automatizado para ejecutar ETL incremental con PySpark
# Diseñado para ser llamado desde CRON

# -------------------------------------------------------------
# 1. CONFIGURACIÓN DE RUTAS
# -------------------------------------------------------------
PROJECT_DIR="/home/benjamin/ETL_PROJECT_SPARK"
LOG_FILE="/home/benjamin/etl_log.txt"

# Timestamp de inicio
echo "=================================================="
echo "🕐 ETL iniciado: $(TZ='America/La_Paz' date '+%Y-%m-%d %H:%M:%S')"
echo "=================================================="

# -------------------------------------------------------------
# 2. ACTIVAR ENTORNO VIRTUAL
# -------------------------------------------------------------
VENV_ACTIVATE_SCRIPT="$PROJECT_DIR/.venv/bin/activate"

if [ ! -f "$VENV_ACTIVATE_SCRIPT" ]; then
    echo "❌ ERROR: Entorno virtual no encontrado en $VENV_ACTIVATE_SCRIPT"
    exit 1
fi

source "$VENV_ACTIVATE_SCRIPT"
echo "✅ Entorno virtual activado"

# -------------------------------------------------------------
# 3. CAMBIAR AL DIRECTORIO DEL PROYECTO
# -------------------------------------------------------------
cd "$PROJECT_DIR" || {
    echo "❌ ERROR: No se pudo acceder al directorio $PROJECT_DIR"
    exit 1
}

# -------------------------------------------------------------
# 4. EJECUTAR ETL (con tee para ver en pantalla Y guardar en log)
# -------------------------------------------------------------
echo "🚀 Ejecutando proceso ETL..."

python3 main_etl.py 2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}

# -------------------------------------------------------------
# 5. FINALIZACIÓN
# -------------------------------------------------------------
deactivate

if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Proceso ETL finalizado exitosamente" | tee -a "$LOG_FILE"
else
    echo "❌ Proceso ETL finalizado con errores (código: $EXIT_CODE)" | tee -a "$LOG_FILE"
fi

echo "🕐 Finalizado: $(TZ='America/La_Paz' date '+%Y-%m-%d %H:%M:%S')" | tee -a "$LOG_FILE"
echo "==================================================" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

exit $EXIT_CODE