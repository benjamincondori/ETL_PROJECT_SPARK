# 🚀 Proyecto ETL: Discretización Geoespacial y Carga a PostgreSQL

Este proyecto implementa un proceso ETL (Extract, Transform, Load) utilizando PySpark para procesar un archivo CSV de geolocalización, discretizar variables clave (Zona, Altitud y Cobertura) y cargar el resultado final en una tabla de PostgreSQL.

## 📁 1. Estructura del Proyecto

Asegúrese de que su proyecto mantenga la siguiente estructura:

## ⚙️ 2. Requisitos y Configuración Previa

### A. Requisitos de Software
1.  **Java Development Kit (JDK):** Requerido por Apache Spark.
2.  **Apache Spark:** Instalado y accesible desde la línea de comandos.
3.  **PostgreSQL:** Servidor de base de datos en ejecución.
4.  **Driver JDBC de PostgreSQL:** Descargue el archivo `postgresql-42.x.x.jar` y colóquelo en la carpeta `drivers/`.

### B. Comandos de Configuración

Ejecute los siguientes comandos en su terminal de VS Code:

| Comando | Descripción |
| :--- | :--- |
| `python -m venv .venv` | Crea el entorno virtual. |
| `source .venv/bin/activate` | **Linux/macOS:** Activa el entorno. |
| `.venv\Scripts\activate` | **Windows:** Activa el entorno. |
| `pip install -r requirements.txt` | Instala las dependencias de Python. |
| `mkdir data drivers` | Crea las carpetas de datos y drivers. |
| *Mover los archivos...* | Mueva su **CSV** a `data/` y el **JAR** a `drivers/`. |

## 🔑 3. Ajuste de Parámetros

**Antes de ejecutar**, debe abrir el archivo `etl_script.py` y editar la sección **1. CONFIGURACIÓN** con las credenciales de su base de datos y el nombre exacto de su archivo JDBC:

```python
# EJEMPLO de lo que debe ajustar en etl_script.py
DB_NAME = "su_basededatos"
DB_USER = "su_usuario"
DB_PASSWORD = "su_clave"
JDBC_DRIVER_FILE = "postgresql-42.7.8.jar" 
```

**Esto es crucial para que Spark pueda conectarse.**

## 🚀 4. Ejecución del Proceso ETL

Una vez configurados los parámetros y activo el entorno virtual, ejecute el script principal.

**Comando de Ejecución:**
```bash
python main.py
