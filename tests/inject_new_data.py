# inject_new_data.py
import psycopg2
from datetime import datetime, timedelta
import os
import sys

# Agrega la ruta del proyecto al path para importar core.config
# sys.path.append(os.getcwd())
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_DIR)

# Importar configuraciones de la DB local (Origen de Prueba)
try:
    from core.config import SOURCE_HOST, SOURCE_PORT, SOURCE_DB, SOURCE_USER, SOURCE_PASSWORD
    from core.config import SOURCE_TABLE
except ImportError:
    print("ERROR: Asegúrate de que el archivo core/config.py exista y esté configurado.")
    sys.exit(1)


def get_max_id(cur, table_name):
    """Consulta el ID máximo actual de la tabla de origen."""
    try:
        cur.execute(f"SELECT MAX(id) FROM public.{table_name};")
        max_id = cur.fetchone()[0]
        # Si la tabla está vacía, MAX(id) es None. Usamos 0 en ese caso.
        return max_id if max_id is not None else 0
    except Exception as e:
        # Esto puede ocurrir si la tabla aún no existe
        print(f"Advertencia: No se pudo obtener MAX(id). Asumiendo 0. Error: {e}")
        return 0


def inject_new_records(host, port, dbname, user, password, table_name, num_records=3):
    """
    Se conecta a la base de datos de origen e inserta nuevos registros
    continuando la secuencia de ID.
    """
    conn = None
    records_injected = 0
    try:
        # Crea la cadena de conexión de psycopg2
        conn_string = f"host={host} dbname={dbname} user={user} password={password} port={port}"
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        
        print(f"Conectado a la base de datos '{dbname}' para inyectar datos.")

        # 1. OBTENER EL ID MÁXIMO
        last_id = get_max_id(cur, table_name)
        start_id = last_id + 1
        print(f"El último ID cargado fue {last_id}. Comenzando la inyección en ID: {start_id}")


        # 2. CALCULAR MARCA DE TIEMPO (siempre más nuevo que el último registro)
        # Sumamos 1 minuto a la hora actual para garantizar que es 'nuevo'.
        new_timestamp = datetime.now() + timedelta(minutes=1)
        new_timestamp_str = new_timestamp.strftime('%Y-%m-%d %H:%M:%S')

        
        # 3. INYECTAR NUEVOS REGISTROS
        for i in range(num_records):
            new_id = start_id + i
            
            # Consulta INSERT con valores de prueba consistentes
            sql = f"""
            INSERT INTO public.{table_name} (
                id, device_name, latitude, longitude, altitude, speed, battery, signal, sim_operator, network_type, timestamp, device_id
            ) VALUES (
                {new_id}, 'TEST_DEVICE_{i}', -17.8 + 0.001 * {i}, -63.1 - 0.001 * {i}, 450, 0, 75, -60, 'ENTEL', '4G', '{new_timestamp_str}', 'TEST_ID_{new_id}'
            );
            """
            cur.execute(sql)
            records_injected += 1

        conn.commit()
        print(f"🎉 ÉXITO: {records_injected} nuevos registros inyectados (IDs {start_id} a {new_id}).")

    except Exception as e:
        print(f"❌ ERROR al inyectar datos: {e}")
        if conn:
            conn.rollback()
        sys.exit(1)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    inject_new_records(
        host=SOURCE_HOST,
        port=SOURCE_PORT,
        dbname=SOURCE_DB,
        user=SOURCE_USER,
        password=SOURCE_PASSWORD,
        table_name=SOURCE_TABLE,
        num_records=3
    )