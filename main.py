from google.cloud import storage
import psycopg2
from psycopg2 import sql
import pandas as pd
import io  # Importar el módulo io para usar StringIO
import psycopg2
from sqlalchemy import create_engine

# Nombre de tu bucket
bucket_name = 'gab_almacen'
# Ruta del archivo dentro del bucket (sin 'gs://')
blob_name = 'Actor_Names.csv'
# Ruta donde guardar el archivo localmente
destination_file_name = 'archivos/Actor_Names.csv'

def connect_to_postgres():
    try:
        # Crear la conexión a la base de datos
        connection = psycopg2.connect(
            host='35.239.91.226',
            port='5432',
            dbname='dvdrental',
            user='gabuser',
            password='123456'
        )
        # Crear un cursor para ejecutar comandos SQL
        cursor = connection.cursor()
        # Ejecutar una consulta de ejemplo
        cursor.execute("SELECT * FROM public.film_actor LIMIT 10;")
        rows = cursor.fetchall()

        for row in rows:
            print(row)
        # Obtener y mostrar el resultado
        db_version = cursor.fetchone()
        print(f"Conectado a la base de datos PostgreSQL. Versión: {db_version}")
        # Cerrar el cursor y la conexión
        cursor.close()
        connection.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error al conectarse a la base de datos: {error}")


def read_file_cloud_storage(bucket_name, blob_name):
    """Leer el archivo desde Google Cloud Storage y retornarlo como un DataFrame."""
    # Crear cliente de Cloud Storage
    storage_client = storage.Client()

    # Acceder al bucket
    bucket = storage_client.bucket(bucket_name)

    # Acceder al blob (archivo dentro del bucket)
    blob = bucket.blob(blob_name)

    # Leer el contenido del archivo directamente en memoria como texto
    file_content = blob.download_as_text()

    # Convertir el contenido en un DataFrame de pandas (asumiendo que es un CSV)
    data = pd.read_csv(io.StringIO(file_content))  # Usamos io.StringIO aquí

    print(f"El archivo {blob_name} ha sido cargado en un DataFrame.")
    return data


def connect_to_postgres_and_insert(df, table_name):
    """Conectar a PostgreSQL y cargar el DataFrame en la tabla especificada."""
    try:
        # Conectar a PostgreSQL
        connection = psycopg2.connect(
            host='35.239.91.226',
            port='5432',
            dbname='dvdrental',
            user='gabuser',
            password='123456'
        )
        cursor = connection.cursor()

        # Insertar datos en la tabla (usamos el método 'to_sql' si existe SQLAlchemy)
        # Ejemplo para insertar filas individualmente
        for i, row in df.iterrows():
            insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s'] * len(row))})"
            cursor.execute(insert_query, tuple(row))

        # Guardar los cambios
        connection.commit()
        print(f"Datos insertados en la tabla {table_name} con éxito.")

        # Cerrar cursor y conexión
        cursor.close()
        connection.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error al conectarse a la base de datos: {error}")
    """Conectar a PostgreSQL y cargar el DataFrame en la tabla especificada."""
    try:
        # Conectar a PostgreSQL
        connection = psycopg2.connect(
            host='35.239.91.226',
            port='5432',
            dbname='dvdrental',
            user='gabuser',
            password='123456'
        )
        cursor = connection.cursor()
        # Insertar datos en la tabla (usamos el método 'to_sql' si existe SQLAlchemy)
        # Ejemplo para insertar filas individualmente
        for i, row in df.iterrows():
            insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s'] * len(row))})"
            cursor.execute(insert_query, tuple(row))

        # Guardar los cambios
        connection.commit()
        print(f"Datos insertados en la tabla {table_name} con éxito.")

        # Cerrar cursor y conexión
        cursor.close()
        connection.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error al conectarse a la base de datos: {error}")


###
def insert_postgres_table_actor(actors_names):
    try:
              # Datos de conexión a PostgreSQL
        db_host='35.222.51.81'
        db_port='5432'
        db_name='dvdrental'
        db_user='gabuser'
        db_password='123456'

        # Crear una conexión a la base de datos
        engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
        conn = engine.raw_connection()
        cursor = conn.cursor()

        # Dataset en Pandas

        # Iterar sobre el DataFrame y actualizar la tabla film_actor
        for index, row in actors_names.iterrows():
            actor_id = row['actor_id']
            actor_name = row['actor_name']
            
            # Actualizar el valor de name_actor en la tabla film_actor
            update_query = """
            UPDATE film_actor 
            SET name_actor = %s, last_update = CURRENT_TIMESTAMP 
            WHERE actor_id = %s;
            """
            cursor.execute(update_query, (actor_name, actor_id))

        # Confirmar los cambios
        conn.commit()
        print(f"Se han actualizado todos los registros.")
        # Cerrar conexión
        cursor.close()
        conn.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error al conectarse a la base de datos: {error}")

###        

if __name__ == '__main__':
    #connect_to_postgres()
    actors_names = read_file_cloud_storage(bucket_name, blob_name)
    insert_postgres_table_actor(actors_names)
    print(actors_names.head())        
    #connect_to_postgres_and_insert(df, "public.film_actor")




