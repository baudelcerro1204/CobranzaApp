# Backend/database/connection.py
import psycopg2
from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde .env
load_dotenv()

def get_db_connection():
    try:
        # Obtener las variables del archivo .env
        user = os.getenv("user")
        password = os.getenv("password")
        host = os.getenv("host")
        port = os.getenv("port")
        dbname = os.getenv("dbname")
        
        # Conectar a la base de datos
        connection = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            dbname=dbname
        )

        # Log para confirmar la conexión
        print("Conexión a la base de datos exitosa")
        return connection
    except Exception as e:
        print(f"Failed to connect: {e}")
        return None

