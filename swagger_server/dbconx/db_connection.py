from os import environ  
from dotenv import load_dotenv
load_dotenv() 
import psycopg2 as DB
from psycopg2.extensions import connection

def db_conectar() -> connection:
    ip = environ.get('DB_HOST')
    puerto = environ.get('DB_PORT') 
    basedatos = environ.get('DB_NAME')
    usuario = environ.get('DB_USER')
    contrasena = environ.get('DB_PASSWORD')

    print("---dbConectar---")
    print("---Conectando a Postgresql---")
    
    if not all([ip, basedatos, usuario, contrasena]):
        print("Error: Missing environment variables.")
        raise ValueError("Error: Missing environment variables for the Database")

    try:
        conexion = DB.connect(
            user=usuario, 
            password=contrasena, 
            host=ip, 
            port=puerto, 
            database=basedatos
        )
        conexion.autocommit = False
        print("Conexión realizada a la base de datos", conexion)
        return conexion
    except DB.DatabaseError as error:
        print("Error en la conexión")
        raise error

def db_desconectar(conexion):
    print("---dbDesconectar---")
    try:
        conexion.close()
        print("Desconexión realizada correctamente")
        return True
    except DB.DatabaseError as error:
        print("Error en la desconexión")
        print(error)
        return False