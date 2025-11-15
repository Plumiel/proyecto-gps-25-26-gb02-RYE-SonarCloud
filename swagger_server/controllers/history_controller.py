import connexion
import six

from swagger_server.models.error import Error  # noqa: E501
from swagger_server.models.history import History  # noqa: E501
from swagger_server.models.identifier import Identifier
from swagger_server.models.user_genres import UserGenres  # noqa: E501
from swagger_server.models.user_metrics import UserMetrics  # noqa: E501
from swagger_server import util
from swagger_server.dbconx.db_connection import dbConectar, dbDesconectar
from collections import Counter
import requests
from swagger_server.controllers.authorization_controller import is_valid_token

TYA_SERVER = 'http://10.1.1.2:8001'

def check_auth(required_scopes=None):
    """
    Verifica autenticación defensiva (backup de Connexion).
    Devuelve (authorized, error_response) tuple.
    """
    token = connexion.request.cookies.get('oversound_auth')
    if not token or not is_valid_token(token):
        error = Error(code="401", message="Unauthorized: Missing or invalid token")
        return False, (error, 401)
    return True, None, token


def delete_artist_history(body):  # noqa: E501
    """Deletes an artist from user&#x27;s history.

    Delete an user&#x27;s artist history. # noqa: E501

    :param artist_id: Artist id to delete.
    :type artist_id: str

    :rtype: None
    """
    """Add a new track to the database"""

    # Verificar autenticación defensiva
    authorized, error_response, token = check_auth(required_scopes=['write:tracks'])
    if not authorized:
        return error_response
    
    if not connexion.request.is_json:
        return Error(code="400", message="Invalid JSON"), 400
    
    user = is_valid_token(token)
    user_id = user.idUsuario
    print(user_id)
    
    try:
        connection = dbConectar()
        cursor = connection.cursor()

        if connexion.request.is_json:
            body = Identifier.from_dict(connexion.request.get_json())  # noqa: E501

        # Deleting artist
        sql = """DELETE FROM HistorialArtistas
                    WHERE idUsuario = %s AND idArtista = %s """
        cursor.execute(sql, 
                       (user_id, body.id))

        connection.commit()

        return True
    except Exception as e:
        print(f"Error deleting artist history: {e}")
        if connection:
            connection.rollback()
        return False
    finally:
        if connection:
            dbDesconectar()


def delete_song_history(body):  # noqa: E501
    """Deletes a song from user&#x27;s history.

    Delete an user&#x27;s song history. # noqa: E501

    :param song_id: Song id to delete.
    :type song_id: int

    :rtype: None
    """
    # Verificar autenticación defensiva
    authorized, error_response, token = check_auth(required_scopes=['write:tracks'])
    if not authorized:
        return error_response
    
    if not connexion.request.is_json:
        return Error(code="400", message="Invalid JSON"), 400
    
    user = is_valid_token(token)
    user_id = user.idUsuario
    
    try:
        connection = dbConectar()
        cursor = connection.cursor()

        if connexion.request.is_json:
            body = Identifier.from_dict(connexion.request.get_json())  # noqa: E501

        # Deleting song
        sql = """DELETE FROM HistorialCanciones
                    WHERE idUsuario = %s AND idCancion = %s """
        cursor.execute(sql, 
                       (user_id, body.id))

        connection.commit()
    except Exception as e:
        print(f"Error deleting song history: {e}")
        if connection:
            connection.rollback()
        return False
    finally:
        if connection:
            dbDesconectar()


def get_genre_count():  # noqa: E501
    """Get an user&#x27;s genre count.

    Returns the genre types that an user has listened to and the amount of songs per genre. # noqa: E501


    :rtype: List[UserGenres]
    """
    # Verificar autenticación defensiva
    authorized, error_response, token = check_auth(required_scopes=['write:tracks'])
    if not authorized:
        return error_response
    
    if not connexion.request.is_json:
        return Error(code="400", message="Invalid JSON"), 400
    
    user = is_valid_token(token)
    user_id = user.idUsuario

    try:
        connection = dbConectar()
        cursor = connection.cursor()

       #Getting the songs the user's been listening to
        sql = """SELECT idCancion 
                FROM HistorialCanciones 
                WHERE idUsuario=%s;"""
        cursor.execute(sql, 
                       (user_id,))
        
        song_rows = cursor.fetchall()
        song_ids = [row[0] for row in song_rows]  #song list 4 later

        if not song_ids:
            return []

        # Fetch the genres
        genre_counter = Counter() #util
        genre = []
        for song_id in song_ids:
            try:
                # En el ae hay que poner el endpoint que me de los generos qliaos
                response = requests.get(f"{TYA_SERVER}/song/{song_id}") #TODO
                if response.status_code == 200: #if OK
                    song_data = response.json()
                    genre = song_data.get("genres") or [] 
                    for gen in genre:
                        genre_counter[gen] += 1
            except Exception as e:
                print(f"Error fetching song {song_id}: {e}")

        # Now make the genre list
        result = []
        for genre, count in genre_counter.items():
            result.append(UserGenres(genre=genre, count=count))

        return result
    except Exception as e:
        print(f"Error deleting artist history: {e}")
        if connection:
            connection.rollback()
        return None
    finally:
        if connection:
            dbDesconectar()


def get_user_metrics():  # noqa: E501
    """Get an user&#x27;s metrics.

    Returns an user&#x27;s top artist, top song and total listening time. # noqa: E501


    :rtype: UserMetrics
    """

    # Verificar autenticación defensiva
    authorized, error_response, token = check_auth(required_scopes=['write:tracks'])
    if not authorized:
        return error_response
    
    if not connexion.request.is_json:
        return Error(code="400", message="Invalid JSON"), 400
    
    user = is_valid_token(token)
    user_id = user.idUsuario

    try:
        connection = dbConectar()
        cursor = connection.cursor()

        # Top song
        song_sql = """SELECT idCancion
                    FROM HistorialCanciones
                    WHERE idUsuario = %s
                    ORDER BY escuchas DESC 
                    LIMIT 1; """
        cursor.execute(song_sql, 
                       (user_id,))
    
        top_song_row = cursor.fetchone()
        top_song_id = top_song_row[0] if top_song_row else None #[0] ya que lo toma de SELECT

        # Top artist
        art_sql = """SELECT idArtista
                    FROM HistorialArtistas
                    WHERE idUsuario = %s
                    ORDER BY escuchas DESC 
                    LIMIT 1; """
        cursor.execute(art_sql,
                        (user_id,))
        
        top_artist_row = cursor.fetchone()
        top_artist_id = top_artist_row[0] if top_artist_row else None

        #Total listening time -- maybe...
        listen_sql = """SELECT MIN(fechaPrimera), MAX(fechaUltima)
                            FROM HistorialCanciones
                            WHERE idUsuario = %s;"""
        cursor.execute(listen_sql, 
                       (user_id))
        
        listening_row = cursor.fetchone()
        first_listen = listening_row[0] #fechas, en teoría
        last_listen = listening_row[1]

        if first_listen and last_listen:
            listening_period = (last_listen - first_listen).total_seconds() / 60
        else:
            listening_period = 0

        # Build and return the UserMetrics object
        metrics = UserMetrics(
            listenTime = listening_period,
            topArtistId = top_artist_id,
            topSongId = top_song_id
        )

        return metrics
    except Exception as e:
        print(f"Error deleting artist history: {e}")
        if connection:
            connection.rollback()
        return None
    finally:
        if connection:
            dbDesconectar()



def new_song_history(body):  # noqa: E501
    """Add a song to an user&#x27;s song history.

    Add a song to an existing user&#x27;s song history. # noqa: E501

    :param body: Add a song to an user&#x27;s song history.
    :type body: dict | bytes

    :rtype: None
    """
    # Verificar autenticación defensiva
    authorized, error_response, token = check_auth(required_scopes=['write:tracks'])
    if not authorized:
        return error_response
    
    if not connexion.request.is_json:
        return Error(code="400", message="Invalid JSON"), 400
    
    user = is_valid_token(token)
    user_id = user.idUsuario

    try:
        connection = dbConectar()
        # Schizoid
        cursor = connection.cursor()

        if connexion.request.is_json:
            body = History.from_dict(connexion.request.get_json())  # noqa: E501
        #Insertar en DB
        sql = """INSERT INTO HistorialCanciones (idUsuario, idCancion, fechaPrimera, fechaUltima, escuchas)
                VALUES (%s, %s, %s, NOW(), %s) 
                ON CONFLICT (idUsuario, idCancion)
                DO UPDATE SET escuchas = HistorialCanciones.escuchas + EXCLUDED.escuchas, fechaUltima = NOW();"""
        
        cursor.execute(sql, (
            user_id,
            body.subject_id,
            body.startDate,
            body.playbacks,
        ))

        connection.commit()
        return True
    except Exception as e:
        print(f"Error adding song history: {e}")
        if connection:
            connection.rollback()
        return False
    finally:
        if connection:
            dbDesconectar()


def post_artist_history(body):  # noqa: E501
    """Add an artist to an user&#x27;s history.

    Add an artist to an user&#x27;s history. # noqa: E501

    :param body: Adds an artist to an user&#x27;s history.
    :type body: dict | bytes

    :rtype: None
    """
    # Verificar autenticación defensiva
    authorized, error_response, token = check_auth(required_scopes=['write:tracks'])
    if not authorized:
        return error_response
    
    if not connexion.request.is_json:
        return Error(code="400", message="Invalid JSON"), 400
    
    user = is_valid_token(token)
    user_id = user.idUsuario

    try: 
        connection = dbConectar()
        cursor = connection.cursor()

        if connexion.request.is_json:
            body = History.from_dict(connexion.request.get_json())  # noqa: E501

        #Insertar en DB
        sql = """INSERT INTO HistorialArtistas (idUsuario, idArtista, fechaPrimera, fechaUltima, escuchas)
                VALUES (%s, %s, %s, NOW(), %s) 
                ON CONFLICT (idUsuario, idArtista)
                DO UPDATE SET escuchas = HistorialArtistas.escuchas + EXCLUDED.escuchas, fechaUltima = NOW();"""
        
        cursor.execute(sql, (
            user_id,
            body.subject_id,
            body.startDate,
            body.playbacks,
        ))

        connection.commit()
        return True
    except Exception as e:
        print(f"Error adding artist history: {e}")
        if connection:
            connection.rollback()
        return False
    finally:
        if connection:
            dbDesconectar()
