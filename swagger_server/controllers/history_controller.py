import connexion
import six

from swagger_server.models.error import Error  # noqa: E501
from swagger_server.models.history import History  # noqa: E501
from swagger_server.models.user_genres import UserGenres  # noqa: E501
from swagger_server.models.user_metrics import UserMetrics  # noqa: E501
from swagger_server import util
from swagger_server.dbconx.db_connection import dbConectar, dbDesconectar
from collections import Counter
import requests
from swagger_server.controllers.authorization_controller import is_valid_token


def check_auth(required_scopes=None):
    """
    Verifica autenticación defensiva (backup de Connexion).
    Devuelve (authorized, error_response) tuple.
    """
    token = connexion.request.cookies.get('oversound_auth')
    if not token or not is_valid_token(token):
        error = Error(code="401", message="Unauthorized: Missing or invalid token")
        return False, (error, 401)
    return True, None


def delete_artist_history(artist_id):  # noqa: E501
    """Deletes an artist from user&#x27;s history.

    Delete an user&#x27;s artist history. # noqa: E501

    :param artist_id: Artist id to delete.
    :type artist_id: str

    :rtype: None
    """
    """Add a new track to the database"""

    # Verificar autenticación defensiva
    authorized, error_response = check_auth(required_scopes=['write:tracks'])
    if not authorized:
        return error_response
    
    if not connexion.request.is_json:
        return Error(code="400", message="Invalid JSON"), 400
    
    try:
        connection = dbConectar()
        cursor = connection.cursor()

        # Deleting artist
        sql = """DELETE FROM HistorialCanciones
                    WHERE idUsuario = %s AND idArtista = %s """
        cursor.execute(sql, 
                       (user_id, artist_id))

        connection.commit()
        dbDesconectar()
        return True

    except Exception as e:
        print(f"Error deleting artist history: {e}")
        connection.rollback()
        dbDesconectar()
        return False


def delete_song_history(song_id):  # noqa: E501
    """Deletes a song from user&#x27;s history.

    Delete an user&#x27;s song history. # noqa: E501

    :param song_id: Song id to delete.
    :type song_id: int

    :rtype: None
    """
    # Verificar autenticación defensiva
    authorized, error_response = check_auth(required_scopes=['write:tracks'])
    if not authorized:
        return error_response
    
    if not connexion.request.is_json:
        return Error(code="400", message="Invalid JSON"), 400
    
    try:
        connection = dbConectar()
        cursor = connection.cursor()

        # Deleting song
        sql = """DELETE FROM HistorialCanciones
                    WHERE idUsuario = %s AND idCancion = %s """
        cursor.execute(sql, 
                       (user_id, song_id))

        connection.commit()
        dbDesconectar()
        return True

    except Exception as e:
        print(f"Error deleting song history: {e}")
        connection.rollback()
        dbDesconectar()
        return False


def get_genre_count():  # noqa: E501
    """Get an user&#x27;s genre count.

    Returns the genre types that an user has listened to and the amount of songs per genre. # noqa: E501


    :rtype: List[UserGenres]
    """
    # Verificar autenticación defensiva
    authorized, error_response = check_auth(required_scopes=['write:tracks'])
    if not authorized:
        return error_response
    
    if not connexion.request.is_json:
        return Error(code="400", message="Invalid JSON"), 400
    
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

        dbDesconectar()

        if not song_ids:
            return []

        # Fetch the genres
        genre_counter = Counter() #util
        for song_id in song_ids:
            try:
                # En el ae hay que poner el endpoint que me de los generos qliaos
                response = requests.get(f"http://ae")
                if response.status_code == 200: #if OK
                    song_data = response.json()
                    genre = song_data.get("genre") #(?) TODO genre es una lista uergfgg
                    if genre:
                        genre_counter[genre] += 1
            except Exception as e:
                print(f"Error fetching song {song_id}: {e}")

        # Now make the genre list
        result = []
        for genre, count in genre_counter.items():
            result.append(UserGenres(genre=genre, count=count))

        return result

    except Exception as e:
        connection.rollback()
        dbDesconectar()
        #Si da error...
        return []


def get_user_metrics():  # noqa: E501
    """Get an user&#x27;s metrics.

    Returns an user&#x27;s top artist, top song and total listening time. # noqa: E501


    :rtype: UserMetrics
    """

    # Verificar autenticación defensiva
    authorized, error_response = check_auth(required_scopes=['write:tracks'])
    if not authorized:
        return error_response
    
    if not connexion.request.is_json:
        return Error(code="400", message="Invalid JSON"), 400
    
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
            listening_period = (first_listen - last_listen).days #días
            listening_period = listening_period * 24 *60 #minutos
        else:
            listening_period = 0

        dbDesconectar()

        # Build and return the UserMetrics object
        metrics = UserMetrics(
            listenTime = listening_period,
            topArtistId = top_artist_id,
            topSongId = top_song_id
        )
        return metrics

    except Exception as e:
        print(f"Error getting user metrics: {e}")
        connection.rollback()
        dbDesconectar()
        # Si da error...
        return UserMetrics(top_artist=None, top_song=None, total_listening_time=0)


def new_song_history(body):  # noqa: E501
    """Add a song to an user&#x27;s song history.

    Add a song to an existing user&#x27;s song history. # noqa: E501

    :param body: Add a song to an user&#x27;s song history.
    :type body: dict | bytes

    :rtype: None
    """
    # Verificar autenticación defensiva
    authorized, error_response = check_auth(required_scopes=['write:tracks'])
    if not authorized:
        return error_response
    
    if not connexion.request.is_json:
        return Error(code="400", message="Invalid JSON"), 400
    
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
        #Nosecomohacereluser_id,investigarlo con lupa TODO

        connection.commit()
        dbDesconectar()
        return True
    except Exception as e:
        print(f"Error adding song history: {e}")
        connection.rollback()
        dbDesconectar()
    return False


def post_artist_history(body):  # noqa: E501
    """Add an artist to an user&#x27;s history.

    Add an artist to an user&#x27;s history. # noqa: E501

    :param body: Adds an artist to an user&#x27;s history.
    :type body: dict | bytes

    :rtype: None
    """
    # Verificar autenticación defensiva
    authorized, error_response = check_auth(required_scopes=['write:tracks'])
    if not authorized:
        return error_response
    
    if not connexion.request.is_json:
        return Error(code="400", message="Invalid JSON"), 400
    

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
        #Nosecomohacereluser_id,investigarlo con lupa

        #TODO -- Actualizar artista favorito si es necesario T~T
        connection.commit()
        dbDesconectar()
    except Exception as e:
        print(f"Error adding artist history: {e}")
        connection.rollback()
        dbDesconectar()
    return False
