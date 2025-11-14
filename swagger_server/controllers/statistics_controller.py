import connexion
import six

from swagger_server.models.artist_metrics import ArtistMetrics  # noqa: E501
from swagger_server.models.artist_recommendations import ArtistRecommendations  # noqa: E501
from swagger_server.models.error import Error  # noqa: E501
from swagger_server.models.song_metrics import SongMetrics  # noqa: E501
from swagger_server.models.song_recommendations import SongRecommendations  # noqa: E501
from swagger_server import util
from swagger_server.dbconx.db_connection import dbConectar, dbDesconectar
from collections import Counter
import requests



def get_artist_metrics(artist_id):  # noqa: E501
    """Get artist metrics by ID.

    Returns the artist&#x27;s all-time playblacks, number of songs, and current popularity placing. # noqa: E501

    :param artist_id: ID of artist.
    :type artist_id: int

    :rtype: ArtistMetrics
    """
    #Pasos:
    #1. Buscar en la base de historial del usuario todas las instancias (historial artista) y sumar la sección de escuchas --> playbacks
    #2. Pedirle al micro de temas y autores las canciones que tengan el ID del artista y contarlas --> number of songs
    #3. Uhm. No sé
    try:
        connection = dbConectar()
        cursor = connection.cursor()

        #Playbacks
        play_sql = """SELECT COALESCE(SUM(escuchas), 0)
                        FROM HistorialArtistas
                        WHERE idArtista = %s;"""

        cursor.execute(play_sql, 
                       (artist_id,))
        playbacks = cursor.fetchone()[0]


        # Popularity 
        # Playbacks de TODOS los artistas
        popu_sql = """
            SELECT idArtista, SUM(escuchas) AS total_playbacks
            FROM HistorialArtistas
            GROUP BY idArtista
            ORDER BY total_playbacks DESC;
        """
        cursor.execute(popu_sql)
        all_artists = cursor.fetchall()  #pilla idArtista, total_playbacks

        # Ranking - busca los artistas y el que coincida, lo asigna
        popularity_rank = None
        for idx, (a_id, _) in enumerate(all_artists, start=1):
            if a_id == artist_id:
                popularity_rank = idx
                break

        dbDesconectar() #no necesitamos mas de la nuestra for now

        #Number of songs
        try:
            # En el ae hay que poner el endpoint que me de los generos qliaos
            response = requests.get(f"http://ae")
            if response.status_code == 200: #if OK
                songs = response.json()
                song_number= len(songs)
        except Exception as e:
            print(f"Error fetching songs from API: {e}")
            song_number = 0

        return ArtistMetrics(
            id = song_id,
            playbacks=playbacks,
            sales=sales,
            downloads=downloads
        )

    except Exception as e:
        print(f"Error getting artist metrics: {e}")
        connection.rollback()
        dbDesconectar()
        return SongMetrics(playbacks=0, sales=0, downloads=0)


def get_song_metrics(song_id):  # noqa: E501
    """Get song metrics by ID.

    Returns the song&#x27;s all-time playblacks, sales and downloads. # noqa: E501

    :param song_id: ID of chosen song.
    :type song_id: int

    :rtype: SongMetrics
    """
    #Pasos:
    #1. Buscar en la base de historial de usuario (historialCanciones) todas las instancias de esta canción --> sales/downloads
    #2. Buscar en la base de historial del usuario todas las instancias y sumar la sección de escuchas --> playbacks
    try:
        connection = dbConectar()
        cursor = connection.cursor()

        #Playbacks
        play_sql = """SELECT COALESCE(SUM(escuchas), 0)
                        FROM HistorialCanciones
                        WHERE idCancion = %s;"""

        cursor.execute(play_sql, 
                       (song_id,))
        playbacks = cursor.fetchone()[0]

        #Sales + downloads
        sale_sql = """SELECT COUNT(*)
                        FROM HistorialCanciones
                        WHERE idCancion = %s;"""
        
        cursor.execute(sale_sql, 
                       (song_id,))
        sales, downloads = cursor.fetchone()[0]

        dbDesconectar()

        return SongMetrics(
            id = song_id,
            playbacks=playbacks,
            sales=sales,
            downloads=downloads
        )

    except Exception as e:
        print(f"Error getting song metrics: {e}")
        connection.rollback()
        dbDesconectar()
        return SongMetrics(playbacks=0, sales=0, downloads=0)


def get_top10_artists():  # noqa: E501
    """Get the top 10 artists.

    Returns the most listened artists. # noqa: E501


    :rtype: List[ArtistRecommendations]
    """
    #Pasos:
    #1. Obtener los artistas con su contador de playbacks --> ordenados
    #2. Cortar los 10 primeros 

    try:
        connection = dbConectar()
        cursor = connection.cursor()

        popu_sql = """
            SELECT idArtista, SUM(escuchas) AS total_playbacks
            FROM HistorialArtistas
            GROUP BY idArtista
            ORDER BY total_playbacks DESC
            LIMIT 10;
            """
        cursor.execute(popu_sql)
        top_artists = cursor.fetchall()  #pilla idArtista, total_playbacks

        dbDesconectar()

        # Ranking - busca los artistas y el que coincida, lo asigna
        top_10 = []

        for idx in top_artists:
            try:
                artist_id = idx[0]
                response = requests.get(f"http://your-api/artists/{artist_id}", timeout=5)
                if response.status_code == 200:
                    data = response.json()
                    top_10.append(
                        ArtistRecommendations(
                            id=data.get("id", artist_id),
                            name=data.get("name", "Unknown Artist"),
                            image=data.get("image", None)
                        )
                    )
                else:
                    top_10.append(ArtistRecommendations(id=artist_id, name="Unknown", image=None))
            except Exception as e:
                print(f"Error fetching artist info: {e}")
                top_10.append(ArtistRecommendations(id=artist_id, name="Unknown", image=None))
                
        return top_10
    except Exception as e:
        print(f"Error getting song metrics: {e}")
        connection.rollback()
        dbDesconectar()
        return []


def get_top10_songs():  # noqa: E501
    """Get the top 10 songs.

    Returns the most listened songs. # noqa: E501


    :rtype: List[SongRecommendations]
    """
    try:
        connection = dbConectar()
        cursor = connection.cursor()

        popu_sql = """
            SELECT idCancion, SUM(escuchas) AS total_playbacks
            FROM HistorialArtistasCanciones
            GROUP BY idCancion
            ORDER BY total_playbacks DESC
            LIMIT 10;
            """
        cursor.execute(popu_sql)
        top_songs = cursor.fetchall()  #pilla idArtista, total_playbacks

        dbDesconectar()

        # Ranking - busca los artistas y el que coincida, lo asigna
        top_10 = []

        for idx in top_songs:
            try:
                song_id = idx[0]
                response = requests.get(f"http://your-api/artists/{song_id}", timeout=5) #TODO cambiar
                if response.status_code == 200:
                    data = response.json()
                    genre_ids=data.get("genres", [])
                    genre_id= genre_ids[0] #no pienso cambiar el swagger otra vez, pillamos solo 1
                    top_10.append(
                        SongRecommendations(
                            id=data.get("songId", song_id),
                            name=data.get("title", "Unknown Artist"),
                            genre= genre_id,
                            image=data.get("cover", None)
                        )
                    )
                else:
                    top_10.append(SongRecommendations(id=song_id, name="Unknown", genre="Unkown", image=None))
            except Exception as e:
                print(f"Error fetching song info: {e}")
                top_10.append(SongRecommendations(id=song_id, name="Unknown", genre="Unkown", image=None))

        return top_10
    except Exception as e:
        print(f"Error getting song metrics: {e}")
        connection.rollback()
        dbDesconectar()
        return []
