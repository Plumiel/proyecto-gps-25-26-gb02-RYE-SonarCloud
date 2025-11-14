import connexion
import six

from swagger_server.models.artist_recommendations import ArtistRecommendations  # noqa: E501
from swagger_server.models.error import Error  # noqa: E501
from swagger_server.models.song_recommendations import SongRecommendations  # noqa: E501
from swagger_server import util
from swagger_server.dbconx.db_connection import dbConectar, dbDesconectar
from collections import Counter
import requests
import random
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


def get_artist_recs():  # noqa: E501
    """Get artist recommendations.

    Returns artist recommendations. # noqa: E501


    :rtype: List[ArtistRecommendations]
    """
    # Verificar autenticación defensiva
    authorized, error_response = check_auth(required_scopes=['write:tracks'])
    if not authorized:
        return error_response
    
    if not connexion.request.is_json:
        return Error(code="400", message="Invalid JSON"), 400
    
    #Pillar el usuario de la sesión... Revisar el historial y la vainita de los generos capah para hacerlo
    #Y luego hay que mirar las putisimas listas de canciones/artistas.

    try:
        connection = dbConectar()
        cursor = connection.cursor()

        # 1. Pick a random artist the user has listened to
        cursor.execute("""
            SELECT idArtista
            FROM HistorialArtistas
            WHERE idUsuario = %s,
            GROUP BY idArtista;
        """, (user_id,))
        artists = cursor.fetchall()
        dbDesconectar()

        if not artists:
            return []

        random_artist_id = random.choice(artists)[0]
        print(f"Random artist: {random_artist_id}")

        # 2. Get songs from that artist via external API...
        try:
            response = requests.get(f"http://songs-service/songs?artistId={random_artist_id}")
            if response.status_code != 200:
                print("Couldn't fetch songs for the chosen artist")
                return []
            artist_songs = response.json()
        except Exception as e:
            print(f"Error fetching songs for artist {random_artist_id}: {e}")
            return []

        if not artist_songs:
            return []

        # 3. Pick a random song from that artist
        random_song = random.choice(artist_songs)
        genres = random_song.get("genres", [])
        if not genres:
            print("⚠️ No genres found for chosen song.")
            return []
        genre_id = genres[0]  # pick first genre
        print(f"🎶 Picked genre ID: {genre_id}")

        # 4. Get first 10 songs from that genre
        try:
            genre_response = requests.get(f"http://songs-service/songs?genreId={genre_id}&limit=10")
            if genre_response.status_code != 200:
                print("⚠️ Couldn't fetch songs for genre")
                return []
            genre_songs = genre_response.json()
        except Exception as e:
            print(f"Error fetching songs for genre {genre_id}: {e}")
            return []

        # 5. Build SongRecommendations list
        recs = [
            SongRecommendations(
                id=song.get("songId"),
                name=song.get("title"),
                image=song.get("cover"),
                genre=song.get("genres", [None])[0]
            )
            for song in genre_songs
        ]

        return recs

    except Exception as e:
        print(f"Error generating artist recommendations: {e}")
        connection.rollback()
        dbDesconectar()
        return []


def get_song_recs():  # noqa: E501
    """Get song recommendations.

    Returns song recommendations. # noqa: E501


    :rtype: List[SongRecommendations]
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

        cursor.execute("""
        SELECT idArtista
        FROM HistorialArtistas
        WHERE idUsuario = %s,
        GROUP BY idArtista;
        """, (user_id,))

        artists = cursor.fetchall()
        if not artists:
            return []  # or handle "no artists" case

        # Pick one random artist ID from the result list
        random_artist_id = random.choice(artists)[0]

        try:
            response = requests.get(f"http://your-api/artists/{song_id}", timeout=5) #pides las canciones del artista cuando este
            if response.status_code == 200:
                data = response.json()
                song_ids=data.get("songs", [])

                if len(song_ids) > 5:
                    random_songs = random.sample(song_ids, 5)
                else:
                    random_songs = song_ids  # if fewer than 3, just take them all
            else:
                random_songs = None           
        except Exception as e:

            dbDesconectar()

        return random_songs #hay que componerlas uerrghhh uerghgfhhf TODO
    except Exception as e:
        return False
    return 'do some magic!'
