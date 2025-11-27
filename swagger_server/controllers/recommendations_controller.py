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

TYA_SERVER = 'http://localhost:8081'

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

def safe_get(url, timeout=10): #Method for json fetching - Aumentado timeout a 10s
    try:
        print(f"DEBUG: Requesting URL: {url}")
        r = requests.get(url, timeout=timeout)
        print(f"DEBUG: Response status code: {r.status_code}")
        print(f"DEBUG: Response content preview: {r.text[:200] if r.text else 'Empty'}")
        
        if r.status_code == 200:
            try:
                json_data = r.json()
                print(f"DEBUG: Successfully parsed JSON, type: {type(json_data)}, length: {len(json_data) if isinstance(json_data, (list, dict)) else 'N/A'}")
                return json_data
            except ValueError as e:
                print(f"DEBUG: JSON parsing error: {e}")
                return None
        else:
            print(f"DEBUG: Non-200 status code received: {r.status_code}")
            return None
    except requests.exceptions.Timeout as e:
        print(f"DEBUG: Request timeout: {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"DEBUG: Request exception: {e}")
        return None
    except Exception as e:
        print(f"DEBUG: Unexpected error in safe_get: {e}")
        return None

def get_artist_recs():  # noqa: E501
    """Get artist recommendations.

    Returns artist recommendations. # noqa: E501


    :rtype: List[ArtistRecommendations]
    """
    print("DEBUG: Starting get_artist_recs")

    # Verificar autenticación defensiva
    print("DEBUG: Checking authentication")
    authorized, error_response, token = check_auth(required_scopes=['read'])
    if not authorized:
        print("DEBUG: Authentication failed")
        return error_response
    
    # if not connexion.request.is_json:
    #     return Error(code="400", message="Invalid JSON"), 400
    
    user = is_valid_token(token)
    user_id = user["userId"]
    print(f"DEBUG: User ID: {user_id}")

    try:
        print("DEBUG: Connecting to database")
        connection = dbConectar()
        cursor = connection.cursor()

        print("DEBUG: Fetching user's artist history")
        cursor.execute("""
        SELECT idArtista
        FROM HistorialArtistas
        WHERE idUsuario = %s
        GROUP BY idArtista;
        """, (user_id,))

        artists = cursor.fetchall()
        print(f"DEBUG: Found {len(artists)} artists in history")
        if not artists:
            print("DEBUG: No artists found, returning empty list")
            return []  # or handle "no artists" case

        # Pick one random artist ID from the result list
        random_artist_id = random.choice(artists)[0] #Picks a random artist from the user's story
        print(f"DEBUG: Selected random artist ID: {random_artist_id}")


        print(f"DEBUG: Fetching artist data from API: {TYA_SERVER}/artist/{random_artist_id}")
        data = safe_get(f"{TYA_SERVER}/artist/{random_artist_id}")
        if not data:
            print("DEBUG: Failed to fetch artist data, returning empty list")
            return []
        
        song_ids=data.get("owner_songs", []) #Gets their songs' ids
        print(f"DEBUG: Raw song_ids type and content: {type(song_ids)}, {song_ids[:3] if song_ids else 'empty'}")
        
        # If song_ids contains tuples, extract the first element
        if song_ids and isinstance(song_ids[0], (tuple, list)):
            song_ids = [s[0] if isinstance(s, (tuple, list)) else s for s in song_ids]
            print(f"DEBUG: Extracted IDs from tuples: {song_ids[:3]}")
        
        random_songs = random.sample(song_ids, min(5, len(song_ids)))
        print(f"DEBUG: Selected {len(random_songs)} random songs from artist: {random_songs}")

        genre_list = [] #make a list for the genres
        # for song in random_songs:
        #     print(f"DEBUG: Fetching song data for genre: {TYA_SERVER}/song/{song}")
        #     data = safe_get(f"{TYA_SERVER}/song/{song}")
        #     if not data:
        #         continue
        #     songs = data.get("genres", []) #get their genres
        #     if not songs:
        #         continue 
        #     genre_list.append(songs[0]) #put it in a list
        #     print(f"DEBUG: Added genre {songs[0]}")
        # -------------------------------------------------------------------------------------
        print(f"DEBUG: Fetching song data in list mode for genres")
        data = safe_get(f"{TYA_SERVER}/song/list?ids={','.join(map(str, random_songs))}")
        # convert json response to list of dicts
        if not data:
            print("DEBUG: Failed to fetch song list data, returning empty list")
            return []
        
        for song in data:
            songs = song.get("genres", []) #get their genres
            if not songs:
                continue 
            genre_list.append(songs[0]) #put it in a list
            print(f"DEBUG: Added genre {songs[0]}")
        # -------------------------------------------------------------------------------------
        print(f"DEBUG: Collected genres: {genre_list}")
        # Limitar a los primeros 3 géneros para evitar timeouts
        genre_list = genre_list[:3]
        print(f"DEBUG: Limited to first 3 genres: {genre_list}")
        #We have a buncha genres
        canciones = []
        for g in genre_list:
            try:
                print(f"DEBUG: Filtering songs by genre {g}")
                song_resp = requests.get(f"{TYA_SERVER}/song/filter", 
                                        params={"genres": g},
                                        timeout=5,
                                        headers={"Accept": "application/json"}
                                        )
                song_resp.raise_for_status()
                song_ids = song_resp.json() or []
                print(f"DEBUG: Found {len(song_ids)} songs for genre {g}")
                print(f"DEBUG: song_ids type and sample: {type(song_ids)}, {song_ids[:2] if song_ids else 'empty'}")
                
                # If song_ids contains nested structures, flatten them
                if song_ids and isinstance(song_ids[0], (tuple, list)):
                    song_ids = [s[0] if isinstance(s, (tuple, list)) else s for s in song_ids]
                    print(f"DEBUG: Flattened song_ids: {song_ids[:2]}")
            except Exception as e:
                print(f"DEBUG: Error calling genre filtering for {g}: {e}")
                continue 

            if not song_ids:
                continue

            sampled = random.sample(song_ids, min(2, len(song_ids)))  # Reducido de 3 a 2 para mejorar rendimiento
            canciones.extend(sampled)
            print(f"DEBUG: Added {len(sampled)} songs from genre {g}")
        
        print(f"DEBUG: Total songs collected: {len(canciones)}")
        print(f"DEBUG: Sample canciones: {canciones[:5] if canciones else 'empty'}")
        
        if not canciones:
            print("DEBUG: No songs to fetch, returning empty list")
            return []
        
        #Now we got a buncha song ids from each genre
        artist_list = set()
        data = safe_get(f"{TYA_SERVER}/song/list?ids={','.join(map(str, canciones))}")
        if not data:
            print("DEBUG: Failed to fetch song list data, returning empty list")
            return []
        
        print(f"DEBUG: Received {len(data)} songs from song/list endpoint")
        print(f"DEBUG: Sample song data: {data[0] if data else 'empty'}")
        
        # for i in data:
        #     print(f"DEBUG: Fetching artist ID for song {i}")
        #     data = safe_get(f"{TYA_SERVER}/song/{i}")
        #     if not data:
        #         continue
        #     artist_list.add(i.get("artistId")) #we look for songs and then get the artists 's ids
        #     print(f"DEBUG: Added artist ID {data.get('artistId')}")

                #Now we got a buncha song ids from each genre
        for i in data:
            artist_id = i.get("artistId")
            if artist_id is not None:  # Filter out None values
                artist_list.add(artist_id)
                print(f"DEBUG: Added artist ID {artist_id}")
            else:
                print(f"DEBUG: Song {i.get('id', 'unknown')} has no artistId")

        
        print(f"DEBUG: Unique artists found: {len(artist_list)}")
        print(f"DEBUG: Artist list: {list(artist_list)[:10]}")
        
        if not artist_list:
            print("DEBUG: No artists to fetch, returning empty list")
            return []
        
        #now that we have the fucking artist id list, we get their info and pop it in the last list that we'll return
        recs = []
        data = safe_get(f"{TYA_SERVER}/artist/list?ids={','.join(map(str, artist_list))}")
        if not data:    
            print("DEBUG: Failed to fetch artist list data, returning empty list")
            return []
        
        print(f"DEBUG: Received {len(data)} artists from artist/list endpoint")
        print(f"DEBUG: Sample artist data: {data[0] if data else 'empty'}")
        
        for a in data:
            # El endpoint devuelve un objeto con datos de usuario + artistId
            artist_id = a.get("artistId", 0)
            # Usar 'username' o 'name' del usuario como nombre del artista
            artist_name = a.get("username", a.get("name", "Unknown Artist"))
            artist_image = a.get("image", None)  # 'image' en lugar de 'artisticImage'
            
            print(f"DEBUG: Processing artist - ID: {artist_id}, Name: {artist_name}")
            
            if artist_id:  # Solo agregar si tiene artistId válido
                recs.append(
                    ArtistRecommendations (
                        id = artist_id,
                        name = artist_name,
                        image = artist_image
                    )
                )
                print(f"DEBUG: Added recommendation for artist {artist_name} (ID: {artist_id})")
            else:
                print(f"DEBUG: Skipping entry without valid artistId: {a}")

        print(f"DEBUG: Returning {len(recs)} artist recommendations")
        return recs

    except Exception as e:
        print(f"Error getting artist recommendations: {e}")
        if connection:
            connection.rollback()
        return Error(code="500", message="Internal server error"), 500

    finally:
        if connection:
            print("DEBUG: Disconnecting from database")
            dbDesconectar(connection)


def get_song_recs():  # noqa: E501
    """Get song recommendations.

    Returns song recommendations. # noqa: E501


    :rtype: List[SongRecommendations]
    """
    print("DEBUG: Starting get_song_recs")

    # Verificar autenticación defensiva
    print("DEBUG: Checking authentication")
    authorized, error_response, token = check_auth(required_scopes=['read'])
    if not authorized:
        print("DEBUG: Authentication failed")
        return error_response
    
    # if not connexion.request.is_json:
    #     return Error(code="400", message="Invalid JSON"), 400
    
    user = is_valid_token(token)
    user_id = user["userId"]
    print(f"DEBUG: User ID: {user_id}")

    try:
        print("DEBUG: Connecting to database")
        connection = dbConectar()
        cursor = connection.cursor()

        print("DEBUG: Fetching user's song history")
        cursor.execute("""
        SELECT idCancion
        FROM HistorialCanciones
        WHERE idUsuario = %s
        GROUP BY idCancion;
        """, (user_id,))

        db_songs = cursor.fetchall()
        print(f"DEBUG: Found {len(db_songs)} songs in history")
        if not db_songs:
            print("DEBUG: No songs found, returning empty list")
            return [] 

        # db_songs is a list of tuples, e.g. [(12,), (55,), (102,)]
        random_songs = random.sample(db_songs, min(5, len(db_songs)))  # pick up to 3
        random_song_ids = [s[0] for s in random_songs]  # extract the IDs from tuples
        print(f"DEBUG: Selected {len(random_song_ids)} random songs")

        genre_list = set() #make a list for the genres, non-repeated
        # for song in random_song_ids:
        #     print(f"DEBUG: Fetching genres for song {song}")
        #     data = safe_get(f"{TYA_SERVER}/song/{song}")
        #     if not data:
        #         continue
        #     songs = data.get("genres", []) #get their genres
        #     if not songs:
        #         continue 
        #     genre_list.add(songs[0]) #put it in a list
        #     print(f"DEBUG: Added genre {songs[0]}")
        # -------------------------------------------------------------------------------------
        print(f"DEBUG: Fetching song data in list mode for genres")
        data = safe_get(f"{TYA_SERVER}/song/list?ids={','.join(map(str, random_song_ids))}")
        # convert json response to list of dicts
        if not data:
            print("DEBUG: Failed to fetch song list data, returning empty list")
            return []
        
        for song in data:
            songs = song.get("genres", []) #get their genres
            if not songs:
                continue 
            genre_list.add(songs[0]) #put it in a list
            print(f"DEBUG: Added genre {songs[0]}")
        # -------------------------------------------------------------------------------------
        print(f"DEBUG: Collected unique genres: {genre_list}")
        # Limitar a los primeros 3 géneros para evitar timeouts (convertir set a lista para slicing)
        genre_list = list(genre_list)[:3]
        print(f"DEBUG: Limited to first 3 genres: {genre_list}")
        #We have a buncha genres
        canciones = []
        for g in genre_list:
            try:
                print(f"DEBUG: Filtering songs by genre {g}")
                song_resp = requests.get(f"{TYA_SERVER}/song/filter",
                                        params={"genres": g},
                                        timeout=5,
                                        headers={"Accept": "application/json"}
                                        )
                song_resp.raise_for_status()
                song_ids = song_resp.json() or []
                print(f"DEBUG: Found {len(song_ids)} songs for genre {g}")
            except Exception as e:
                print("Error calling genre filtering", e)
                continue 

            if not song_ids:
                continue

            sampled = random.sample(song_ids, min(2, len(song_ids)))  # Reducido de 3 a 2 para mejorar rendimiento
            canciones.extend(sampled)
            print(f"DEBUG: Added {len(sampled)} songs from genre {g}")

        print(f"DEBUG: Total songs collected: {len(canciones)}")
        #now that we have the fucking song id list, we get their info and pop it in the last list that we'll return
        recs = []
        # for a in canciones:
        #     print(f"DEBUG: Fetching song info for ID {a}")
        #     data = safe_get(f"{TYA_SERVER}/song/{a}")
        #     if not data:
        #         continue
        #     genres = data.get("genres", [])
        #     if not genres:
        #         continue  # or handle missing genres
        #     singular_genre = genres[0]
        #     recs.append(
        #         SongRecommendations (
        #             id = a,
        #             name = data.get("title", "Jane Doe"),
        #             genre = singular_genre,
        #             image = data.get("cover", None)
        #         )
        #     )
        #     print(f"DEBUG: Added recommendation for song {a}")
        # -------------------------------------------------------------------------------------
        print(f"DEBUG: Fetching song info for collected song IDs")
        data = safe_get(f"{TYA_SERVER}/song/list?ids={','.join(map(str, canciones))}")
        if not data:    
            print("DEBUG: Failed to fetch song list data, returning empty list")
            return []
        
        for song in data:
            genres = song.get("genres", [])
            if not genres:
                continue  # or handle missing genres
            singular_genre = genres[0]
            recs.append(
                SongRecommendations (
                    id = song.get("id"),
                    name = song.get("title", "Jane Doe"),
                    genre = singular_genre,
                    image = song.get("cover", None)
                )
            )
            print(f"DEBUG: Added recommendation for song {song.get('id')}")
        # -------------------------------------------------------------------------------------
        print(f"DEBUG: Returning {len(recs)} song recommendations")
        return recs

    except Exception as e:
        print(f"Error getting song recommendations: {e}")
        if connection:
            connection.rollback()
        return Error(code="500", message="Internal server error"), 500

    finally:
        if connection:
            print("DEBUG: Disconnecting from database")
            dbDesconectar(connection)
