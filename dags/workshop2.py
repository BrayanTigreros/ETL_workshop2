from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import pandas as pd
import os

# Paths
CSV_SPOTIFY_PATH = r"/opt/airflow/data/spotify_dataset.csv"
TEMP_S = "/opt/airflow/data/spotify_processing.csv"
TEMP_G = "/opt/airflow/data/grammy_processing.csv"

# CSV output paths for dimensional model
TEMP_DIM_TRACK   = "/opt/airflow/data/dim_track.csv"
TEMP_DIM_ARTIST  = "/opt/airflow/data/dim_artist.csv"
TEMP_DIM_GRAMMY  = "/opt/airflow/data/dim_grammy.csv"
TEMP_DIM_DATE    = "/opt/airflow/data/dim_date.csv"
TEMP_FACT        = "/opt/airflow/data/fact_table.csv"

# Define the DAG using the @dag decorator
@dag(
    dag_id="workshop_2",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "workshop", "mysql", "spotify", "grammy"],
    max_active_runs=1
)
def workshop_2(): 

# Extraction tasks
    @task
    def extract_spotify_dataset():
        df_spotify = pd.read_csv(CSV_SPOTIFY_PATH)

        df_spotify.to_csv(TEMP_S, index=False)
        return TEMP_S

    @task
    def extract_grammy_db():
        hook = MySqlHook(mysql_conn_id="grammy_db")
        df_grammy = hook.get_pandas_df("SELECT * FROM workshop2.grammy")

        df_grammy.to_csv(TEMP_G, index=False)
        return TEMP_G


# Validate input data task
    @task(multiple_outputs=True)
    def validate_input(path_spotify, path_grammy):
        df_spotify = pd.read_csv(path_spotify)
        df_grammy  = pd.read_csv(path_grammy)

        print(f"Spotify Dataset Dimensions:", df_spotify.shape)
        print(f"Grammy DB dimensions:", df_grammy.shape)

        print(f"\nMissing values in Spotify:", df_spotify.isnull().sum().sum())
        print(f"Missing values in Grammy:", df_grammy.isnull().sum().sum())

        cols_key = ['track_name', 'album_name', 'artists', 'track_genre']
        duplicated_songs = df_spotify.duplicated(subset=cols_key, keep='first').sum()
        print(f"\nDuplicated rows in Spotify: {duplicated_songs}")

        duplicated_grammy = df_grammy.duplicated(keep='first').sum()
        print(f"Duplicated rows in Grammy: {duplicated_grammy}")

        return {"spotify": path_spotify, "grammy": path_grammy}


# Cleaning task
    @task(multiple_outputs=True)
    def cleaning_data(path_spotify, path_grammy):
        df_spotify = pd.read_csv(path_spotify)
        df_grammy  = pd.read_csv(path_grammy)

        df_spotify = df_spotify.drop_duplicates(subset=['track_id', 'track_genre'], keep='first')
        df_spotify = df_spotify.drop_duplicates(subset=['track_genre', 'artists', 'album_name', 'track_name'], keep='first')

        df_spotify = df_spotify.dropna() 
        df_grammy = df_grammy.fillna("Unknown")

        df_spotify = df_spotify.rename(columns={'Unnamed: 0': 'id'})
        df_spotify = df_spotify.rename(columns={'track_genre': 'gender'})

        # Sobrescribimos el archivo con la data limpia usando la ruta que entró
        df_spotify.to_csv(path_spotify, index=False)
        df_grammy.to_csv(path_grammy, index=False)
        
        return {"spotify": path_spotify, "grammy": path_grammy}


# Validate output data task
    @task(multiple_outputs=True)
    def validate_output(path_spotify, path_grammy):
        df_spotify = pd.read_csv(path_spotify)
        df_grammy  = pd.read_csv(path_grammy)

        print(f"Spotify Dataset Dimensions:", df_spotify.shape)
        print(f"Grammy DB dimensions:", df_grammy.shape)

        print(f"\nMissing values in Spotify:", df_spotify.isnull().sum().sum())
        print(f"Missing values in Grammy:", df_grammy.isnull().sum().sum())

        cols_key = ['track_name', 'album_name', 'artists', 'gender']

        duplicated_songs = df_spotify.duplicated(subset=cols_key, keep='first').sum()
        print(f"\nDuplicated rows in Spotify: {duplicated_songs}")

        duplicated_grammy = df_grammy.duplicated(keep='first').sum()
        print(f"Duplicated rows in Grammy: {duplicated_grammy}")

        return {"spotify": path_spotify, "grammy": path_grammy}


# Transform and Merge
    @task(multiple_outputs=True)
    def transform_and_merge(path_spotify, path_grammy):
        df_spotify = pd.read_csv(path_spotify)
        df_grammy  = pd.read_csv(path_grammy)

        df_enriquecido = pd.merge(
            df_spotify, 
            df_grammy, 
            left_on='artists',  # Spotify
            right_on='artist',  # Grammy
            how='left'          # Maintain all Spotify records
        )

        print(f"Enriched dataset dimensions: {df_enriquecido.shape}")
        print(df_enriquecido.head(3))

    # Dimensional Model
        # Track dim
        dim_track = df_enriquecido[['track_id', 'track_name', 'album_name', 'gender']].drop_duplicates().reset_index(drop=True)
        dim_track['track_key'] = dim_track.index + 1

        # Artist dim
        dim_artist = df_enriquecido[['artists', 'workers', 'img']].drop_duplicates().reset_index(drop=True)
        dim_artist['artist_key'] = dim_artist.index + 1

        # Grammy dim
        dim_grammy = df_enriquecido[['title', 'category', 'winner']].drop_duplicates().reset_index(drop=True)
        dim_grammy['grammy_key'] = dim_grammy.index + 1

        # Date dim
        dim_date = df_enriquecido[['year', 'published_at', 'updated_at']].drop_duplicates().reset_index(drop=True)
        dim_date['date_key'] = dim_date.index + 1

    # Fact table
        fact_table = df_enriquecido.merge(dim_track, on=['track_id', 'track_name', 'album_name', 'gender'], how='left')
        fact_table = fact_table.merge(dim_artist, on=['artists', 'workers', 'img'], how='left')
        fact_table = fact_table.merge(dim_grammy, on=['title', 'category', 'winner'], how='left')
        fact_table = fact_table.merge(dim_date, on=['year', 'published_at', 'updated_at'], how='left')

        fact_table = fact_table[['id', 'track_key', 'artist_key', 'grammy_key', 'date_key', 'popularity', 'duration_ms']]

        print("Dimensional model created successfully")
        print(fact_table.head(3))

        return {
            "dim_track":  dim_track,
            "dim_artist": dim_artist,
            "dim_grammy": dim_grammy,
            "dim_date":   dim_date,
            "fact":       fact_table
        }
    
# Load to local csv
    @task
    def load_to_csv(data_dict):
        # dimensions and fact table
        dim_track  = data_dict["dim_track"]
        dim_artist = data_dict["dim_artist"]
        dim_grammy = data_dict["dim_grammy"]
        dim_date   = data_dict["dim_date"]
        fact_table = data_dict["fact"]

        # Load local to csv
        dim_track.to_csv(TEMP_DIM_TRACK, index=False)
        dim_artist.to_csv(TEMP_DIM_ARTIST, index=False)
        dim_grammy.to_csv(TEMP_DIM_GRAMMY, index=False)
        dim_date.to_csv(TEMP_DIM_DATE, index=False)
        fact_table.to_csv(TEMP_FACT, index=False)

        print("DataFrames saved to CSV successfully")


# Load to DW
    @task
    def load_to_dw(dim_track, dim_artist, dim_grammy, dim_date, df_fact):

        from sqlalchemy import create_engine
        import pandas as pd

        host = "mysql"
        port = 3306
        user = "airflow"
        password = "airflow"
        database = "workshop2"

        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)

        track_to_load  = dim_track.drop(columns=['track_key'])
        artist_to_load = dim_artist.drop(columns=['artist_key'])
        grammy_to_load = dim_grammy.drop(columns=['grammy_key'])
        date_to_load   = dim_date.drop(columns=['date_key'])

        track_to_load.to_sql("dim_track", engine, if_exists="append", index=False)
        artist_to_load.to_sql("dim_artist", engine, if_exists="append", index=False)
        grammy_to_load.to_sql("dim_grammy", engine, if_exists="append", index=False)
        date_to_load.to_sql("dim_date", engine, if_exists="append", index=False)

        db_track  = pd.read_sql("SELECT track_key, track_id, track_name, album_name, gender FROM dim_track", engine)
        db_artist = pd.read_sql("SELECT artist_key, artists, workers, img FROM dim_artist", engine)
        db_grammy = pd.read_sql("SELECT grammy_key, title, category, winner FROM dim_grammy", engine)
        db_date   = pd.read_sql("SELECT date_key, year, published_at, updated_at FROM dim_date", engine)

        map_track  = dict(zip(dim_track['track_key'], db_track['track_key']))
        map_artist = dict(zip(dim_artist['artist_key'], db_artist['artist_key']))
        map_grammy = dict(zip(dim_grammy['grammy_key'], db_grammy['grammy_key']))
        map_date   = dict(zip(dim_date['date_key'], db_date['date_key']))

        fact_final = df_fact.copy()
        fact_final['track_key']  = fact_final['track_key'].map(map_track)
        fact_final['artist_key'] = fact_final['artist_key'].map(map_artist)
        fact_final['grammy_key'] = fact_final['grammy_key'].map(map_grammy)
        fact_final['date_key']   = fact_final['date_key'].map(map_date)

        fact_final = fact_final[['track_key','artist_key','grammy_key','date_key','popularity','duration_ms']]

        fact_final.to_sql("fact_table", engine, if_exists="append", index=False)

        print("✅ DW loaded correctly with dimensions and fact table")


# Define task execution order
    spotify_path = extract_spotify_dataset()
    grammy_path  = extract_grammy_db()
    
    val_input = validate_input(spotify_path, grammy_path)

    clean_paths = cleaning_data(val_input["spotify"], val_input["grammy"])
    
    val_output = validate_output(clean_paths["spotify"], clean_paths["grammy"])
    
    transform = transform_and_merge(val_output["spotify"], val_output["grammy"])

    load_csv = load_to_csv(transform)

    load_dw = load_to_dw(
        transform["dim_track"],
        transform["dim_artist"],
        transform["dim_grammy"],
        transform["dim_date"],
        transform["fact"]
    )   


workshop_2()