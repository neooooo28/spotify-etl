

# Import libraries
from datetime import datetime
import datetime
import json
import requests
import sqlite3
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd 

# Define constants
DB_LOCATION = "sqlite:///played_tracks.db"
USER_ID = "" # Spotify username
TOKEN = "" # Spotify API Token

# Use this link to generate Spotify token: https://developer.spotify.com/console/get-recently-played/


## EXTRACT

def yesterday_unix_ts() -> int:
    """Converts time to unix timestamp in milliseconds"""
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_ts = int(yesterday.timestamp()) * 1000
    return yesterday_unix_ts

def download_songs() -> json:
    """Download songs you've listened to 'after yesterday', 
    meaning all songs in the last 24 hours
    Returns a json file"""
    headers = {
        "Accept": "application/json",
        "Content-Type":"application/json",
        "Authorization":f"Bearer {TOKEN}"
    }
    time = yesterday_unix_ts()
    api_link = f"https://api.spotify.com/v1/me/player/recently-played?"
    r = requests.get(f"{api_link}after={time}", headers=headers)
    data = r.json()
    return data

## TRANSFORM

def clean_data(data_json: json) -> pd.DataFrame:
    """Inputs a json file, returns a pandas dataframe"""
    # Create arrays to contain information
    song_names = []
    artist_names = []
    played_at = []
    timestamp = []

    for song in data_json["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at.append(song["played_at"])
        timestamp.append(song['played_at'][0:10])

    data_df = pd.DataFrame({
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at, 
        "timestamp": timestamp
    }, columns = ["song_name", "artist_name", "played_at", "timestamp"]) 

    return data_df

def validate_data(df: pd.DataFrame) -> bool:
    
    # 1. Check if data is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False

    # 2. Check for primary key
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # 3. Check for nulls
    if df.isnull().values.any():
        raise Exception("NULL values found")

    # # 4. Check that all timestamps are of yesterday's date
    # yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    # yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    # timestamps = df["timestamp"].tolist()
    # for timestamp in timestamps:
    #     if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
    #         raise Exception("At least one song did not have yesterday's timestamp")
    
    return True

## LOAD
def create_sql_engine(data_df):
    """Takes in a pandas dataframe,
    then creates a database to store that data, 
    then appends previous data"""
    engine = sqlalchemy.create_engine(DB_LOCATION, echo=True)
    sql_conn = engine.connect()
    
    # cursor = sql_conn.cursor() 
    # sql_query = """
    # CREATE TABLE IF NOT EXISTS played_tracks(
    #     song_name VARCHAR(200),
    #     artist_name VARCHAR(200),
    #     played_at VARCHAR(200),
    #     timestamp VARCHAR(200),
    #     CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    # )
    # """
    # cursor.execute(sql_query)


    print("SUCCESS: Opened Database")

    try:
        data_df.to_sql(
            'played_tracks', # table name 
            sql_conn,
            index = False, 
            if_exists = 'append'
        )
    except:
        print("Data already exists in database")
    
    sql_conn.close()
    print("SUCCESS: Closed Database")


def main(): 
    data_json = download_songs()     # extract 
    data_df = clean_data(data_json)  # transform

    if validate_data(data_df):       # validate 
        print("Proceed to Load stage")

    create_sql_engine(data_df)       # load

if __name__ == "__main__":
    main()

