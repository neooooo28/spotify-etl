

## Introduction to Data Engineering using Spotify API

This personal projects aims to showcase the data engineering skills I've learned by building a data processing pipeline from the ground up. In particular, I aim to get my most recent (last 24 hours) played tracks from [Spotify API](https://developer.spotify.com/console/get-recently-played/?limit=50&after=1484811043508&before=) and automate the ETL process using Airflow.

## Requirements

```
pip install -r requirements.txt
```

## Improvements

Right now this project relies on saving a `.db` file locally, in the future the plan is to incorporate Airflow DAGs to automatically refresh the API tokens and pull data on a daily basis. And then probably level up this entire process by using the AWS Ecosystem (s3, EMR, Redshift)

## Demo

This is a sample view of my played tracks on May 1st 2021, as seen on a SQLite viewer.

![](https://github.com/neooooo28/spotify-etl/blob/main/demo_photos/demo1_sqlite_view.png)
