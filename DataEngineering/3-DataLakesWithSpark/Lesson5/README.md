# Sparkify Data Lake


## Data Lake Purpose

Sparkify has a growing user base, and therefore it's data is growing. It wants to move its data and ETL processes entirely to the cloud.

Sparkify will store its raw data about (1) users' interaction with their music streaming app, and (2) the songs users play in AWS S3 buckets. Currently, all the data for users' app interactions exists as JSON log files, and the song data exists as JSON metadata. This project loads the raw data from S3 buckets into an Amazon EMR cluster wherein Spark ETLs the data using a star schema design into another S3 bucket.

The purpose of this data lake is to hold all the relevant data in one place, organized in a fact-and-dimension star schema, so that analysts and business intelligence users can easily query the data in order to learn about what songs users are listening to.


## Data Lake Design

### Schema

The data warehouse data is organized into a fact-and-dimension star schema. The fact table of this scheme is `songplays_table` (the target user activity), and the dimension tables are `songs_table` (song data), `artists_table` (artist data), `users_table` (user data), and `time_table` (songplay start time data).

![songplay star schema](img/songplay_star_schema_2.svg "songplay star schema")

### ETL Pipeline

The `etl.py` script does all the heavy lifting. 

#### Star Schema

The `etl.py` script user PySpark to (1) load the raw JSON data into the staging dataframes `song_df` and `log_df`, and (2) insert data into the fact table (`songplays_table`) and dimension tables of the star schema (`songs_table`, `artists_table`, `users_table`, `time_table`).

Data for this star schema is sourced from raw JSON files in an S3 bucket. Data for the `songs_table` and `artists_table` is parsed from the JSON song metadata files. Data for the `users_table` and `time_table` is parsed from the JSON log files. Data for the `songplays_table` is parsed from the raw JSON log data and the `songs_table`.

#### Selected Fields

Because the source of the `song_table` and `artist_table` is the song metadata files, they are processed similarly from the `song_df`. The only difference between the two is which fields are selected. The `songs_table` is comprised of `song_id`, `title`, `artist_id`, `artist_name`, `year`, and `duration`. The `artists_table` is comprised of `artist_id`, `artist_name`, `artist_location`, `artist_latitude`, and `artist_longitude`.

The source of the `users_table` is the `log_df`. The `users_table` is comprised of `user_id`, `first_name`, `last_name`, `gender`, and `level`. 

The source of the `time_table` is also the `log_df`, but it needs more work. The `log_df` contains a `ts` Unix epoch timestamp column (of type Long). This has to be transformed to a column with a data type of Timestamp before it can be loaded into the `time_table`. Once `ts` (Long) is transformed to `start_time` (Timestamp), the `start_time` column can be selected into the `time_table`. Then the `start_time` column can be used to derive additional columns in `time_table`: `hour`, `day`, `week`, `month`, `year`, and `weekday`.

Loading data into the `songplays_table` relies on a join between the `log_df` stating dataframe and `artists_table` on the columns `title`/`song`, `artist_name`/`artist`, and `duration`/`length`.


