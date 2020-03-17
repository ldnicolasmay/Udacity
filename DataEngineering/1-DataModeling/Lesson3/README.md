# Database Purpose

Sparkify wants to be able to analyze data they collect about (1) users' interaction with their music streaming app, and (2) the songs users play. Currently, all the data for users' app interaction exists as JSON log files, and the song data exists as JSON metadata.

The purpose of this database is to hold all the relevant data in one place, organized in a fact and dimension star schema, so that analysts can easily query the data.

# Database Schema and ETL Pipeline

## Database Schema

As mentioned, the database is organized in a star schema. The fact table of this schema is `songplays` (the target user activity) and the dimension tables are `time` (songplay start time data), `users` (user data), `songs` (song data), and `artists` (arists data).

## ETL Pipeline

In short, data for the `songs` and `artists` tables is parsed from the JSON song metadata files. Data for the `time`, and `users` tables is parsed from the JSON log files. Data for the `songplays` table is parsed from the JSON log files as well as from derived data in the `songs` and `artists` tables.

Because the source of the `songs` and `artists` tables are the song metadata files, they a processed similarly using the `process_song_file` in `etl.py`. The only difference between the two is which fields are selected from the JSON files to include in either table. The `songs` table includes the fields `song_id`, `title`, `artist_id`, `year`, `duration`. The `artists` table includes the fields `artist_id`, `artist_name`, `artist_location`, `artist_latitude`, `artist_longitude`.

As mentioned, the source for the `songplays`, `time`, and `users` tables is the log files. The `time` and `users` tables are relatively simple  in their creation (like the `songs` and `artists` tables). Desired fields are selected for each table. The `time` table includes a user activity timestamp, `start_time`, from which other fields are derived: `hour`, `day`, `week`, `month`, `year`, `weekday`. The `users` table includes the fields `userId`, `firstName`, `lastName`, `gender`, `level`. 

The `songplays` table is derived from the log files but also the `songs` and `artists` tables. This requires select fields from the log files `ts` (timestamp), `user_id`, `level`, `session_id`, `location`, `user_agent`. In addition, the fields `song_id` and `artist_id` are selected from a table joined between the `songs` and `artists` tables.

# Example Query

Count the number of songplays for each `song_id` in the `songplays` table.

The query `SELECT song_id, COUNT(*) FROM songplays GROUP BY song_id;` yields the following:

| song_id            | count |
|:-------------------|------:|
| None               |  6891 |
| SOZCTXZ12AB0182364 |     1 |
