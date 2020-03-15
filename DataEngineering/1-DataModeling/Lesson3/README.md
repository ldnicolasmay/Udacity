# Database Purpose

Sparkify wants to be able to analyze data they collect about (1) users' interaction with their music streaming app, and (2) the songs users play. Currently, all the data for users' app interaction exists as JSON log files, and the song data exists as JSON metadata.

The purpose of this database is to hold all the relevant data in one place, organized in a fact and dimension star schema, so that analysts can easily query the data.

# Database Schema and ETL Pipeline

As mentioned, the database is organized in a star schema. The fact table of this schema is `songplays` (the target user activity) and the dimension tables are `time` (songplay start time data), `users` (user data), `songs` (song data), and `artists` (arists data).

Data for the `songs` and `artists` tables is parsed from the JSON song metadata files. Data for the `songplays`, `time`, and `users` tables is parsed from the JSON log files.
