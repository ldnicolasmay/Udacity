import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table;"
user_table_drop = "DROP TABLE IF EXISTS user_table;"
song_table_drop = "DROP TABLE IF EXISTS song_table;"
artist_table_drop = "DROP TABLE IF EXISTS artist_table;"
time_table_drop = "DROP TABLE IF EXISTS time_table;"

# CREATE TABLES

staging_events_table_create = (
    """
    CREATE TABLE staging_events_table (
      id             INTEGER  IDENTITY(0,1)  PRIMARY KEY,
      artist         VARCHAR,
      auth           VARCHAR,
      firstName      VARCHAR,
      gender         VARCHAR(1),
      itemInSession  INTEGER,
      lastName       VARCHAR,
      length         DECIMAL,
      level          VARCHAR,
      location       VARCHAR,
      method         VARCHAR(6),
      page           VARCHAR,
      registration   VARCHAR,
      sessionId      INTEGER,
      song           VARCHAR,
      status         INTEGER,
      ts             BIGINT,
      userAgent      VARCHAR,
      userId         INTEGER
    );
    """
)

staging_songs_table_create = (
    """
    CREATE TABLE staging_songs_table (
      id                INTEGER  IDENTITY(0,1)  PRIMARY KEY,
      num_songs         INTEGER,
      artist_id         VARCHAR,
      artist_latitude   NUMERIC,
      artist_longitude  NUMERIC,
      artist_location   VARCHAR(512),
      artist_name       VARCHAR(512),
      song_id           VARCHAR,
      title             VARCHAR,
      duration          NUMERIC,
      year              INTEGER
    );
    """
)

user_table_create = (
    """
    CREATE TABLE user_table (
      user_id     INTEGER  PRIMARY KEY  SORTKEY,
      first_name  VARCHAR,
      last_name   VARCHAR,
      gender      VARCHAR,
      level       VARCHAR
    ) DISTSTYLE ALL;
    """
)

song_table_create = (
    """
    CREATE TABLE song_table (
      song_id    VARCHAR  PRIMARY KEY  SORTKEY  DISTKEY,
      title      VARCHAR,
      artist_id  VARCHAR,
      year       INTEGER,
      duration   INTEGER
    );
    """
)

artist_table_create = (
    """
    CREATE TABLE artist_table (
      artist_id  VARCHAR  PRIMARY KEY  SORTKEY,
      name       VARCHAR,
      location   VARCHAR,
      latitude   DECIMAL,
      longitude  DECIMAL
    ) DISTSTYLE ALL;
    """
)

time_table_create = (
    """
    CREATE TABLE time_table (
      start_time  TIMESTAMP  PRIMARY KEY  SORTKEY,
      hour        INTEGER,
      day         INTEGER,
      week        INTEGER,
      month       INTEGER,
      year        INTEGER,
      weekday     INTEGER
    ) DISTSTYLE ALL;
    """
)

songplay_table_create = (
    """
    CREATE TABLE songplay_table (
      songplay_id  INTEGER    IDENTITY(0,1)                      PRIMARY KEY,
      start_time   TIMESTAMP  REFERENCES time_table(start_time)  SORTKEY,
      user_id      INTEGER    REFERENCES user_table(user_id),
      level        VARCHAR,
      song_id      VARCHAR    REFERENCES song_table(song_id)     DISTKEY,
      artist_id    VARCHAR    REFERENCES artist_table(artist_id),
      session_id   INTEGER,
      location     VARCHAR,
      user_agent   VARCHAR
    );
    """
)

# STAGING TABLES

staging_events_copy = (
    """
    COPY staging_events_table
    FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    JSON AS '{}'
    REGION 'us-west-2';
    """
).format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = (
    """
    COPY staging_songs_table
    FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    JSON AS 'auto'
    REGION 'us-west-2';
    """
).format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))
    
# FINAL TABLES

songplay_table_insert = (
    """
    INSERT INTO songplay_table
    (
      start_time,
      user_id,
      level,
      song_id,
      artist_id,
      session_id,
      location,
      user_agent
    )
    SELECT DISTINCT
      DATE_ADD('ms', ev.ts, '1970-01-01') AS start_time,
      ev.userid,
      ev.level,
      so.song_id,
      so.artist_id,
      ev.sessionid,
      ev.location,
      ev.useragent
    FROM staging_events_table AS ev
    JOIN staging_songs_table  AS so
    ON 
      ev.artist = so.artist_name AND
      ev.song   = so.title       AND
      ev.length = so.duration
    WHERE
      ev.page = 'NextSong';
    """
)

user_table_insert = (
    """
    INSERT INTO user_table (
      user_id,
      first_name,
      last_name,
      gender,
      level
    )
    SELECT DISTINCT
      userid, 
      firstname, 
      lastname, 
      gender, 
      level
    FROM staging_events_table
    WHERE
      page = 'NextSong' AND
      userid IS NOT NULL;
    """
)

song_table_insert = (
    """
    INSERT INTO song_table (
      song_id,
      title,
      artist_id,
      year,
      duration
    )
    SELECT DISTINCT
      song_id,
      title,
      artist_id,
      year,
      duration
    FROM staging_songs_table
    WHERE song_id IS NOT NULL;
    """
)

artist_table_insert = (
    """
    INSERT INTO artist_table (
      artist_id,
      name,
      location,
      latitude,
      longitude
    )
    SELECT DISTINCT
      artist_id,
      artist_name,
      artist_location,
      artist_latitude,
      artist_longitude
    FROM staging_songs_table
    WHERE artist_id IS NOT NULL;
    """
)

time_table_insert = (
    """
    INSERT INTO time_table (
      start_time,
      hour,
      day,
      week,
      month,
      year,
      weekday
    ) 
    SELECT
      start_time,
      EXTRACT(HOUR  FROM start_time) AS hour,
      EXTRACT(DAY   FROM start_time) AS day,
      EXTRACT(WEEK  FROM start_time) AS week,
      EXTRACT(MONTH FROM start_time) AS month,
      EXTRACT(YEAR  FROM start_time) AS year,
      EXTRACT(DOW   FROM start_time) AS weekday
    FROM songplay_table
    WHERE start_time IS NOT NULL;
    """
)

# QUERY LISTS

create_table_queries = [staging_events_table_create, 
                        staging_songs_table_create, 
                        user_table_create, 
                        song_table_create, 
                        artist_table_create, 
                        time_table_create,
                        songplay_table_create]

drop_table_queries = [staging_events_table_drop, 
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop, 
                      song_table_drop, 
                      artist_table_drop, 
                      time_table_drop]

copy_table_queries = [staging_events_copy, 
                      staging_songs_copy]

insert_table_queries = [user_table_insert, 
                        song_table_insert, 
                        artist_table_insert, 
                        time_table_insert,
                        songplay_table_insert]
