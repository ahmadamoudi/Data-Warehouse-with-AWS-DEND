import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events(
    event_id INTEGER IDENTITY(0,1) NOT NULL,
    artist_name VARCHAR(255),
    auth VARCHAR(50),
    user_first_name VARCHAR(255),
    user_gender VARCHAR(1),
    item_in_session INTEGER,
    user_last_name VARCHAR(255),
    song_length FLOAT(32), 
    user_level VARCHAR(50),
    location VARCHAR(255),
    method VARCHAR(25),
    page VARCHAR(35),
    registration VARCHAR(50),
    session_id BIGINT,
    song_title VARCHAR(255),
    status INTEGER,
    ts VARCHAR(50),
    user_agent TEXT,
    user_id VARCHAR(100),
    PRIMARY KEY (event_id))
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    song_id VARCHAR(100) NOT NULL,
    num_songs INTEGER,
    artist_id VARCHAR(100),
    artist_latitude FLOAT(32),
    artist_longitude FLOAT(32),
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    title VARCHAR(255),
    duration DECIMAL,
    year INTEGER,
    PRIMARY KEY (song_id))
""")




songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP, 
    user_id varchar , 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id INTEGER, 
    location varchar, 
    user_agent varchar)
""")


user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id varchar PRIMARY KEY NOT NULL, first_name varchar, last_name varchar, gender varchar, level varchar)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id varchar, title varchar, artist_id varchar, year INTEGER, duration float, PRIMARY KEY(song_id))
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, artist_name varchar, artist_location varchar, artist_latitude float, artist_longitude float)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time varchar PRIMARY KEY, hour INTEGER, day INTEGER, week INTEGER, month INTEGER, year INTEGER, weekday INTEGER)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'], role_arn=config['IAM_ROLE']['ARN'], log_json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT
    TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, 
    staging_events.user_id,
    staging_events.user_level,
    staging_songs.song_id,
    staging_songs.artist_id,
    staging_events.session_id,
    staging_events.location,
    staging_events.user_agent
FROM staging_events, staging_songs
WHERE staging_events.page = 'NextSong'
    AND staging_events.song_title = staging_songs.title
    AND user_id NOT IN (SELECT songplays.user_id FROM songplays WHERE songplays.user_id = user_id
    AND songplays.start_time = start_time AND songplays.session_id = session_id )
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)  
SELECT user_id, user_first_name, user_last_name, user_gender, user_level
FROM staging_events
WHERE page = 'NextSong'
    AND user_id NOT IN (SELECT user_id FROM users)
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
SELECT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id NOT IN (SELECT song_id FROM songs)
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude) 
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
WHERE artist_id NOT IN (SELECT artist_id FROM artists)
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT start_time, 
    EXTRACT(hr from start_time) AS hour,
    EXTRACT(d from start_time) AS day,
    EXTRACT(w from start_time) AS week,
    EXTRACT(mon from start_time) AS month,
    EXTRACT(yr from start_time) AS year, 
    EXTRACT(weekday from start_time) AS weekday 
FROM (
    SELECT  TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time 
    FROM staging_events )
WHERE start_time NOT IN (SELECT start_time FROM time)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]