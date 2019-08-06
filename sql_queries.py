import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN=config.get('IAM_ROLE','ARN')

#CREATE SCHEMA
public_schema_create="CREATE SCHEMA IF NOT EXISTS sparkify";
staging_schema_create="CREATE SCHEMA IF NOT EXISTS sparkify_stage";

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS sparkify_stage.staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS sparkify_stage.staging_songs"
user_table_drop = "DROP TABLE IF EXISTS sparkify.user"
time_table_drop = "DROP TABLE IF EXISTS sparkify.time"
song_table_drop = "DROP TABLE IF EXISTS sparkify.song"
artist_table_drop = "DROP TABLE IF EXISTS sparkify.artist"
songplay_table_drop = "DROP TABLE IF EXISTS sparkify.songplays"

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS sparkify_stage.staging_events (
        artist VARCHAR,
        auth VARCHAR(50),
        firstName VARCHAR(25),
        gender CHAR(1),
        iteminSession SMALLINT,
        lastName VARCHAR(25),
        length DECIMAL,
        level CHAR(5),
        location TEXT,
        method CHAR(3),
        page VARCHAR(25),
        registration DECIMAL,
        sessionId INTEGER,
        song TEXT,
        status SMALLINT,
        ts BIGINT,
        userAgent TEXT,
        userId INTEGER
        );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS sparkify_stage.staging_songs (
        num_songs INTEGER,
        artist_id VARCHAR,
        latitude VARCHAR(50),
        longitude VARCHAR(50),
        location VARCHAR(50),
        artist_name VARCHAR,
        song_id TEXT,
        title TEXT,
        duration DECIMAL,
        year INTEGER
        );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS sparkify.songplays (
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, 
        start_time TIMESTAMP NOT NULL DISTKEY, 
        user_id INTEGER NOT NULL, 
        level VARCHAR(5), 
        song_id VARCHAR NOT NULL, 
        artist_id VARCHAR NOT NULL, 
        session_id INTEGER NOT NULL, 
        location TEXT, 
        user_agent TEXT
        , FOREIGN KEY (start_time) REFERENCES sparkify.time (start_time)
        , FOREIGN KEY (user_id) REFERENCES sparkify.user (user_id)
        , FOREIGN KEY (song_id) REFERENCES sparkify.song (song_id)
        , FOREIGN KEY (artist_id) REFERENCES sparkify.artist (artist_id)
        ) SORTKEY (start_time,artist_id,song_id,user_id);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS sparkify.user (
        user_id INTEGER SORTKEY PRIMARY KEY,
        first_name VARCHAR(25) NOT NULL, 
        last_name VARCHAR(25) NOT NULL, 
        gender CHAR(1) NOT NULL, 
        level VARCHAR
       ) DISTSTYLE all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS sparkify.song (
        song_id VARCHAR SORTKEY PRIMARY KEY, 
        title TEXT NOT NULL, 
        artist_id VARCHAR NOT NULL, 
        year INTEGER, 
        duration NUMERIC
       ) DISTSTYLE all;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS sparkify.artist (
        artist_id VARCHAR SORTKEY PRIMARY KEY, 
        name VARCHAR NOT NULL, 
        location TEXT, 
        latitude NUMERIC, 
        longitude NUMERIC
        ) DISTSTYLE all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS sparkify.time (
        start_time TIMESTAMP SORTKEY DISTKEY PRIMARY KEY, 
        hour INTEGER NOT NULL, 
        day INTEGER NOT NULL, 
        week INTEGER NOT NULL, 
        month INTEGER NOT NULL, 
        year INTEGER NOT NULL, 
        weekday INTEGER NOT NULL   
        );
""")

# STAGING TABLES
staging_events_copy = ("""
    COPY sparkify_stage.staging_events FROM 's3://udacity-dend/log_data/'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    compupdate off
    JSON 's3://udacity-dend/log_json_path.json';  
""").format(DWH_ROLE_ARN)

staging_songs_copy = ("""
    COPY sparkify_stage.staging_songs FROM 's3://udacity-dend/song_data/'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    compupdate off
    JSON 'auto';
""").format(DWH_ROLE_ARN)

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO sparkify.songplays (start_time, 
        user_id, 
        level, 
        song_id, 
        artist_id, 
        session_id, 
        location, 
        user_agent) 
        SELECT TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second',
            se.userId, 
            se.level, 
            s.song_id, 
            s.artist_id, 
            sessionId, 
            location, 
            userAgent
        FROM sparkify_stage.staging_events se
        JOIN sparkify.song s ON se.song=s.title and se.page='NextSong'
        ;
""")

user_table_insert = ("""    
    INSERT INTO sparkify.user (
        SELECT DISTINCT userId,
            firstName, 
            lastName, 
            gender, 
            level
        FROM sparkify_stage.staging_events
        WHERE page='NextSong'
        );
""")

song_table_insert = ("""
    INSERT INTO sparkify.song (
        SELECT song_id, 
            title, 
            artist_id, 
            year, 
            duration 
        FROM sparkify_stage.staging_songs
        );
""")

artist_table_insert = ("""
    INSERT INTO sparkify.artist ( 
        SELECT DISTINCT artist_Id, 
            artist_name, 
            location, 
            latitude, 
            longitude
        FROM sparkify_stage.staging_songs
       );
""")

time_table_insert = ("""
    INSERT INTO sparkify.time (
        SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second', 
            EXTRACT(HOUR FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'), 
            EXTRACT(DAY FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'), 
            EXTRACT(WEEK FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'), 
            EXTRACT(MONTH FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'), 
            EXTRACT(YEAR FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'),  
            EXTRACT(WEEKDAY FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')
        FROM sparkify_stage.staging_events
       );
""")
#SCHEMA LISTS
create_schema_queries=[staging_schema_create, public_schema_create]

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create,]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert, ]
