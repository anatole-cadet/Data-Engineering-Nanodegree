import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs; "
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
        event_id          INTEGER IDENTITY(1,1) ,
        artist            VARCHAR ,
        auth              VARCHAR ,
        firstname         VARCHAR  ,
        gender            VARCHAR ,
        item_in_session   INT ,
        last_name         VARCHAR(70) ,
        length            VARCHAR(50) ,
        level             VARCHAR(25) ,
        location          VARCHAR(50) ,
        method            VARCHAR(10) ,
        page              VARCHAR(30) ,
        registration      VARCHAR  ,
        session_id        BIGINT    SORTKEY DISTKEY,
        song              VARCHAR ,
        status            BIGINT,
        ts                BIGINT,
        user_agent        TEXT ,
        user_id           INT                   
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs          INTEGER,
        artist_id          VARCHAR,
        artist_latitude    FLOAT,
        artist_longitude   FLOAT,
        artist_location    VARCHAR(MAX),
        artist_name        VARCHAR(MAX),
        song_id            VARCHAR SORTKEY,
        title              VARCHAR,
        duration           FLOAT,
        year               INTEGER        
)diststyle all;
""")

songplay_table_create = ("""
CREATE TABLE songplays (
        songplay_id   INTEGER IDENTITY(0,1) PRIMARY KEY SORTKEY, 
        start_time    TIMESTAMP             NOT NULL, 
        user_id       INT                   NOT NULL    DISTKEY, 
        level         VARCHAR(30)           NOT NULL, 
        song_id       VARCHAR(30)           NOT NULL, 
        artist_id     VARCHAR(30)           NOT NULL, 
        session_id    INT                   NOT NULL, 
        location      VARCHAR               NULL, 
        user_agent    VARCHAR               NULL
    )
""")

user_table_create = ("""
CREATE TABLE users(
        user_id       INT      PRIMARY KEY SORTKEY, 
        first_name    VARCHAR(70)  NOT NULL, 
        last_name     VARCHAR(70)  NOT NULL, 
        gender        VARCHAR(15)  NOT NULL, 
        level         VARCHAR(30)  NOT NULL
    )diststyle all;
""")

song_table_create = ("""
CREATE TABLE songs(
        song_id       VARCHAR(30)   PRIMARY KEY SORTKEY, 
        title         VARCHAR(90)   NOT NULL,  
        artist_id     VARCHAR(30)   NOT NULL, 
        year          INT           NOT NULL, 
        duration      DECIMAL       NOT NULL
        )diststyle all;
""")

artist_table_create = ("""
CREATE TABLE artists(
        artist_id     VARCHAR(30)   PRIMARY KEY SORTKEY, 
        name          VARCHAR(80), 
        location      VARCHAR(50), 
        latitude      FLOAT, 
        longitude     FLOAT
    )diststyle all;
""")
        #time_id       INTEGER IDENTITY(0,1) PRIMARY KEY SORTKEY, 

time_table_create = ("""
    CREATE TABLE time(
        start_time    TIMESTAMP PRIMARY KEY SORTKEY, 
        hour          INT, 
        day           INT,  
        week          INT, 
        month         INT, 
        year          INT, 
        weekday       INT
        )diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events(       
artist          
, auth            
, firstname       
, gender          
, item_in_session 
, last_name       
, length          
, level           
, location        
, method          
, page            
, registration    
, session_id      
, song            
, status          
, ts              
, user_agent      
, user_id   )
FROM {}
CREDENTIALS 'aws_iam_role={}'
JSON {}
REGION 'us-west-2'
""").format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs            
FROM {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF region 'us-west-2'
TIMEFORMAT as 'epochmillisecs'
JSON 'auto';
""").format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
start_time
,user_id
,level  
,song_id    
,artist_id  
,session_id 
,location   
,user_agent )
SELECT TO_TIMESTAMP(a.ts,'YYYYMMDD')::TIMESTAMP, a.user_id, a.level, b.song_id, b.artist_id, a.session_id, a.location, a.user_agent
FROM staging_events a
JOIN staging_songs b
ON  a.artist = b.artist_name
AND a.song = b.title
AND a.length = b.duration
WHERE a.page= 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users(
user_id   
,first_name
,last_name 
,gender    
,level     
)
SELECT DISTINCT a.user_id, a.firstname, a.last_name, a.gender, a.level
FROM staging_events a
WHERE a.page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs(song_id,title,artist_id,year,duration )
SELECT a.song_id, a.title, a.artist_id, a.year,a.duration
FROM staging_songs a
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT a.artist_id, a.artist_name, a.artist_location, a.artist_latitude, a.artist_longitude
FROM staging_songs a
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT b.start_time, extract(hour from b.start_time), extract(day from b.start_time), extract(week from b.start_time),
        extract(month from b.start_time), extract(year from b.start_time), extract(weekday from b.start_time)
FROM songplays b
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

           