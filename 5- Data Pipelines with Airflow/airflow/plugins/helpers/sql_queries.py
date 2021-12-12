#from dags.udac_example_dag import create_tables

from random import randint

class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
    

    quality_check_query = ("""
        SELECT
        (SELECT COUNT(*) FROM {} ) nb_time,
        (SELECT COUNT(*) FROM {} ) nb_artist,
        (SELECT COUNT(*) FROM {} ) nb_songs,
        (SELECT COUNT(*) FROM {} ) nb_users,
        (SELECT COUNT(*) FROM {} ) nb_songplay
    """)



    create_tables_artists = ("""
    DROP TABLE IF EXISTS public.artists;
    CREATE TABLE public.artists (
	artistid varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	lattitude numeric(18,0),
	longitude numeric(18,0)
    );
    """)

    create_table_songplays= ("""
    DROP TABLE IF EXISTS public.songplays;
    CREATE TABLE public.songplays (
	playid varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	userid int4 NOT NULL,
	"level" varchar(256),
	songid varchar(256),
	artistid varchar(256),
	sessionid int4,
	location varchar(256),
	user_agent varchar(256),
	CONSTRAINT songplays_pkey PRIMARY KEY (playid)
    );
    """)


    create_table_songs = ("""
    DROP TABLE IF EXISTS public.songs;
    CREATE TABLE public.songs (
	songid varchar(256) NOT NULL,
	title varchar(256),
	artistid varchar(256),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
    );
    """)

    create_table_time = ("""
    DROP TABLE IF EXISTS public.time;
    CREATE TABLE public."time" (
	start_time timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
    );
    """)

    create_table_users = ("""
    DROP TABLE IF EXISTS public.users;
    CREATE TABLE public.users (
	userid int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	"level" varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (userid)
    );
    """)


    create_table_staging_events =  ("""   
    DROP TABLE IF EXISTS public.staging_events;
    CREATE TABLE public.staging_events (
        artist varchar(256),
        auth varchar(256),
        firstname varchar(256),
        gender varchar(256),
        iteminsession int4,
        lastname varchar(256),
        length numeric(18,0),
        "level" varchar(256),
        location varchar(256),
        "method" varchar(256),
        page varchar(256),
        registration numeric(18,0),
        sessionid int4,
        song varchar(256),
        status int4,
        ts int8,
        useragent varchar(256),
        userid int4
    );
    """)

    create_table_staging_songs = ("""
    DROP TABLE IF EXISTS public.staging_songs;
    CREATE TABLE public.staging_songs (
        num_songs int4,
        artist_id varchar(256),
        artist_name varchar(256),
        artist_latitude numeric(18,0),
        artist_longitude numeric(18,0),
        artist_location varchar(256),
        song_id varchar(256),
        title varchar(256),
        duration numeric(18,0),
        "year" int4
        );
    """ )


    
    insert_into_fact_table = ("""
    INSERT INTO public.songplays
    (playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)
    SELECT
        md5(a.sessionid || a.start_time) playid
        ,a.start_time --TIMESTAMP 'epoch' + a.ts/1000 * interval '1 second' as start_time
        ,a.userid
        ,a.level
        ,b.song_id
        ,b.artist_id
        ,a.sessionid
        ,a.location
        ,a.useragent
    FROM (SELECT TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time, e.* FROM staging_events e WHERE e.page = 'NextSong') a
    LEFT JOIN staging_songs b
    ON a.artist = b.artist_name
    AND a.song = b.title
    AND a.length = b.duration
    
        
    """)

    
  