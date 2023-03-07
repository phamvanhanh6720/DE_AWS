import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS factSongPlay"
user_table_drop = "DROP TABLE IF EXISTS dimUser"
song_table_drop = "DROP TABLE IF EXISTS dimSong"
artist_table_drop = "DROP TABLE IF EXISTS dimArtist"
time_table_drop = "DROP TABLE IF EXISTS dimTime"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events_table(
        artist              varchar,
        auth                varchar,
        firstName           varchar,
        gender              varchar,
        itemInSession       integer,
        lastName            varchar,
        length              double precision,
        level               varchar,
        location            varchar,
        method              varchar,
        page                varchar,
        registration        bigint,
        sessionId           integer,
        song                varchar,
        status              integer,
        ts                  bigint,
        userAgent           varchar,
        userId              integer
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs_table(
        num_songs           int,
        artist_id           varchar,
        artist_latitude     double precision,
        artist_longitude    double precision,
        artist_location     varchar,
        artist_name         varchar,
        song_id             varchar,
        title               varchar,
        duration            double precision,
        year                int
    );
""")

songplay_table_create = ("""
    CREATE TABLE factSongPlay(
        songplay_id     integer     IDENTITY (1, 1) PRIMARY KEY ,
        start_time      bigint      NOT NULL        REFERENCES dimTime(time_key),
        user_id         integer     NOT NULL        REFERENCES dimUser(user_id),
        level           varchar     NOT NULL,
        song_id         varchar     NOT NULL        REFERENCES dimSong(song_id),
        artist_id       varchar     NOT NULL       REFERENCES dimArtist(artist_id),
        session_id      varchar     NOT NULL,
        location        varchar,
        user_agent      varchar
    );
""")

user_table_create = ("""
    CREATE TABLE dimUser(
            user_id      integer    NOT NULL PRIMARY KEY,
            first_name   varchar    NOT NULL,
            last_name    varchar    NOT NULL,
            gender       varchar    NOT NULL,
            level        varchar    NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE dimSong(
        song_id         varchar             NOT NULL PRIMARY KEY ,
        title           varchar             NOT NULL,
        artist_id       varchar             NOT NULL,
        year            int                 NOT NULL,
        duration        double precision    NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE dimArtist
    (
        artist_id   varchar     NOT NULL PRIMARY KEY,
        name        varchar     NOT NULL,
        location    varchar,
        latitude    double precision,
        longitude   double precision
    );
""")

time_table_create = ("""
    CREATE TABLE dimTime
    (
        time_key        bigint      NOT NULL PRIMARY KEY,
        start_time      timestamp   NOT NULL ,
        hour            smallint    NOT NULL ,
        day             smallint    NOT NULL ,
        month           smallint    NOT NULL,
        year            smallint    NOT NULL,
        weekday         smallint    NOT NULL
    );
""")

# STAGING TABLES

staging_songs_copy = ("""
COPY staging_songs_table
FROM '{}'
credentials 'aws_iam_role={}'
json 'auto'
region 'us-west-2';
""").format('s3://udacity-dend/song_data/A/A/', config.get('IAM_ROLE', 'ARN'))

staging_events_copy = ("""
COPY staging_events_table
FROM '{}'
iam_role '{}'
json '{}'
region 'us-west-2';
""").format('s3://udacity-dend/log_data/', config.get('IAM_ROLE', 'ARN'), 's3://udacity-dend/log_json_path.json')

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO factSongPlay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT(e.ts)  AS start_time,
            e.userId        AS user_id,
            e.level         AS level,
            s.song_id       AS song_id,
            s.artist_id     AS artist_id,
            e.sessionId     AS session_id,
            e.location      AS location,
            e.userAgent     AS user_agent
    FROM staging_events_table e
    JOIN staging_songs_table  s   ON (e.song = s.title AND e.artist = s.artist_name)
    AND e.page  =  'NextSong';
""")

user_table_insert = ("""
    INSERT INTO dimUser(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userid as user_id,
                    firstName as first_name,
                    lastName as last_name,
                    gender as gender,
                    level as level
    FROM staging_events_table
    WHERE user_id IS NOT NULL
    AND page  =  'NextSong';
""")

song_table_insert = ("""
    INSERT INTO dimSong(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs_table;
""")

artist_table_insert = ("""
    INSERT INTO dimArtist( artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude  as latitude,
        artist_longitude as longitude
    FROM staging_songs_table;
    
    SELECT * FROM dimArtist;
""")

time_table_insert = ("""
INSERT INTO dimTime(time_key, start_time, hour, day, month, year, weekday)
SELECT DISTINCT ts AS time_key,
                date_add('ms',ts,'1970-01-01') as start_time,
                extract(hour from date_add('ms',ts,'1970-01-01')) as hour,
                extract(day from date_add('ms',ts,'1970-01-01')) as day,
                extract(month from date_add('ms',ts,'1970-01-01')) as month,
                extract(year from date_add('ms',ts,'1970-01-01')) as year,
                extract(dow	from date_add('ms',ts,'1970-01-01')) as weekday
FROM staging_events_table;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
