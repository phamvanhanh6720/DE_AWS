SELECT
  DISTINCT tablename
FROM
  PG_TABLE_DEF
WHERE
  schemaname = 'public';

/* Drop table*/
DROP TABLE IF EXISTS staging_events_table;
DROP TABLE IF EXISTS staging_songs_table;

DROP TABLE IF EXISTS staging_events;


/* Create staging tables*/
CREATE TABLE staging_songs_table (
    num_songs int,
    artist_id varchar(30),
    artist_latitude double precision,
    artist_location text,
    artist_longitude double precision,
    artist_name varchar(45),
    duration double precision,
    song_id varchar(30),
    title text,
    year smallint
);

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

COPY staging_songs_table
FROM 's3://udacity-dend/song_data/'
iam_role 'arn:aws:iam::482862536640:role/myRedshiftRole'
json 'auto'
region 'us-west-2';

SELECT * FROM staging_songs_table;

DROP TABLE IF EXISTS staging_events_table;
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

COPY staging_events_table
FROM 's3://udacity-dend/log_data/'
iam_role 'arn:aws:iam::482862536640:role/myRedshiftRole'
json 's3://udacity-dend/log_json_path.json'
region 'us-west-2';

SELECT * FROM staging_events_table;

/*DWH Modeling*/

DROP TABLE IF EXISTS dimUser;
CREATE TABLE dimUser(
        user_id      integer    NOT NULL PRIMARY KEY,
        first_name   varchar    NOT NULL,
        last_name    varchar    NOT NULL,
        gender       varchar    NOT NULL,
        level        varchar    NOT NULL
);

DROP TABLE IF EXISTS  dimSong;
CREATE TABLE dimSong(
    song_id         varchar             NOT NULL PRIMARY KEY ,
    title           varchar             NOT NULL,
    artist_id       varchar             NOT NULL,
    year            int                 NOT NULL,
    duration        double precision    NOT NULL
);

DROP TABLE IF EXISTS dimArtist;
CREATE TABLE dimArtist
(
    artist_id   varchar     NOT NULL PRIMARY KEY,
    name        varchar     NOT NULL,
    location    varchar,
    latitude    double precision,
    longitude   double precision
);

DROP TABLE IF EXISTS dimTime;
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

DROP TABLE IF EXISTS factSongPlay;
CREATE TABLE factSongPlay(
    songplay_id     integer     IDENTITY (1, 1) PRIMARY KEY ,
    start_time      bigint      NOT NULL        REFERENCES dimTime(time_key),
    user_id         integer     NOT NULL        REFERENCES dimUser(user_id),
    level           varchar     NOT NULL,
    song_id         varchar     NOT NULL        REFERENCES dimSong(song_id),
    artist_id       varchar      NOT NULL       REFERENCES dimArtist(artist_id),
    location        varchar,
    user_agent      varchar
);

/*Drop DWH*/

DROP TABLE public.factSongPlay;

DROP TABLE  IF EXISTS public.dimTime;
DROP TABLE IF EXISTS dimSong;
DROP TABLE IF EXISTS dimArtist;
DROP TABLE IF EXISTS public.dimUser;

/*Ingest data to DWH*/


INSERT INTO dimTime(time_key, start_time, hour, day, month, year, weekday)
SELECT DISTINCT ts AS time_key,
                date_add('ms',ts,'1970-01-01') as start_time,
                extract(hour from date_add('ms',ts,'1970-01-01')) as hour,
                extract(day from date_add('ms',ts,'1970-01-01')) as day,
                extract(month from date_add('ms',ts,'1970-01-01')) as month,
                extract(year from date_add('ms',ts,'1970-01-01')) as year,
                extract(dow	from date_add('ms',ts,'1970-01-01')) as weekday
FROM staging_events_table;

INSERT INTO dimArtist(artist_key, artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id as artist_key,
    artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude  as latitude,
    artist_longitude as longitude
FROM staging_songs_table;


INSERT INTO dimSong(song_key, song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id as song_key,
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs_table;

INSERT INTO dimUser(user_key, user_id, first_name, last_name, gender, level)
SELECT DISTINCT userid as user_key,
                userid as user_id,
                firstName as first_name,
                lastName as last_name,
                gender as gender,
                level as level
FROM staging_events_table;


INSERT INTO factSongPlay(start_key, user_key, level, song_key, artist_key, location, user_agent)
SELECT events.ts as start_key,
       events.userid as user_key,
       events.level as level,
       songs.song_id as song_key,
       songs.artist_id as artist_key,
       events.location as location,
       events.userAgent as user_agent

FROM staging_events_table as events
INNER JOIN staging_songs_table as songs ON events.song = songs.title
WHERE events.page = 'NextSong';


select * from staging_events_table
where page = 'NextSong';

select * from staging_songs_table;

select * from dimTime;
select * from staging_songs_table;
select * from dimSong;
select * from dimUser;
select * from factSongPlay;