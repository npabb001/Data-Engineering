import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN='arn:aws:iam::608956766401:role/dwhRole'

# DROP TABLES

staging_events_table_drop = "drop table if exists  staging_events;"
staging_songs_table_drop = "drop table if exists  staging_songs;"
songplay_table_drop = "drop table if exists  songplays;"
user_table_drop = "drop table if exists  users;"
song_table_drop = "drop table if exists  songs;"
artist_table_drop = "drop table if exists  artists;"
time_table_drop = "drop table if exists  time;"

# CREATE TABLES

staging_events_table_create= ("""create table staging_events (artist varchar, auth varchar, firstname varchar , gender varchar, iteminsession int, lastName varchar, length float, level varchar, location varchar, method varchar, page varchar, registration varchar, sessionid int, song varchar, status int, ts bigint , userAgent varchar, userid int sortkey)
""")

staging_songs_table_create = ("""create table staging_songs (num_songs int, artist_id varchar distkey, artist_latitude varchar, artist_longitude varchar, artist_location varchar, artist_name varchar, song_id varchar, title varchar, duration float, year int)
""")

# songplay_table_create = ("""
# """)

# user_table_create = ("""
# """)

# song_table_create = ("""
# """)

# artist_table_create = ("""
# """)

# time_table_create = ("""
# """)

songplay_table_create = ("""create table songplays (songplay_id int IDENTITY(0,1) primary key, start_time bigint sortkey not null, user_id int not null , song_id varchar not null, artist_id varchar not null, level varchar, session_id int not null, location varchar, user_agent varchar) ;
""") 

user_table_create = (""" create table users (user_id int primary key , first_name varchar, last_name varchar, gender varchar, level varchar);""")

song_table_create = (""" create table songs (song_id varchar primary key , title varchar, artist_id varchar, year int, duration float);""")

artist_table_create = (""" create table artists (artist_id varchar primary key , name varchar, location varchar, latitude varchar, longitude varchar);""")

time_table_create = (""" create table time (start_time bigint primary key , hour int, day int, week int, month int, year int, weekday int);""")

# STAGING TABLES

staging_songs_copy = ("""copy staging_songs from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    format as json 'auto' ACCEPTINVCHARS EMPTYASNULL compupdate off region 'us-west-2';
""").format(ARN)

staging_events_copy = ("""copy staging_events from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    format as json 's3://udacity-dend/log_json_path.json'
    ACCEPTINVCHARS EMPTYASNULL compupdate off region 'us-west-2';
""").format(ARN)

# FINAL TABLES

songplay_table_insert = ("""insert into songplays(start_time, user_id, song_id, artist_id, level, session_id , location , user_agent)(SELECT distinct se.ts start_time, se.userId user_id, se.level,s.song_id song_id,a.artist_id artist_id, se.sessionId session_id, se.location, se. userAgent user_agent FROM staging_events se JOIN artists a ON se.artist = a.name JOIN songs s ON se.song = s.title WHERE page='NextSong');
""")

user_table_insert = ("""insert into users (SELECT userId, firstName, lastName, gender, level FROM staging_events WHERE page='NextSong' AND (userId, ts) IN (SELECT userId, max(ts) FROM staging_events GROUP BY userId));
""")

song_table_insert = ("""insert into songs (select distinct song_id, title, artist_id, year,  duration from staging_songs );
""")

artist_table_insert = ("""insert into artists(SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs WHERE (artist_id, duration) IN (SELECT artist_id, max(duration) FROM staging_songs GROUP BY artist_id));
""")

time_table_insert = ("""insert into time (select distinct ts, extract(hour from timestamp with time zone 'epoch' + ts/1000 * interval '1 second' ) as int , extract(day from timestamp with time zone 'epoch' + ts/1000 * interval '1 second') , extract(week from timestamp with time zone 'epoch' + ts/1000 * interval '1 second') , extract(month from timestamp with time zone 'epoch' + ts/1000 * interval '1 second') , extract(year from timestamp with time zone 'epoch' + ts/1000 * interval '1 second') , extract(weekday from timestamp with time zone 'epoch' + ts/1000 * interval '1 second') from staging_events where page = 'NextSong');
""")

# QUERY LISTS

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
