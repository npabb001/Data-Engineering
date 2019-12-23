# DROP TABLES 

songplay_table_drop = "DROP TABLE if exists songplays;"
user_table_drop = "DROP TABLE if exists users;"
song_table_drop = "DROP TABLE if exists songs;"
artist_table_drop = "DROP TABLE if exists artists;"
time_table_drop = "DROP TABLE if exists time;"

# CREATE TABLES

#songplay_table_create = ("""create table songplays (songplay_id SERIAL primary key, start_time time, user_id int not null, level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar) ; """) 
songplay_table_create = ("""create table songplays (songplay_id SERIAL primary key, start_time bigint, user_id int , song_id varchar, artist_id varchar,  foreign key (start_time) references time (start_time), foreign key (user_id) references users (user_id), level varchar, foreign key (song_id) references songs (song_id), foreign key (artist_id) references artists (artist_id), session_id int, location varchar, user_agent varchar) ; 
""") 

user_table_create = (""" create table users (user_id int primary key, first_name varchar, last_name varchar, gender varchar, level varchar);""")

song_table_create = (""" create table songs (song_id varchar primary key, title varchar, artist_id varchar, year int, duration float);""")

artist_table_create = (""" create table artists (artist_id varchar primary key, name varchar, location varchar, latitude varchar, longitude varchar);""")

time_table_create = (""" create table time (start_time bigint primary key, hour int, day int, week int, month int, year int, weekday int);""")

# INSERT RECORDS

songplay_table_insert = ("""Insert into songplays( start_time , user_id , level , song_id , artist_id , session_id , location , user_agent ) values( %s, %s, %s, %s, %s, %s, %s, %s)  ;""")
#Insert into songplays (songplay_id , start_time , user_id , level , song_id , artist_id , session_id , location , user_agent ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)

user_table_insert = ("""insert into users(first_name , last_name , gender , level, user_id ) values(%s, %s, %s, %s, %s)  ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level;""")

song_table_insert = "Insert into songs  values (%s, %s, %s, %s, %s) on conflict (song_id) do nothing ;"

artist_table_insert = "insert into artists values (%s, %s, %s, %s, %s) on conflict (artist_id) do nothing "

time_table_insert = "Insert into time values(%s, %s, %s, %s, %s, %s, %s) on conflict (start_time) do nothing "

# FIND SONGS

song_select = "select songs.song_id, artists.artist_id from songs inner join artists on songs.artist_id = artists.artist_id where songs.title = %s and songs.duration = %s and artists.name = %s;"
#select artist_id from artists where name = %s;                                                                                                          

# QUERY LISTS

create_table_queries = [ user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]