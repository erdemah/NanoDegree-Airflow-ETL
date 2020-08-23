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
    
    test_pk_songplays = """
    select count(*) from public.songplays 
    where playid is null
    """
    test_pk_songs = """
    select count(*) from public.songs
    where songid is null
    """
    test_pk_artists = """
    select count(*) from public.artists
    where artistid is null
    """
    test_pk_users = """
    select count(*) from public.users
    where userid is null
    """
    test_pk_time = """
    select count(*) from public.time_table
    where start_time is null
    """
    
    test_list = [test_pk_songplays, test_pk_songs, test_pk_artists, test_pk_users, test_pk_time]
    result = [0,0,0,0,0]
    