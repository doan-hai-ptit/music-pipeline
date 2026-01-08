{{config(materialized="table")}}

SELECT 
    da.artist_id,
    da.artist_name,
    COUNT(*) AS played_count
FROM music_datalake.dim_artists da
JOIN music_datalake.dim_songs ds ON da.artist_id = ds.artist_id
JOIN music_datalake.music_events_ext m ON m.song_id = ds.song_id
GROUP BY da.artist_id, da.artist_name
ORDER BY played_count DESC, da.artist_name ASC
LIMIT 10
