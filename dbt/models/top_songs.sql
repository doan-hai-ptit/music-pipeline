{{config(materialized="table")}}

SELECT 
  d.song_id,
  d.song_name,
  COUNT(*) AS played_count
FROM {{ref('stg_music_events')}} m
JOIN music_datalake.dim_songs d ON d.song_id = m.song_id
GROUP BY d.song_id, d.song_name
ORDER BY played_count DESC, d.song_name ASC
LIMIT 10
