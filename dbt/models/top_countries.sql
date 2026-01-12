
{{config(materialized="table")}}

SELECT 
  d.country_id,
  d.country_name,
  COUNT(*) AS played_count
FROM {{ref('stg_music_events')}} m
JOIN music_datalake.dim_countries d ON d.country_id = m.country_id
GROUP BY d.country_id, d.country_name
ORDER BY played_count DESC, d.country_name ASC