SELECT * FROM `music_datalake.music_events_ext`
WHERE NOT (status = 'raw' AND event_date < CURRENT_DATE('Asia/Ho_Chi_Minh'))