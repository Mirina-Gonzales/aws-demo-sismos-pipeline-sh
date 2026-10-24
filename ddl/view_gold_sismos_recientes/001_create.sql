-- Últimas 24 horas: base para "¿qué tembló durante la charla?"
-- Usa time_utc (cuándo OCURRIÓ el sismo), no ingest_ts: el MERGE actualiza
-- ingest_ts cada vez que el sismo vuelve a llegar en el feed.
CREATE OR REPLACE VIEW sismos_db.gold_sismos_recientes AS
SELECT
  region,
  mag,
  depth_km,
  time_cl,
  tsunami,
  date_diff('minute', time_utc, CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp)) AS minutos_atras
FROM sismos_db.silver_sismos
WHERE time_utc >= CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp) - INTERVAL '24' HOUR;
