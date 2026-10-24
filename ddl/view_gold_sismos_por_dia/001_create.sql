-- ¿Cómo va la actividad día a día? (fecha de Chile)
CREATE OR REPLACE VIEW sismos_db.gold_sismos_por_dia AS
SELECT
  event_date,
  count(*)                 AS sismos,
  count_if(mag >= 4.0)     AS sismos_4_o_mas,
  max(mag)                 AS mag_maxima
FROM sismos_db.silver_sismos
GROUP BY event_date;
