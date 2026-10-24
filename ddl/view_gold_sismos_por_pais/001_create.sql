-- ¿Qué país de Sudamérica tiembla más?
CREATE OR REPLACE VIEW sismos_db.gold_sismos_por_pais AS
SELECT
  pais,
  count(*)                 AS sismos,
  count_if(mag >= 4.0)     AS sismos_4_o_mas,
  round(avg(mag), 2)       AS mag_promedio,
  max(mag)                 AS mag_maxima,
  max(time_cl)             AS ultimo_sismo_cl
FROM sismos_db.silver_sismos
GROUP BY pais;
