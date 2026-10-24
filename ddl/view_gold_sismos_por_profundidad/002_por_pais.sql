-- ¿Superficiales o profundos? Por país
CREATE OR REPLACE VIEW sismos_db.gold_sismos_por_profundidad AS
SELECT
  pais,
  CASE
    WHEN depth_km < 70  THEN '1. Superficial (< 70 km)'
    WHEN depth_km < 300 THEN '2. Intermedio (70-300 km)'
    ELSE                     '3. Profundo (> 300 km)'
  END                      AS tipo,
  count(*)                 AS sismos,
  round(avg(mag), 2)       AS mag_promedio
FROM sismos_db.silver_sismos
WHERE depth_km IS NOT NULL
GROUP BY 1, 2;
