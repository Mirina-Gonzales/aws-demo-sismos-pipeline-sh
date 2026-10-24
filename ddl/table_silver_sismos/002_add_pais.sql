-- Sudamérica: el país de cada sismo (antes la tabla era solo de Chile)
ALTER TABLE sismos_db.silver_sismos ADD COLUMNS (pais string COMMENT 'país del sismo según USGS')
