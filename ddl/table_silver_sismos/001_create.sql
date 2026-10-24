CREATE TABLE IF NOT EXISTS sismos_db.silver_sismos (
  id           string    COMMENT 'id único del sismo en USGS',
  mag          double    COMMENT 'magnitud',
  magtype      string    COMMENT 'tipo de magnitud (ml, mb, mww...)',
  place        string    COMMENT 'texto original de USGS',
  region       string    COMMENT 'localidad extraída de place',
  lat          double,
  lon          double,
  depth_km     double    COMMENT 'profundidad en km',
  time_utc     timestamp COMMENT 'hora del sismo en UTC',
  time_cl      timestamp COMMENT 'hora del sismo en Chile',
  event_date   date      COMMENT 'fecha del sismo en Chile (partición)',
  updated_utc  timestamp COMMENT 'última actualización en USGS',
  tsunami      bigint    COMMENT '1 si hubo alerta de tsunami',
  ingest_ts    timestamp COMMENT 'cuándo lo trajo el pipeline (UTC)'
)
PARTITIONED BY (event_date)
LOCATION 's3://{{BUCKET_DATALAKE}}/silver/silver_sismos/'
TBLPROPERTIES (
  'table_type'        = 'ICEBERG',
  'format'            = 'parquet',
  'write_compression' = 'zstd'
);
