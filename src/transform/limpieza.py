"""Limpieza bronze -> silver de los sismos de Sudamérica de USGS.

Solo usa pandas: no toca AWS. Así se puede probar con pytest en tu máquina
y la Lambda transform (etapa 4) solo se encarga de leer y escribir.
"""
from datetime import datetime, timezone

import pandas as pd

ZONA_HORARIA_CHILE = "America/Santiago"

# Esquema de la tabla silver: nombre de columna -> qué contiene
COLUMNAS_SILVER = [
    "id",           # id único del sismo en USGS
    "mag",          # magnitud
    "magtype",      # tipo de magnitud (ml, mb, mww...)
    "place",        # texto original de USGS
    "region",       # localidad extraída de place ("Calama")
    "lat",
    "lon",
    "depth_km",     # profundidad en km
    "time_utc",     # hora del sismo en UTC
    "time_cl",      # hora del sismo en Chile
    "event_date",   # fecha del sismo en Chile (partición de la tabla)
    "updated_utc",  # última vez que USGS actualizó el sismo
    "tsunami",      # 1 si USGS emitió alerta de tsunami
    "ingest_ts",    # cuándo lo trajo nuestro pipeline (UTC)
    "pais",         # país del sismo según USGS ("Chile")
]

# Países de Sudamérica como los escribe USGS (en inglés, sin tilde)
PAISES = [
    "Argentina", "Bolivia", "Brazil", "Chile", "Colombia", "Ecuador", "French Guiana",
    "Guyana", "Paraguay", "Peru", "Suriname", "Uruguay", "Venezuela",
]
_PAISES_RE = "|".join(PAISES)

# "45 km W of Calama, Chile"          -> region=Calama,   pais=Chile
# "off the coast of Coquimbo, Chile"  -> region=Coquimbo, pais=Chile
# "Calama, Chile"                     -> region=Calama,   pais=Chile
_PATRON_REGION = rf"^(?:.*\bof\s+)?(?P<region>[^,]+?)\s*,\s*(?P<pais>{_PAISES_RE})\s*$"
# "Chile-Argentina border region" (sin coma) -> el primer país que aparezca: Chile
_PATRON_PAIS = rf"\b({_PAISES_RE})\b"


def feed_a_dataframe(feed: dict) -> pd.DataFrame:
    """Paso 1 - Aplanar: convierte el GeoJSON de USGS en una tabla, sin limpiar nada."""
    filas = []
    for sismo in feed.get("features", []):
        props = sismo.get("properties") or {}
        coords = list((sismo.get("geometry") or {}).get("coordinates") or [])
        coords += [None] * (3 - len(coords))  # por si vienen incompletas
        filas.append(
            {
                "id": sismo.get("id"),
                "mag": props.get("mag"),
                "magtype": props.get("magType"),
                "place": props.get("place"),
                "time_ms": props.get("time"),
                "updated_ms": props.get("updated"),
                "tsunami": props.get("tsunami"),
                "lon": coords[0],
                "lat": coords[1],
                "depth_km": coords[2],
            }
        )
    return pd.DataFrame(
        filas,
        columns=["id", "mag", "magtype", "place", "time_ms", "updated_ms",
                 "tsunami", "lon", "lat", "depth_km"],
    )


def solo_sudamerica(df: pd.DataFrame) -> pd.DataFrame:
    """Paso 2 - Filtrar: sismos cuyo lugar menciona un país de Sudamérica.

    La caja de la ingesta también trae Panamá y el Caribe: este filtro los deja fuera.
    """
    es_sudamerica = df["place"].fillna("").str.contains(rf"\b(?:{_PAISES_RE})\b", case=False)
    return df[es_sudamerica].copy()


def calidad_minima(df: pd.DataFrame) -> pd.DataFrame:
    """Paso 3 - Calidad: descartamos filas sin los datos mínimos para analizar."""
    obligatorias = ["id", "mag", "time_ms", "lat", "lon"]
    return df.dropna(subset=obligatorias).copy()


def _a_utc(serie_ms: pd.Series) -> pd.Series:
    """Epoch en milisegundos -> fecha y hora con zona UTC."""
    return pd.to_datetime(serie_ms, unit="ms", utc=True)


def transformar(df: pd.DataFrame, ingest_ts: datetime) -> pd.DataFrame:
    """Paso 4 - Transformar: fechas legibles, hora de Chile, región y tipos correctos."""
    tiempo_utc = _a_utc(df["time_ms"])
    tiempo_cl = tiempo_utc.dt.tz_convert(ZONA_HORARIA_CHILE)

    salida = pd.DataFrame(
        {
            "id": df["id"].astype(str),
            "mag": df["mag"].astype("float64"),
            "magtype": df["magtype"].fillna("").astype(str).str.lower(),
            "place": df["place"].astype(str).str.strip(),
            "lat": df["lat"].astype("float64"),
            "lon": df["lon"].astype("float64"),
            "depth_km": df["depth_km"].astype("float64"),
            # Guardamos las horas SIN zona horaria (así las entiende Athena / Iceberg):
            # time_utc está en UTC y time_cl en hora de Chile.
            "time_utc": tiempo_utc.dt.tz_localize(None).astype("datetime64[us]"),
            "time_cl": tiempo_cl.dt.tz_localize(None).astype("datetime64[us]"),
            "event_date": tiempo_cl.dt.date,
            "updated_utc": _a_utc(df["updated_ms"].fillna(df["time_ms"]))
            .dt.tz_localize(None)
            .astype("datetime64[us]"),
            "tsunami": df["tsunami"].fillna(0).astype("int64"),
        },
        index=df.index,
    )

    partes = salida["place"].str.extract(_PATRON_REGION)
    salida["region"] = partes["region"].fillna(salida["place"]).str.strip()
    salida["pais"] = partes["pais"].fillna(salida["place"].str.extract(_PATRON_PAIS, expand=False))

    ts = ingest_ts.astimezone(timezone.utc).replace(tzinfo=None)
    salida["ingest_ts"] = pd.Timestamp(ts).as_unit("us")

    return salida[COLUMNAS_SILVER]


def sin_duplicados(df: pd.DataFrame) -> pd.DataFrame:
    """Paso 5 - Deduplicar: si un sismo viene repetido, nos quedamos con su versión más nueva."""
    return (
        df.sort_values("updated_utc", ascending=False)
        .drop_duplicates(subset="id", keep="first")
        .sort_values("time_utc")
        .reset_index(drop=True)
    )


def lotes_por_particion(df: pd.DataFrame, max_particiones: int = 100) -> list[pd.DataFrame]:
    """Parte el DataFrame en lotes de hasta N días (event_date).

    Athena no deja que un MERGE escriba en más de 100 particiones a la vez
    (ICEBERG_TOO_MANY_OPEN_PARTITIONS); un backfill de meses las supera.
    """
    fechas = sorted(df["event_date"].unique())
    return [
        df[df["event_date"].isin(fechas[i : i + max_particiones])]
        for i in range(0, len(fechas), max_particiones)
    ]


def limpiar(feed: dict, ingest_ts: datetime) -> pd.DataFrame:
    """Bronze -> silver: el pipeline completo de limpieza."""
    df = feed_a_dataframe(feed)
    df = solo_sudamerica(df)
    df = calidad_minima(df)
    df = transformar(df, ingest_ts)
    return sin_duplicados(df)