"""Lambda de ingesta (bronze): descarga sismos de USGS y los guarda tal cual en S3.

Dos modos:
- Sin parámetros   -> feed de las últimas 24 h (el flujo normal, cada hora)
- {"start", "end"} -> carga histórica (reproceso) de Sudamérica entre esas fechas
"""
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")

BUCKET = os.environ["BUCKET_NAME"]
FEED_URL = os.environ["FEED_URL"]
QUERY_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

# Caja que contiene a Sudamérica (incluye mar). También trae Panamá y el Caribe:
# los deja fuera limpieza.py (solo_sudamerica)
SUDAMERICA = {"minlatitude": -56, "maxlatitude": 13, "minlongitude": -82, "maxlongitude": -34}


def armar_url(event: dict) -> tuple[str, str]:
    """Devuelve (url, prefijo del archivo) según el modo."""
    if event.get("start"):
        params = {
            "format": "geojson",
            "starttime": event["start"],
            "endtime": event["end"],
            "minmagnitude": event.get("min_mag", 2.5),
            "orderby": "time-asc",
            **SUDAMERICA,
        }
        url = f"{QUERY_URL}?{urllib.parse.urlencode(params)}"
        return url, f"backfill_{event['start']}_{event['end']}"
    return FEED_URL, "feed"


def handler(event, context):
    event = event or {}
    ahora = datetime.now(timezone.utc)
    url, prefijo = armar_url(event)

    # 1. Extraer: descargar de USGS
    req = urllib.request.Request(url, headers={"User-Agent": "scd-chile-demo/1.0"})
    with urllib.request.urlopen(req, timeout=50) as resp:
        cuerpo = resp.read()

    # Validamos que sea JSON antes de guardarlo (si no, la Lambda falla y Step Functions reintenta)
    feed = json.loads(cuerpo)
    sismos_en_feed = len(feed.get("features", []))

    # 2. Cargar en bronze: el archivo va SIN tocar, particionado por fecha de ingesta
    key = f"bronze/sismos/ingest_date={ahora:%Y-%m-%d}/{prefijo}_{ahora:%H%M%S}Z.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=cuerpo,
        ContentType="application/json",
        Metadata={"ingest_ts": ahora.isoformat(), "source": prefijo},
    )

    # 3. Devolver dónde quedó: Step Functions se lo pasa a la Lambda transform
    resultado = {
        "bucket": BUCKET,
        "key": key,
        "sismos_en_feed": sismos_en_feed,
        "ingest_ts": ahora.isoformat(),
    }
    print(json.dumps(resultado))
    return resultado