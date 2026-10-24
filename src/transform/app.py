"""Lambda transform (bronze -> silver): limpia archivos de bronze y hace MERGE en la tabla Iceberg.

Tres modos (los decide la máquina de estados sismos-pipeline):
- {"key": "bronze/..."}                               -> un archivo (flujo normal y reproceso bronze_silver)
- {"dia": "AAAA-MM-DD"}                               -> todos los archivos de bronze de ese día (reproceso silver)
- {"accion": "listar_dias", "start": ..., "end": ...} -> qué días hay en bronze en ese rango (reproceso silver)
"""
import json
import os
from datetime import datetime

import awswrangler as wr
import boto3

from limpieza import limpiar, lotes_por_particion
from reproceso import dias_en_rango, unir_silver

s3 = boto3.client("s3")

BUCKET = os.environ["BUCKET_NAME"]
DATABASE = os.environ["GLUE_DATABASE"]
TABLE = os.environ["SILVER_TABLE"]
WORKGROUP = os.environ["ATHENA_WORKGROUP"]
PREFIJO_BRONZE = "bronze/sismos/"


def leer_bronze(key: str) -> tuple[dict, datetime]:
    """Lee un archivo crudo de bronze y cuándo se ingirió."""
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    feed = json.loads(obj["Body"].read())
    ingest_ts = obj["Metadata"].get("ingest_ts")
    return feed, datetime.fromisoformat(ingest_ts) if ingest_ts else obj["LastModified"]


def listar(prefijo: str, carpetas: bool = False) -> list[str]:
    """Keys (o subcarpetas, con carpetas=True) bajo un prefijo de S3."""
    paginas = s3.get_paginator("list_objects_v2").paginate(
        Bucket=BUCKET, Prefix=prefijo, **({"Delimiter": "/"} if carpetas else {})
    )
    if carpetas:
        return [p["Prefix"] for pagina in paginas for p in pagina.get("CommonPrefixes", [])]
    return [o["Key"] for pagina in paginas for o in pagina.get("Contents", []) if o["Key"].endswith(".json")]


def escribir_silver(df, request_id: str) -> int:
    """MERGE por id en silver, en lotes de hasta 100 días (máximo de particiones por query en Athena)."""
    # La tabla la crea solo el flujo de DDL (ddl/table_silver_sismos/).
    # Si no existe, fallamos en vez de dejar que awswrangler la cree a su manera.
    if not wr.catalog.does_table_exist(database=DATABASE, table=TABLE):
        raise RuntimeError(
            f"La tabla {DATABASE}.{TABLE} no existe. Créala con la máquina sismos-ddl: "
            f'{{"carpeta": "table_{TABLE}", "archivo": "001_create.sql"}}'
        )

    # MERGE por id: si el sismo ya existe lo actualiza (USGS lo corrigió), si es nuevo lo inserta
    lotes = lotes_por_particion(df)
    for n, lote in enumerate(lotes, start=1):
        wr.athena.to_iceberg(
            df=lote,
            database=DATABASE,
            table=TABLE,
            table_location=f"s3://{BUCKET}/silver/{TABLE}/",
            temp_path=f"s3://{BUCKET}/tmp/{TABLE}/{request_id}/lote_{n}/",
            partition_cols=["event_date"],
            merge_cols=["id"],
            keep_files=False,
            workgroup=WORKGROUP,
        )
    return len(lotes)


def handler(event, context):
    request_id = getattr(context, "aws_request_id", "local")

    # Reproceso silver, paso 1: qué días de ingesta hay en bronze dentro del rango
    if event.get("accion") == "listar_dias":
        dias = dias_en_rango(listar(PREFIJO_BRONZE, carpetas=True), event["start"], event["end"])
        resultado = {"dias": dias, "total_dias": len(dias)}
        print(json.dumps(resultado))
        return resultado

    # 1. Qué archivos procesar: uno ({"key"}) o todos los de un día ({"dia"})
    if "dia" in event:
        keys = listar(f"{PREFIJO_BRONZE}ingest_date={event['dia']}/")
        resultado = {"dia": event["dia"], "archivos": len(keys)}
    else:
        keys = [event["key"]]
        resultado = {"key": event["key"]}

    # 2 y 3. Leer cada archivo crudo y limpiarlo (la misma función que probamos con pytest)
    feeds = [leer_bronze(key) for key in keys]
    df = unir_silver([limpiar(feed, ingest_ts) for feed, ingest_ts in feeds])
    resultado["sismos_en_feed"] = sum(len(feed.get("features", [])) for feed, _ in feeds)
    resultado["filas_silver"] = len(df)

    if df.empty:
        resultado["mensaje"] = "Sin sismos de Sudamérica: no hay nada que escribir"
        print(json.dumps(resultado))
        return resultado

    # 4. Escribir en silver
    resultado["lotes_merge"] = escribir_silver(df, request_id)
    resultado["tabla"] = f"{DATABASE}.{TABLE}"
    print(json.dumps(resultado))
    return resultado
