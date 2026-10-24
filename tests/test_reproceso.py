"""Tests del reproceso de silver desde bronze. Corren sin AWS: solo pandas."""
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from limpieza import COLUMNAS_SILVER, limpiar
from reproceso import dias_en_rango, unir_silver

FIXTURES = Path(__file__).parent / "fixtures"

PREFIJOS = [
    "bronze/sismos/ingest_date=2026-09-30/",
    "bronze/sismos/ingest_date=2026-10-01/",
    "bronze/sismos/ingest_date=2026-10-02/",
    "bronze/sismos/ingest_date=2026-10-08/",
    "bronze/sismos/otra_cosa/",
]


def test_dias_en_rango_incluye_start_y_excluye_end():
    assert dias_en_rango(PREFIJOS, "2026-10-01", "2026-10-08") == ["2026-10-01", "2026-10-02"]


def test_dias_en_rango_ignora_carpetas_que_no_son_dias():
    assert dias_en_rango(PREFIJOS, "2000-01-01", "2100-01-01") == [
        "2026-09-30", "2026-10-01", "2026-10-02", "2026-10-08",
    ]


def test_dias_en_rango_sin_datos():
    assert dias_en_rango(PREFIJOS, "2025-01-01", "2025-02-01") == []


def test_unir_silver_se_queda_con_la_version_mas_nueva_entre_archivos():
    feed = json.loads((FIXTURES / "feed_sucio.json").read_text(encoding="utf-8"))
    temprano = limpiar(feed, datetime(2026, 11, 20, 14, 0, tzinfo=timezone.utc))
    tarde = limpiar(feed, datetime(2026, 11, 20, 14, 5, tzinfo=timezone.utc))
    # el mismo sismo llega en dos archivos; en el segundo USGS lo corrigió
    tarde.loc[tarde["id"] == "us001", ["mag", "updated_utc"]] = [5.0, pd.Timestamp("2026-11-21")]

    df = unir_silver([temprano, tarde])

    assert df["id"].is_unique
    assert sorted(df["id"]) == sorted(temprano["id"])
    assert df.set_index("id").loc["us001", "mag"] == 5.0


def test_unir_silver_sin_datos():
    vacio = pd.DataFrame(columns=COLUMNAS_SILVER)
    assert unir_silver([]).empty
    assert unir_silver([vacio, vacio]).empty
    assert list(unir_silver([]).columns) == COLUMNAS_SILVER
