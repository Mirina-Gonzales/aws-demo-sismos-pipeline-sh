"""Tests de la limpieza bronze -> silver. Corren sin AWS: solo pandas."""
import json
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from limpieza import COLUMNAS_SILVER, feed_a_dataframe, limpiar, lotes_por_particion

FIXTURES = Path(__file__).parent / "fixtures"
INGEST_TS = datetime(2026, 11, 20, 14, 30, tzinfo=timezone.utc)


def cargar(nombre: str) -> dict:
    return json.loads((FIXTURES / nombre).read_text(encoding="utf-8"))


def silver_de_prueba() -> pd.DataFrame:
    return limpiar(cargar("feed_sucio.json"), INGEST_TS)


# ---------- Paso 1: aplanar ----------

def test_aplanar_mantiene_todas_las_filas():
    df = feed_a_dataframe(cargar("feed_sucio.json"))
    assert len(df) == 10  # bronze no se filtra: llega todo


def test_coordenadas_incompletas_no_rompen():
    df = feed_a_dataframe(cargar("feed_sucio.json"))
    fila = df[df["id"] == "us007"].iloc[0]
    assert pd.isna(fila["depth_km"])


# ---------- Pasos 2 y 3: filtrar y calidad ----------

def test_solo_quedan_sismos_de_sudamerica():
    df = silver_de_prueba()
    assert "ak002" not in set(df["id"])            # Alaska fuera
    assert "pa009" not in set(df["id"])            # Panamá fuera (la caja de la ingesta lo trae)
    assert "us007" not in set(df["id"])            # sin lugar fuera
    assert "pe008" in set(df["id"])                # Perú dentro


def test_descarta_sismos_sin_magnitud():
    df = silver_de_prueba()
    assert "us005" not in set(df["id"])


def test_resultado_esperado():
    df = silver_de_prueba()
    assert sorted(df["id"]) == ["pe008", "us001", "us003", "us004", "us006"]


# ---------- Paso 4: transformar ----------

def test_esquema_silver():
    df = silver_de_prueba()
    assert list(df.columns) == COLUMNAS_SILVER
    assert df["mag"].dtype == "float64"
    assert df["tsunami"].dtype == "int64"
    assert str(df["time_utc"].dtype) == "datetime64[us]"
    assert str(df["time_cl"].dtype) == "datetime64[us]"


def test_hora_utc_y_hora_chile():
    fila = silver_de_prueba().set_index("id").loc["us001"]
    # 14:15 UTC = 11:15 en Chile (noviembre, horario de verano UTC-3)
    assert fila["time_utc"] == pd.Timestamp("2026-11-20 14:15:00")
    assert fila["time_cl"] == pd.Timestamp("2026-11-20 11:15:00")


def test_event_date_usa_la_fecha_de_chile():
    fila = silver_de_prueba().set_index("id").loc["us003"]
    # 02:30 UTC del 20 = 23:30 del 19 en Chile -> el sismo es del día 19
    assert fila["event_date"] == date(2026, 11, 19)


def test_region_extraida_del_texto():
    regiones = silver_de_prueba().set_index("id")["region"]
    assert regiones["us001"] == "Calama"
    assert regiones["us003"] == "Coquimbo"
    assert regiones["us006"] == "San Pedro de Atacama"            # limpia espacios
    assert regiones["us004"] == "Chile-Argentina border region"   # sin coma: deja el texto
    assert regiones["pe008"] == "Arequipa"


def test_pais_extraido_del_texto():
    paises = silver_de_prueba().set_index("id")["pais"]
    assert paises["us001"] == "Chile"
    assert paises["us006"] == "Chile"       # con espacios de más
    assert paises["pe008"] == "Peru"
    assert paises["us004"] == "Chile"       # "Chile-Argentina border region": el primero que aparece


def test_valores_por_defecto():
    fila = silver_de_prueba().set_index("id").loc["us004"]
    assert fila["tsunami"] == 0                       # tsunami nulo -> 0
    assert fila["updated_utc"] == fila["time_utc"]    # updated nulo -> hora del sismo
    assert fila["magtype"] == "mb"


def test_ingest_ts_en_todas_las_filas():
    df = silver_de_prueba()
    assert (df["ingest_ts"] == pd.Timestamp("2026-11-20 14:30:00")).all()


# ---------- Paso 5: deduplicar ----------

def test_sin_duplicados():
    df = silver_de_prueba()
    assert df["id"].is_unique


def test_se_queda_con_la_version_mas_nueva():
    fila = silver_de_prueba().set_index("id").loc["us001"]
    # USGS corrigió el sismo: la versión nueva dice 4.6 (mb), la vieja 4.4 (ml)
    assert fila["mag"] == 4.6
    assert fila["magtype"] == "mb"


# ---------- Lotes para el MERGE ----------

def test_feed_normal_es_un_solo_lote():
    df = silver_de_prueba()
    lotes = lotes_por_particion(df)
    assert len(lotes) == 1
    assert len(lotes[0]) == len(df)


def test_backfill_largo_se_parte_en_lotes_de_100_dias():
    # 250 días distintos -> 100 + 100 + 50
    fechas = pd.date_range("2026-01-01", periods=250, freq="D").date
    df = pd.DataFrame({"id": [f"s{i}" for i in range(250)], "event_date": fechas})
    lotes = lotes_por_particion(df)
    assert [lote["event_date"].nunique() for lote in lotes] == [100, 100, 50]
    assert sum(len(lote) for lote in lotes) == len(df)                  # no se pierde ninguna fila
    assert pd.concat(lotes)["id"].is_unique                             # ni se repite


def test_un_dia_nunca_queda_partido_entre_lotes():
    # varios sismos el mismo día: todos van al mismo lote
    fechas = list(pd.date_range("2026-01-01", periods=101, freq="D").date) * 3
    df = pd.DataFrame({"id": [f"s{i}" for i in range(len(fechas))], "event_date": fechas})
    lotes = lotes_por_particion(df)
    dias_por_lote = [set(lote["event_date"]) for lote in lotes]
    assert not dias_por_lote[0] & dias_por_lote[1]


# ---------- Casos borde ----------

def test_feed_vacio():
    df = limpiar({"type": "FeatureCollection", "features": []}, INGEST_TS)
    assert df.empty
    assert list(df.columns) == COLUMNAS_SILVER


def test_archivos_reales_de_bronze():
    """Corre la limpieza sobre archivos reales copiados de bronze (tests/fixtures/feed_real*.json)."""
    reales = sorted(FIXTURES.glob("feed_real*.json"))
    if not reales:
        pytest.skip("Copia un archivo de bronze como tests/fixtures/feed_real.json")
    for archivo in reales:
        df = limpiar(json.loads(archivo.read_text(encoding="utf-8")), INGEST_TS)
        assert list(df.columns) == COLUMNAS_SILVER
        assert df["id"].is_unique
        assert df["pais"].notna().all()
        assert df["mag"].notna().all()