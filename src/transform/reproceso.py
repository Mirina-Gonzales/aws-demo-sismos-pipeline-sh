"""Reproceso de silver desde bronze: qué días hay y cómo juntar varios archivos.

Solo usa pandas y texto: no toca AWS, así se prueba con pytest.
"""
import re

import pandas as pd

from limpieza import COLUMNAS_SILVER, sin_duplicados

# bronze/sismos/ingest_date=2026-10-01/  ->  2026-10-01
_PATRON_DIA = re.compile(r"ingest_date=(\d{4}-\d{2}-\d{2})/?$")


def dias_en_rango(prefijos: list[str], start: str, end: str) -> list[str]:
    """De las carpetas de bronze, los días de ingesta entre start (incluido) y end (no incluido)."""
    dias = []
    for prefijo in prefijos:
        encontrado = _PATRON_DIA.search(prefijo)
        if encontrado and start <= encontrado.group(1) < end:
            dias.append(encontrado.group(1))
    return sorted(set(dias))


def unir_silver(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Junta la limpieza de varios archivos de bronze y deja una fila por sismo (la más nueva)."""
    con_datos = [df for df in dfs if not df.empty]
    if not con_datos:
        return pd.DataFrame(columns=COLUMNAS_SILVER)
    return sin_duplicados(pd.concat(con_datos, ignore_index=True))
