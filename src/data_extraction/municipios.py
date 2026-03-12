"""
Gera tabela de municípios (geocode IBGE → nome) para enriquecimento dos dados.
Prioridade: (1) CSVs em reference/municipios (versionável, sem download);
(2) pysus.utilities.brasil.MUNICIPALITIES.
"""
from pathlib import Path
from typing import Optional

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
MUNICIPIOS_PARQUET = PROJECT_ROOT / "data" / "SIM" / "silver" / "municipios.parquet"
REFERENCE_MUNICIPIOS_DIR = PROJECT_ROOT / "reference" / "municipios"
MUNICIPIOS_CSV = "municipios.csv"


def _load_from_reference() -> Optional[pd.DataFrame]:
    """
    Carrega municípios de reference/municipios/municipios.csv se existir.
    Esperado: codigo, geocodigo, municipio, uf (codigo 6 dígitos, geocodigo 7).
    """
    csv_path = REFERENCE_MUNICIPIOS_DIR / MUNICIPIOS_CSV
    if not csv_path.is_file():
        return None
    try:
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        df = df.rename(columns=str.strip)
        required = {"codigo", "geocodigo", "municipio", "uf"}
        if not required.issubset(df.columns):
            return None
        # codigo: 6 dígitos (SIM); se fonte tiver 7, usa os 6 primeiros; senão preenche à esquerda
        cod_raw = df["codigo"].astype(str).str.strip().str.replace(r"\D", "", regex=True)
        df["codigo"] = cod_raw.str[:6].str.zfill(6)
        df["geocodigo"] = pd.to_numeric(df["geocodigo"].astype(str).str.strip(), errors="coerce").fillna(0).astype("int64")
        df["municipio"] = df["municipio"].astype(str).str.strip()
        df["uf"] = df["uf"].astype(str).str.strip().str.upper()
        df = df[(df["codigo"].str.len() >= 6) & (df["codigo"] != "000000")].copy()
        if df.empty:
            return None
        return df[["codigo", "geocodigo", "municipio", "uf"]]
    except Exception:
        return None


def _build_from_pysus() -> pd.DataFrame:
    """Monta DataFrame de municípios a partir do pysus."""
    from pysus.utilities.brasil import MUNICIPALITIES

    uf_map = {
        11: "RO", 12: "AC", 13: "AM", 14: "RR", 15: "PA", 16: "AP", 17: "TO",
        21: "MA", 22: "PI", 23: "CE", 24: "RN", 25: "PB", 26: "PE", 27: "AL",
        28: "SE", 29: "BA", 31: "MG", 32: "ES", 33: "RJ", 35: "SP", 41: "PR",
        42: "SC", 43: "RS", 50: "MS", 51: "MT", 52: "GO", 53: "DF",
    }
    rows = []
    for m in MUNICIPALITIES:
        geo = str(m["geocodigo"]).zfill(7)
        codigo_6 = geo[:6]
        uf = uf_map.get(m.get("codigo_uf", 0), "??")
        rows.append({
            "codigo": codigo_6,
            "geocodigo": int(geo),
            "municipio": m["municipio"],
            "uf": uf,
        })
    return pd.DataFrame(rows)


def build_municipios_table(output_path: Optional[Path] = None) -> str:
    """
    Cria parquet de municípios. Ordem: (1) reference/municipios/municipios.csv;
    (2) pysus. Colunas: codigo (6 dígitos), geocodigo (7), municipio, uf.
    Retorna caminho do arquivo.
    """
    df = _load_from_reference()
    if df is None:
        df = _build_from_pysus()
    out = Path(output_path or MUNICIPIOS_PARQUET)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    return str(out)


def export_municipios_to_reference() -> Path:
    """
    Exporta a lista de municípios (pysus) para reference/municipios/municipios.csv.
    Útil para gerar o CSV estático uma vez e commitar (assim ninguém precisa do pysus).
    Retorna o path do CSV gravado.
    """
    df = _build_from_pysus()
    REFERENCE_MUNICIPIOS_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = REFERENCE_MUNICIPIOS_DIR / MUNICIPIOS_CSV
    df.to_csv(csv_path, index=False, encoding="utf-8")
    return csv_path
