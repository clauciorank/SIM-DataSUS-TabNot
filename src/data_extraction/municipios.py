"""
Gera tabela de municípios (geocode IBGE → nome) para enriquecimento dos dados.
Usa o dicionário do pysus.
"""
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
MUNICIPIOS_PARQUET = PROJECT_ROOT / "data" / "SIM" / "silver" / "municipios.parquet"


def build_municipios_table(output_path: Path | None = None) -> str:
    """
    Cria parquet de municípios a partir do pysus.
    Colunas: codigo (6 dígitos), geocodigo (7 dígitos), municipio, uf.
    Retorna caminho do arquivo.
    """
    from pysus.utilities.brasil import MUNICIPALITIES

    rows = []
    for m in MUNICIPALITIES:
        geo = str(m["geocodigo"]).zfill(7)
        codigo_6 = geo[:6]  # CODMUN no SIM usa 6 dígitos
        uf_map = {
            11: "RO", 12: "AC", 13: "AM", 14: "RR", 15: "PA", 16: "AP", 17: "TO",
            21: "MA", 22: "PI", 23: "CE", 24: "RN", 25: "PB", 26: "PE", 27: "AL",
            28: "SE", 29: "BA", 31: "MG", 32: "ES", 33: "RJ", 35: "SP", 41: "PR",
            42: "SC", 43: "RS", 50: "MS", 51: "MT", 52: "GO", 53: "DF",
        }
        uf = uf_map.get(m.get("codigo_uf", 0), "??")
        rows.append({
            "codigo": codigo_6,
            "geocodigo": int(geo),
            "municipio": m["municipio"],
            "uf": uf,
        })

    df = pd.DataFrame(rows)
    out = Path(output_path or MUNICIPIOS_PARQUET)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    return str(out)
