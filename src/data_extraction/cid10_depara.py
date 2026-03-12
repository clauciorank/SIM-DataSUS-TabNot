"""
Depara CID-10: código da causa → descrição da causa e descrição do capítulo.
Construído a partir dos CSV oficiais do Datasus (CID10CSV.ZIP), usando intervalo
CATINIC–CATFIM para atribuir cada código ao capítulo correto (evita sobreposição
por letra, ex.: D00–D48 neoplasias, D50–D89 doenças do sangue).

Prioridade das fontes: (1) CSVs ou ZIP na pasta reference/cid10 (versionável, sem download);
(2) download do Datasus.
"""
from pathlib import Path
from typing import Optional, Tuple

import io
import zipfile

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SILVER_PATH = PROJECT_ROOT / "data" / "SIM" / "silver"
# Pasta versionável: coloque CID10CSV.zip ou os CSVs aqui para não precisar baixar
REFERENCE_CID10_DIR = PROJECT_ROOT / "reference" / "cid10"
DATASUS_CID10_ZIP = "http://www2.datasus.gov.br/cid10/V2008/downloads/CID10CSV.zip"
ENCODING = "iso-8859-1"
SEP = ";"

NAMES_CAPITULOS = ("CID-10-CAPITULOS.CSV", "CID10-CAPITULOS.CSV")
NAMES_SUBCATEGORIAS = ("CID-10-SUBCATEGORIAS.CSV", "CID10-SUBCATEGORIAS.CSV")
NAME_ZIP = "CID10CSV.zip"


def _normalize_codigo_for_compare(raw: str) -> str:
    """Coloca código CID-10 em forma comparável: LNN.N (ex.: A00.0, B99.9)."""
    raw = (raw or "").strip().upper()
    if not raw:
        return ""
    # Já tem ponto: garantir 5 chars (LNN.N)
    if "." in raw:
        a, _, b = raw.partition(".")
        a = (a + "  ")[:3]
        b = (b + "0")[0]
        return f"{a}.{b}"
    # Sem ponto: 3 chars -> LNN.0; 4 chars -> LNN.N
    raw = (raw + "   ")[:4]
    if len(raw) == 3:
        return f"{raw}.0"
    return f"{raw[:3]}.{raw[3]}"


def _codigo_canonico(subcat: str) -> str:
    """Formato canônico para armazenar/join: com ponto se 4 caracteres (ex.: A90.0)."""
    subcat = (subcat or "").strip().upper()
    if not subcat:
        return ""
    if len(subcat) == 4 and subcat[3].isalnum():
        return f"{subcat[:3]}.{subcat[3]}"
    return subcat[:3] if len(subcat) >= 3 else subcat


def _build_depara_from_dfs(capitulos: pd.DataFrame, subcategorias: pd.DataFrame) -> pd.DataFrame:
    """Monta tabela codigo -> descricao, capitulo_descricao usando intervalo dos capítulos."""
    # Colunas esperadas: CAPITULOS: NUMCAP, CATINIC, CATFIM, DESCRICAO
    cap_cols = {"CATINIC", "CATFIM", "DESCRICAO"}
    sub_cols = {"SUBCAT", "DESCRICAO"}
    cap_ren = {"DESCRICAO": "capitulo_descricao"}
    sub_ren = {"DESCRICAO": "descricao"}
    if not cap_cols.issubset(capitulos.columns):
        cap_avail = set(capitulos.columns)
        if "DESCRICAO" in cap_avail and ("CATINIC" in cap_avail or "CatInic" in capitulos.columns):
            pass
        else:
            raise ValueError(f"CAPITULOS precisa de {cap_cols}; tem {cap_avail}")
    if not sub_cols.issubset(subcategorias.columns):
        raise ValueError(f"SUBCATEGORIAS precisa de {sub_cols}; tem set(subcategorias.columns)")

    capitulos = capitulos.rename(columns=cap_ren)
    subcategorias = subcategorias.rename(columns=sub_ren)
    cap = capitulos[["CATINIC", "CATFIM", "capitulo_descricao"]].copy()
    cap["_inic"] = cap["CATINIC"].astype(str).apply(_normalize_codigo_for_compare)
    cap["_fim"] = cap["CATFIM"].astype(str).apply(_normalize_codigo_for_compare)

    sub = subcategorias[["SUBCAT", "descricao"]].copy()
    sub["codigo"] = sub["SUBCAT"].astype(str).apply(_codigo_canonico)
    sub["_key"] = sub["SUBCAT"].astype(str).apply(_normalize_codigo_for_compare)
    sub = sub[sub["codigo"].str.len() >= 3].drop_duplicates(subset=["codigo"], keep="first")

    rows = []
    for _, r in sub.iterrows():
        cod, key, desc = r["codigo"], r["_key"], r["descricao"]
        if not key:
            continue
        for _, c in cap.iterrows():
            if c["_inic"] <= key <= c["_fim"]:
                rows.append({"codigo": cod, "descricao": (desc or "").strip(), "capitulo_descricao": (c["capitulo_descricao"] or "").strip()})
                break
        else:
            rows.append({"codigo": cod, "descricao": (desc or "").strip(), "capitulo_descricao": ""})

    return pd.DataFrame(rows)


def _normalize_column_names(capitulos: pd.DataFrame, subcategorias: pd.DataFrame) -> None:
    """Normaliza nomes de colunas (in-place)."""
    capitulos.columns = [c.strip().upper() for c in capitulos.columns]
    subcategorias.columns = [c.strip().upper() for c in subcategorias.columns]
    if "DESCRICAO" not in capitulos.columns and "DESCRIÇÃO" in capitulos.columns:
        capitulos.rename(columns={"DESCRIÇÃO": "DESCRICAO"}, inplace=True)
    if "DESCRICAO" not in subcategorias.columns and "DESCRIÇÃO" in subcategorias.columns:
        subcategorias.rename(columns={"DESCRIÇÃO": "DESCRICAO"}, inplace=True)


def _load_from_reference_dir() -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
    """
    Carrega CAPITULOS e SUBCATEGORIAS a partir de reference/cid10.
    Aceita: (1) CID10CSV.zip na pasta; (2) CID-10-CAPITULOS.CSV e CID-10-SUBCATEGORIAS.CSV na pasta.
    Retorna (capitulos, subcategorias) ou None se não houver fonte local.
    """
    if not REFERENCE_CID10_DIR.is_dir():
        return None

    # 1) Tentar ZIP
    zip_path = REFERENCE_CID10_DIR / NAME_ZIP
    if not zip_path.is_file():
        for name in REFERENCE_CID10_DIR.iterdir():
            if name.suffix.upper() == ".ZIP" and "CID10" in name.name.upper():
                zip_path = name
                break
        else:
            zip_path = None
    if zip_path is not None and zip_path.is_file():
        try:
            with zipfile.ZipFile(zip_path, "r") as z:
                names = {n.upper(): n for n in z.namelist()}
                cap_name = next((names.get(n) for n in NAMES_CAPITULOS if n.upper() in names), None)
                sub_name = next((names.get(n) for n in NAMES_SUBCATEGORIAS if n.upper() in names), None)
                if cap_name and sub_name:
                    with z.open(cap_name) as f:
                        capitulos = pd.read_csv(f, sep=SEP, encoding=ENCODING, dtype=str, keep_default_na=False)
                    with z.open(sub_name) as f:
                        subcategorias = pd.read_csv(f, sep=SEP, encoding=ENCODING, dtype=str, keep_default_na=False)
                    _normalize_column_names(capitulos, subcategorias)
                    return (capitulos, subcategorias)
        except Exception:
            pass
        return None

    # 2) Tentar CSVs soltos
    cap_file = sub_file = None
    for f in REFERENCE_CID10_DIR.iterdir():
        if not f.is_file():
            continue
        u = f.name.upper()
        if u in (n.upper() for n in NAMES_CAPITULOS):
            cap_file = f
        if u in (n.upper() for n in NAMES_SUBCATEGORIAS):
            sub_file = f
    if cap_file and sub_file:
        try:
            capitulos = pd.read_csv(cap_file, sep=SEP, encoding=ENCODING, dtype=str, keep_default_na=False)
            subcategorias = pd.read_csv(sub_file, sep=SEP, encoding=ENCODING, dtype=str, keep_default_na=False)
            _normalize_column_names(capitulos, subcategorias)
            return (capitulos, subcategorias)
        except Exception:
            pass
    return None


def build_cid10_depara(silver_path: Optional[Path] = None, zip_url: Optional[str] = None) -> Path:
    """
    Monta cid10_depara.parquet na silver a partir dos CSV oficiais.
    Ordem: (1) CSVs ou ZIP em reference/cid10; (2) zip_url local; (3) download Datasus.
    Retorna o path do parquet.
    """
    import urllib.request

    silver_path = Path(silver_path or SILVER_PATH)
    silver_path.mkdir(parents=True, exist_ok=True)
    out_path = silver_path / "cid10_depara.parquet"

    loaded = _load_from_reference_dir()
    if loaded is None and zip_url:
        # zip_url pode ser path local (ex.: /path/foo.zip ou C:\path\foo.zip)
        try:
            path_or_url = Path(zip_url)
        except TypeError:
            path_or_url = None
        if path_or_url is not None and path_or_url.is_file():
            with zipfile.ZipFile(path_or_url, "r") as z:
                names = {n.upper(): n for n in z.namelist()}
                cap_name = next((names.get(n) for n in NAMES_CAPITULOS if n.upper() in names), None)
                sub_name = next((names.get(n) for n in NAMES_SUBCATEGORIAS if n.upper() in names), None)
                if cap_name and sub_name:
                    with z.open(cap_name) as f:
                        capitulos = pd.read_csv(f, sep=SEP, encoding=ENCODING, dtype=str, keep_default_na=False)
                    with z.open(sub_name) as f:
                        subcategorias = pd.read_csv(f, sep=SEP, encoding=ENCODING, dtype=str, keep_default_na=False)
                    _normalize_column_names(capitulos, subcategorias)
                    loaded = (capitulos, subcategorias)

    if loaded is None:
        try:
            with urllib.request.urlopen(zip_url or DATASUS_CID10_ZIP, timeout=60) as resp:
                data = resp.read()
        except Exception as e:
            raise FileNotFoundError(
                f"Não há fonte em {REFERENCE_CID10_DIR} e não foi possível baixar {DATASUS_CID10_ZIP}. "
                "Coloque CID10CSV.zip ou os CSVs em reference/cid10 ou verifique a conexão."
            ) from e
        with zipfile.ZipFile(io.BytesIO(data), "r") as z:
            names = {n.upper(): n for n in z.namelist()}
            cap_name = next((names.get(n) for n in NAMES_CAPITULOS if n.upper() in names), None)
            sub_name = next((names.get(n) for n in NAMES_SUBCATEGORIAS if n.upper() in names), None)
            if not cap_name or not sub_name:
                raise ValueError(
                    f"ZIP não contém CAPITULOS e/ou SUBCATEGORIAS; arquivos: {list(names.keys())}"
                )
            with z.open(cap_name) as f:
                capitulos = pd.read_csv(f, sep=SEP, encoding=ENCODING, dtype=str, keep_default_na=False)
            with z.open(sub_name) as f:
                subcategorias = pd.read_csv(f, sep=SEP, encoding=ENCODING, dtype=str, keep_default_na=False)
        _normalize_column_names(capitulos, subcategorias)
        loaded = (capitulos, subcategorias)

    capitulos, subcategorias = loaded
    depara = _build_depara_from_dfs(capitulos, subcategorias)
    depara.to_parquet(out_path, index=False)
    return out_path


def ensure_cid10_depara(silver_path: Optional[Path] = None) -> bool:
    """
    Garante que cid10_depara.parquet exista na silver. Se não existir, tenta
    construir via build_cid10_depara. Retorna True se o depara existe (ou foi
    criado), False se não foi possível (view gold pode usar fallback por letra).
    """
    silver_path = Path(silver_path or SILVER_PATH)
    out_path = silver_path / "cid10_depara.parquet"
    if out_path.exists():
        try:
            df = pd.read_parquet(out_path)
            if df is not None and len(df) > 0 and "codigo" in df.columns and "capitulo_descricao" in df.columns:
                return True
        except Exception:
            pass
        out_path.unlink(missing_ok=True)
    try:
        build_cid10_depara(silver_path=silver_path)
        return True
    except Exception:
        return False
