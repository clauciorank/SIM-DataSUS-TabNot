"""
Módulo compartilhado de filtros e helpers para páginas SIM (Análise e Previsão).
Sem dependência de Streamlit para evitar execução indesejada ao importar.
"""
from pathlib import Path
from datetime import date, datetime
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
GOLD_DB = PROJECT_ROOT / "data" / "SIM" / "gold" / "obitos.duckdb"
SILVER_PATH = PROJECT_ROOT / "data" / "SIM" / "silver"

VIEW_ANALISE = "v_obitos_completo"

FAIXAS_ETARIAS = [
    "< 1 ano", "1-4 anos", "5-9 anos", "10-14 anos", "15-19 anos", "20-29 anos",
    "30-39 anos", "40-49 anos", "50-59 anos", "60-69 anos", "70-79 anos", "80+ anos", "Ignorado",
]

UFS_ORDEM = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT",
    "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
]

# Limite de itens nos multiselects de município e causa (causa básica) para evitar travar ao "selecionar todos"
MAX_SELECT_MUNICIPIO = 100
MAX_SELECT_CAUSA = 50


def _silver_parquet(name: str) -> str:
    return str((SILVER_PATH / name).resolve()).replace("\\", "/")


def _get_con():
    return duckdb.connect(str(GOLD_DB), read_only=True)


def _to_date(val):
    if val is None:
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if hasattr(val, "date") and callable(getattr(val, "date")):
        return val.date()
    return val if isinstance(val, date) else None


def _view_exists(con) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {VIEW_ANALISE} LIMIT 0").fetchall()
        return True
    except Exception:
        return False


def get_bounds_dt_obito(con) -> tuple:
    """
    Retorna (min_dt_obito, max_dt_obito) para os filtros de data.
    Usa a tabela obitos_bounds quando existir (evita varrer milhões de linhas);
    caso contrário faz min/max na view (fallback para gold antiga).
    """
    try:
        row = con.execute(
            "SELECT min_dt_obito, max_dt_obito FROM obitos_bounds LIMIT 1"
        ).fetchone()
        if row and row[0] is not None and row[1] is not None:
            return (row[0], row[1])
    except Exception:
        pass
    try:
        row = con.execute(
            f"SELECT min(dt_obito), max(dt_obito) FROM {VIEW_ANALISE}"
        ).fetchone()
        if row and row[0] is not None and row[1] is not None:
            return (row[0], row[1])
    except Exception:
        pass
    return (None, None)


def _effective_sel_for_where(sel: list, concrete_options: list, sentinel: str) -> list:
    if not sel or not concrete_options:
        return sel
    if set(sel) == set(concrete_options):
        return [sentinel]
    return sel


def _selection_for_where(sel: list, concrete_options: list, sentinel: str) -> list:
    if not sel:
        return sel
    if sentinel in sel and len(sel) > 1:
        sel = [x for x in sel if x != sentinel]
    return _effective_sel_for_where(sel, concrete_options, sentinel)


def _build_where_and_params(d1, d2, sexo_sel, faixa_sel, uf_sel, mun_sel, cap_sel, causa_sel, circ_sel=None, loc_sel=None):
    parts, params = [], []
    if d1 is not None and d2 is not None:
        parts.append("dt_obito >= ? AND dt_obito <= ?")
        params.extend([d1, d2])
    if sexo_sel and "Todos" not in sexo_sel:
        placeholders = ", ".join("?" * len(sexo_sel))
        parts.append(f"sexo_desc IN ({placeholders})")
        params.extend(sexo_sel)
    if faixa_sel and "Todas" not in faixa_sel:
        placeholders = ", ".join("?" * len(faixa_sel))
        parts.append(f"faixa_etaria IN ({placeholders})")
        params.extend(faixa_sel)
    if uf_sel and "Todas" not in uf_sel:
        placeholders = ", ".join("?" * len(uf_sel))
        parts.append(f"uf_residencia IN ({placeholders})")
        params.extend(uf_sel)
    if mun_sel and "Todos" not in mun_sel:
        placeholders = ", ".join("?" * len(mun_sel))
        parts.append(f"municipio_residencia IN ({placeholders})")
        params.extend(mun_sel)
    if cap_sel and "Todos" not in cap_sel:
        placeholders = ", ".join("?" * len(cap_sel))
        parts.append(f"causa_cid10_capitulo_desc IN ({placeholders})")
        params.extend(cap_sel)
    if causa_sel:
        codigos = [c.split(" - ", 1)[0].strip() for c in causa_sel]
        placeholders = ", ".join("?" * len(codigos))
        parts.append(f"TRIM(CAST(causa_basica AS VARCHAR)) IN ({placeholders})")
        params.extend(codigos)
    circ_sel = circ_sel or []
    loc_sel = loc_sel or []
    if circ_sel and "Todas" not in circ_sel:
        placeholders = ", ".join("?" * len(circ_sel))
        parts.append(f"circunstancia_desc IN ({placeholders})")
        params.extend(circ_sel)
    if loc_sel and "Todos" not in loc_sel:
        placeholders = ", ".join("?" * len(loc_sel))
        parts.append(f"local_ocorrencia_desc IN ({placeholders})")
        params.extend(loc_sel)
    where = " AND ".join(parts) if parts else "1=1"
    return where, params


def _opts_sexo_silver(con) -> list:
    try:
        path = _silver_parquet("legenda_sexo.parquet").replace("'", "''")
        return [r[0] for r in con.execute(f"SELECT descricao FROM read_parquet('{path}') ORDER BY 1").fetchall()]
    except Exception:
        return []


def _opts_uf_silver(con) -> list:
    try:
        path = _silver_parquet("municipios.parquet").replace("'", "''")
        return [r[0] for r in con.execute(f"SELECT DISTINCT uf FROM read_parquet('{path}') ORDER BY 1").fetchall()]
    except Exception:
        return []


def _opts_municipio_silver(con, uf_sel: list) -> list:
    try:
        path = _silver_parquet("municipios.parquet").replace("'", "''")
        if not uf_sel or "Todas" in uf_sel:
            return [r[0] for r in con.execute(f"SELECT municipio FROM read_parquet('{path}') ORDER BY 1").fetchall()]
        placeholders = ", ".join("?" * len(uf_sel))
        return [r[0] for r in con.execute(
            f"SELECT municipio FROM read_parquet('{path}') WHERE uf IN ({placeholders}) ORDER BY 1",
            list(uf_sel),
        ).fetchall()]
    except Exception:
        return []


def _depara_path() -> str:
    p = SILVER_PATH / "cid10_depara.parquet"
    return _silver_parquet("cid10_depara.parquet").replace("'", "''") if p.exists() else ""


def _opts_capitulos_from_opcoes(con) -> list:
    """Lê capítulos da tabela de opções (preenchida no build); evita varrer a base."""
    try:
        rows = con.execute(
            "SELECT causa_cid10_capitulo_desc FROM obitos_opcoes_capitulos ORDER BY 1"
        ).fetchall()
        return [r[0] for r in rows] if rows else []
    except Exception:
        return []


def _opts_capitulos_silver(con) -> list:
    cap_from_opcoes = _opts_capitulos_from_opcoes(con)
    if cap_from_opcoes:
        return cap_from_opcoes
    d_path = _depara_path()
    if d_path:
        try:
            return [
                r[0]
                for r in con.execute(
                    f"SELECT DISTINCT capitulo_descricao FROM read_parquet('{d_path}') "
                    f"WHERE TRIM(COALESCE(CAST(capitulo_descricao AS VARCHAR), '')) != '' ORDER BY 1"
                ).fetchall()
            ]
        except Exception:
            pass
    try:
        path = _silver_parquet("legenda_cid10_capitulo.parquet").replace("'", "''")
        return [
            r[0] for r in con.execute(
                f"SELECT descricao FROM (SELECT descricao, MIN(letra) AS ord FROM read_parquet('{path}') GROUP BY descricao) AS t ORDER BY ord"
            ).fetchall()
        ]
    except Exception:
        return []


def _opts_causas_from_opcoes(con, cap_sel: list) -> list:
    """Lê causas da tabela de opções (preenchida no build); evita varrer a base."""
    def fmt(cod: str, desc: str) -> str:
        c = (cod or "").strip()
        d = (desc or "").strip()
        return f"{c} - {d}" if d else c or ""

    try:
        if not cap_sel or "Todos" in cap_sel:
            rows = con.execute(
                "SELECT causa_basica, causa_cid10_desc FROM obitos_opcoes_causas ORDER BY causa_basica"
            ).fetchall()
        else:
            placeholders = ", ".join("?" * len(cap_sel))
            rows = con.execute(
                f"SELECT causa_basica, causa_cid10_desc FROM obitos_opcoes_causas "
                f"WHERE causa_cid10_capitulo_desc IN ({placeholders}) ORDER BY causa_basica",
                list(cap_sel),
            ).fetchall()
        if rows:
            return [fmt(str(r[0]), str(r[1]) if r[1] is not None else "") for r in rows]
    except Exception:
        pass
    return []


def _opts_causas_silver(con, cap_sel: list) -> list:
    def fmt(cod: str, desc: str) -> str:
        c = (cod or "").strip()
        d = (desc or "").strip()
        return f"{c} - {d}" if d else c or ""

    causas_from_opcoes = _opts_causas_from_opcoes(con, cap_sel)
    if causas_from_opcoes:
        return causas_from_opcoes
    d_path = _depara_path()
    if d_path:
        try:
            if not cap_sel or "Todos" in cap_sel:
                rows = con.execute(
                    f"SELECT codigo, descricao FROM read_parquet('{d_path}') "
                    f"WHERE TRIM(COALESCE(CAST(codigo AS VARCHAR), '')) != '' ORDER BY codigo"
                ).fetchall()
            else:
                placeholders = ", ".join("?" * len(cap_sel))
                rows = con.execute(
                    f"SELECT codigo, descricao FROM read_parquet('{d_path}') "
                    f"WHERE capitulo_descricao IN ({placeholders}) "
                    f"AND TRIM(COALESCE(CAST(codigo AS VARCHAR), '')) != '' ORDER BY codigo",
                    list(cap_sel),
                ).fetchall()
            if rows:
                return [fmt(str(cod), str(desc)) for cod, desc in rows]
        except Exception:
            pass

    try:
        path_cap = _silver_parquet("legenda_cid10_capitulo.parquet").replace("'", "''")
        path_causa = _silver_parquet("legenda_cid10_causa.parquet").replace("'", "''")
        if not cap_sel or "Todos" in cap_sel:
            rows = con.execute(
                f"SELECT codigo, descricao FROM read_parquet('{path_causa}') "
                f"WHERE TRIM(COALESCE(CAST(codigo AS VARCHAR), '')) != '' ORDER BY codigo"
            ).fetchall()
        else:
            placeholders = ", ".join("?" * len(cap_sel))
            letras_sql = f"SELECT DISTINCT letra FROM read_parquet('{path_cap}') WHERE descricao IN ({placeholders})"
            letras = [r[0] for r in con.execute(letras_sql, list(cap_sel)).fetchall()]
            if not letras:
                return []
            ph = ", ".join("?" * len(letras))
            rows = con.execute(
                f"SELECT codigo, descricao FROM read_parquet('{path_causa}') "
                f"WHERE SUBSTR(TRIM(COALESCE(CAST(codigo AS VARCHAR), '')), 1, 1) IN ({ph}) "
                f"AND TRIM(COALESCE(CAST(codigo AS VARCHAR), '')) != '' ORDER BY codigo"
            ).fetchall()
        if rows:
            return [fmt(str(cod), str(desc)) for cod, desc in rows]
    except Exception:
        pass
    return []
