"""
Resolução de causas/doenças CID-10 para o agente: busca fuzzy no depara oficial
e formatação de códigos para o contexto do planejador (evita ILIKE e invenção de códigos).
"""
import re
from pathlib import Path
from typing import List, Dict, Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SILVER_PATH = PROJECT_ROOT / "data" / "SIM" / "silver"
DEPARA_FILENAME = "cid10_depara.parquet"

_depara_cache: Optional[Any] = None


def _load_depara() -> Optional[Any]:
    """Carrega cid10_depara.parquet (cache em memória). Garante existência via ensure_cid10_depara."""
    global _depara_cache
    if _depara_cache is not None:
        return _depara_cache
    try:
        from src.data_extraction.cid10_depara import ensure_cid10_depara
        if not ensure_cid10_depara(SILVER_PATH):
            return None
        import pandas as pd
        path = SILVER_PATH / DEPARA_FILENAME
        if not path.is_file():
            return None
        df = pd.read_parquet(path)
        if df is None or df.empty or "codigo" not in df.columns or "descricao" not in df.columns:
            return None
        _depara_cache = df
        return _depara_cache
    except Exception:
        return None


def _desc_matches_all_words(desc: str, termo: str) -> bool:
    """
    True se a descrição contém todas as palavras relevantes do termo (evita ex.: tuberculose óssea
    para pergunta 'neoplasia óssea' — exige 'neoplasia' também na descrição).
    """
    if not termo or not desc:
        return True
    desc_low = desc.lower()
    # Palavras do termo (len >= 2); sinônimos para aceitar "óssea" em desc "osso"
    words = [w for w in re.findall(r"\w+", termo.lower()) if len(w) >= 2]
    if not words:
        return True
    synonyms: Dict[str, List[str]] = {
        "óssea": ["óssea", "ossea", "osso", "ósseo"], "ossea": ["ossea", "osso"],
        "ósea": ["ósea", "osea", "osso"], "osea": ["osea", "osso"],
    }
    for w in words:
        variants = synonyms.get(w, [w])
        if not any(v in desc_low for v in variants):
            return False
    return True


def search_cid10(
    termo: str,
    limit: int = 20,
    min_score: int = 70,
) -> List[Dict[str, str]]:
    """
    Busca fuzzy no depara CID-10 por descricao (e capitulo_descricao para recall).
    Retorna lista de {codigo, descricao, capitulo_descricao} ordenada por score.
    Para termos com 2+ palavras, exige que todas as palavras apareçam na descrição (evita códigos irrelevantes).
    """
    termo = (termo or "").strip()
    if len(termo) < 2:
        return []
    try:
        from rapidfuzz import process
    except ImportError:
        return []
    df = _load_depara()
    if df is None or df.empty:
        return []
    df = df.copy()
    df["_search"] = (
        df["descricao"].fillna("").astype(str)
        + " "
        + df.get("capitulo_descricao", "").fillna("").astype(str)
    ).str.strip()
    choices = df["_search"].tolist()
    results = process.extract(termo, choices, limit=limit * 2, score_cutoff=min_score)
    if not results:
        return []
    termo_low = termo.lower()
    words_count = len([w for w in re.findall(r"\w+", termo_low) if len(w) >= 2])
    require_all_words = words_count >= 2
    seen_codigos = set()
    out = []
    for match_str, score, idx in results:
        row = df.iloc[idx]
        cod = str(row["codigo"]).strip()
        if cod in seen_codigos:
            continue
        desc = str(row["descricao"]).strip() if "descricao" in row else ""
        if score < 75 and termo_low not in desc.lower():
            continue
        # Termos com 2+ palavras: descrição deve conter todas as palavras (evita tuberculose óssea para "neoplasia óssea")
        if require_all_words and not _desc_matches_all_words(desc, termo):
            continue
        seen_codigos.add(cod)
        out.append({
            "codigo": cod,
            "descricao": desc,
            "capitulo_descricao": str(row.get("capitulo_descricao", "") or "").strip(),
        })
        if len(out) >= limit:
            break
    return out


# Sinônimos para busca de capítulo: termo da pergunta -> palavra que aparece no nome oficial do capítulo CID-10
CHAPTER_SEARCH_SYNONYMS: Dict[str, str] = {
    "cardiovascular": "circulatório",
    "cardiovasculares": "circulatório",
    "cardíaco": "circulatório",
    "cardiaco": "circulatório",
    "coração": "circulatório",
    "coracao": "circulatório",
    "respiratóri": "respiratório",  # cobre respiratória, respiratório
    "digestiv": "digestivo",
    "geniturinário": "geniturinário",
    "geniturinario": "geniturinário",
}

# Termos amplos: quando a pergunta menciona estes, preferir filtro por CAPÍTULO (causa_cid10_capitulo_desc)
# em vez de lista de códigos específicos (evita poucos códigos e resultado vazio).
CHAPTER_LIKE_TERMS = frozenset([
    "cardiovascular", "cardiovasculares", "cardíaco", "cardiaco", "coração", "coracao",
    "circulatório", "circulatorias", "circulatória",
    "respiratóri", "respiratorio", "respiratória", "respiratorias",  # respiratório, doenças respiratórias
    "digestiv", "digestivo", "digestivas",
    "neoplasia", "neoplasias", "câncer", "cancer",
    "doença cardiovascular", "doencas cardiovasculares", "doença circulatória",
])

# Termos específicos que devem ser resolvidos por capítulo (evita misturar códigos de outras doenças)
# Ex.: "neoplasia óssea" -> capítulo de neoplasias ou descrição que contenha neoplasia + osso
CHAPTER_OR_STRICT_TERMS = frozenset([
    "neoplasia óssea", "neoplasia ossea", "neoplasia do osso", "câncer de osso", "cancer de osso",
])


def _expand_chapter_search_term(termo: str) -> str:
    """Expande termo com sinônimos para melhor match nos nomes oficiais dos capítulos (ex.: cardiovascular -> circulatório)."""
    low = termo.lower()
    extra = []
    for key, value in CHAPTER_SEARCH_SYNONYMS.items():
        if key in low and value not in low:
            extra.append(value)
    if extra:
        return f"{termo} {' '.join(extra)}".strip()
    return termo


def search_cid10_chapters(termo: str, limit: int = 5) -> List[str]:
    """
    Busca fuzzy apenas em capitulo_descricao (distinct).
    Retorna lista de descrições de capítulo para causa_cid10_capitulo_desc IN (...).
    Usa sinônimos (ex.: cardiovascular -> circulatório) para match em "Capítulo IX - Doenças do aparelho circulatório".
    """
    termo = (termo or "").strip()
    if len(termo) < 2:
        return []
    try:
        from rapidfuzz import process
    except ImportError:
        return []
    df = _load_depara()
    if df is None or "capitulo_descricao" not in df.columns:
        return []
    chapters = df["capitulo_descricao"].dropna().astype(str).str.strip()
    chapters = chapters[chapters != ""].unique().tolist()
    if not chapters:
        return []
    search_term = _expand_chapter_search_term(termo)
    results = process.extract(search_term, chapters, limit=limit, score_cutoff=60)
    return [r[0] for r in results] if results else []


def format_causas_for_sql(
    candidates: List[Dict[str, str]],
    by_chapter: bool = False,
    max_codes_per_prefix: int = 12,
) -> str:
    """
    Formata candidatos para instrução SQL no contexto do planejador.
    - by_chapter=True: causa_cid10_capitulo_desc IN ('Capítulo I...', ...).
    - by_chapter=False: agrupa por prefixo 3 chars; se muitos códigos usa LIKE 'A90%', senão IN (...).
    """
    if not candidates:
        return ""
    if by_chapter:
        capítulos = list({c.get("capitulo_descricao") or "" for c in candidates if c.get("capitulo_descricao")})
        capítulos = [c for c in capítulos if c]
        if not capítulos:
            return ""
        # Escapar aspas simples para SQL
        quoted = [f"'{str(c).replace(chr(39), chr(39)+chr(39))}'" for c in capítulos]
        return f"causa_cid10_capitulo_desc IN ({', '.join(quoted)})"
    # Agrupar por prefixo de 3 caracteres (ex.: A90, I21)
    # Normalizar códigos: base SIM costuma ter causa_basica SEM ponto (ex.: I219, A90); depara pode vir com I21.9
    from collections import defaultdict
    by_prefix: Dict[str, List[str]] = defaultdict(list)
    for c in candidates:
        cod = (c.get("codigo") or "").strip().replace(".", "")
        if not cod:
            continue
        prefix = cod[:3] if len(cod) >= 3 else cod
        by_prefix[prefix].append(cod)
    parts = []
    for prefix, codigos in sorted(by_prefix.items()):
        codigos = list(dict.fromkeys(codigos))
        if len(codigos) > max_codes_per_prefix:
            parts.append(f"causa_basica LIKE '{prefix}%'")
        else:
            quoted = [f"'{str(c).replace(chr(39), chr(39)+chr(39))}'" for c in codigos]
            parts.append(f"causa_basica IN ({', '.join(quoted)})")
    return " OR ".join(parts) if parts else ""


def extract_cause_phrase_heuristic(pergunta: str) -> str:
    """
    Extrai candidato a 'frase de causa/doença' da pergunta sem IA.
    Padrões: óbitos por X, mortes por X, causa X, devido a X, por X.
    Stop words para não pegar "por 2020" ou "por ano".
    """
    if not pergunta or len(pergunta.strip()) < 4:
        return ""
    text = pergunta.strip()
    low = f" {text.lower()} "
    stop = {
        "em", "no", "na", "nos", "nas", "ano", "anos", "últimos", "total", "sexo",
        "faixa", "estratifique", "quantos", "qual", "quais", "número", "numero",
        "óbitos", "obitos", "mortes", "porque", "que", "de", "do", "da",
        "o", "a", "os", "as", "um", "uma", "e", "ou",
        "existe", "sazonalidade", "comparar", "diferença", "evolução",
    }
    max_words = 6
    # Regex em low; posição em low tem offset 1 em relação a text (low = " " + text.lower() + " ")
    def rest_from_match(m: re.Match) -> str:
        end_low = m.end()
        start_text = max(0, end_low - 1)
        return text[start_text:].strip()

    # Padrões: "para doenças do aparelho X" (ex.: existe sazonalidade para doenças do aparelho circulatório), "óbitos por X", etc.
    for pattern in [
        r"\bpara\s+(?:doenças|doencas|doença|doenca)\s+do\s+aparelho\s+",  # rest = "circulatório", "respiratório", etc.
        r"\b(?:óbitos|obitos|mortes)\s+por\s+causas\s+",  # "por causas cardiovasculares" -> rest = "cardiovasculares..."
        r"\b(?:óbitos|obitos|mortes)\s+por\s+",
        r"\bcausa\s+(?:de\s+)?",
        r"\bdevido\s+a\s+",
        r"\bpor\s+causa\s+de\s+",
        r"\b(?:capítulo|capitulo)\s+(?:de\s+)?",
        r"\b(?:doenças|doencas|doença|doenca)\s+(?:de\s+)?",
    ]:
        m = re.search(pattern, low, re.IGNORECASE)
        if not m:
            continue
        rest = rest_from_match(m)
        if not rest:
            continue
        words = rest.split()
        take = []
        for w in words[:max_words]:
            if w.lower() in stop:
                break
            take.append(w)
        if take:
            return " ".join(take).strip()[:80]
    # Último recurso: "por X" genérico (após "quantos óbitos" etc.)
    m = re.search(r"\bpor\s+(\w+(?:\s+\w+){0,4})", low)
    if m:
        phrase = rest_from_match(m)
        if phrase:
            words = phrase.split()
            take = [w for w in words if w.lower() not in stop][:max_words]
            if take:
                return " ".join(take).strip()[:80]
    return ""


def _is_chapter_like_term(termo: str) -> bool:
    """True se o termo indica causa ampla (ex.: cardiovascular) onde filtro por capítulo é melhor."""
    low = (termo or "").lower().strip()
    for t in CHAPTER_LIKE_TERMS:
        if t in low or low in t:
            return True
    return False
