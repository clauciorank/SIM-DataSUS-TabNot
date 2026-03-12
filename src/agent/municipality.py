"""
Resolução fuzzy de nomes de municípios para uso na SQL.
Prioriza cidades conhecidas e usa estado (UF) para disambiguar (ex.: São Bento do Sul, SC).
Ferramenta resolve_place(phrase) para o agente: recebe frase extraída pela IA e devolve nome canônico.
Estados: carregados de reference/municipios/estados.csv se existir (versionável); senão lista fixa.
"""
import re
from pathlib import Path
from typing import List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
GOLD_DB = PROJECT_ROOT / "data" / "SIM" / "gold" / "obitos.duckdb"
REFERENCE_MUNICIPIOS_DIR = PROJECT_ROOT / "reference" / "municipios"
ESTADOS_CSV = "estados.csv"

# Fallback: nome completo (minúsculo) -> sigla UF
_ESTADOS_BR_FALLBACK: List[Tuple[str, str]] = [
    ("acre", "AC"), ("alagoas", "AL"), ("amapá", "AP"), ("amazonas", "AM"),
    ("bahia", "BA"), ("ceará", "CE"), ("distrito federal", "DF"), ("espírito santo", "ES"),
    ("goiás", "GO"), ("maranhão", "MA"), ("mato grosso", "MT"), ("mato grosso do sul", "MS"),
    ("minas gerais", "MG"), ("pará", "PA"), ("paraíba", "PB"), ("paraná", "PR"),
    ("pernambuco", "PE"), ("piauí", "PI"), ("rio de janeiro", "RJ"), ("rio grande do norte", "RN"),
    ("rio grande do sul", "RS"), ("rondônia", "RO"), ("roraima", "RR"),
    ("santa catarina", "SC"), ("são paulo", "SP"), ("sergipe", "SE"), ("tocantins", "TO"),
]


def _load_estados_br() -> List[Tuple[str, str]]:
    """Carrega (nome_minusculo, uf) de reference/municipios/estados.csv se existir."""
    import csv
    csv_path = REFERENCE_MUNICIPIOS_DIR / ESTADOS_CSV
    if not csv_path.is_file():
        return _ESTADOS_BR_FALLBACK
    try:
        with open(csv_path, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            out = []
            for row in reader:
                uf = (row.get("uf") or "").strip().upper()
                nome = (row.get("nome") or "").strip().lower()
                if uf and nome:
                    out.append((nome, uf))
            return sorted(out, key=lambda x: -len(x[0])) if out else _ESTADOS_BR_FALLBACK
    except Exception:
        return _ESTADOS_BR_FALLBACK


# Nome completo do estado (minúsculo) -> sigla UF (para não confundir estado com município)
ESTADOS_BR: List[Tuple[str, str]] = _load_estados_br()

# Capitais e cidades grandes: priorizar no match (curitiba -> Curitiba, não Muritiba)
MUNICIPIOS_CONHECIDOS = [
    "Curitiba", "São Paulo", "Rio de Janeiro", "Belo Horizonte", "Porto Alegre",
    "Salvador", "Fortaleza", "Brasília", "Recife", "Goiânia", "Manaus", "Belém",
    "Guarulhos", "Campinas", "São Luís", "São Gonçalo", "Maceió",
    "Duque de Caxias", "Natal", "Teresina", "Campo Grande", "João Pessoa",
    "Cuiabá", "Aracaju", "Florianópolis", "Vitória", "Palmas", "Boa Vista",
    "Macapá", "Rio Branco",
]

_municipios_cache: Optional[List[str]] = None
_municipios_uf_cache: Optional[List[Tuple[str, str]]] = None


def _load_municipalities() -> List[str]:
    """Carrega lista distinta de municipio_residencia da gold. Usa cache."""
    global _municipios_cache
    if _municipios_cache is not None:
        return _municipios_cache
    try:
        import duckdb
        con = duckdb.connect(str(GOLD_DB), read_only=True)
        try:
            rows = con.execute(
                "SELECT DISTINCT municipio_residencia FROM v_obitos_analise "
                "WHERE municipio_residencia IS NOT NULL AND TRIM(CAST(municipio_residencia AS VARCHAR)) != ''"
            ).fetchall()
            _municipios_cache = [str(r[0]).strip() for r in rows if r[0]]
        finally:
            con.close()
    except Exception:
        _municipios_cache = []
    return _municipios_cache


def _load_municipalities_with_uf() -> List[Tuple[str, str]]:
    """Carrega (municipio_residencia, uf_residencia) distintos da gold. Usa cache."""
    global _municipios_uf_cache
    if _municipios_uf_cache is not None:
        return _municipios_uf_cache
    try:
        import duckdb
        con = duckdb.connect(str(GOLD_DB), read_only=True)
        try:
            rows = con.execute(
                "SELECT DISTINCT municipio_residencia, uf_residencia FROM v_obitos_analise "
                "WHERE municipio_residencia IS NOT NULL AND TRIM(CAST(municipio_residencia AS VARCHAR)) != '' "
                "AND uf_residencia IS NOT NULL"
            ).fetchall()
            _municipios_uf_cache = [(str(r[0]).strip(), str(r[1]).strip()) for r in rows if r[0] and r[1]]
        finally:
            con.close()
    except Exception:
        _municipios_uf_cache = []
    return _municipios_uf_cache


def _detect_uf_in_phrase(phrase: str) -> Optional[str]:
    """
    Se a frase mencionar um estado (ex.: 'Santa Catarina'), retorna a UF (SC).
    Ordenamos por tamanho decrescente para pegar 'mato grosso do sul' antes de 'mato grosso'.
    """
    low = phrase.lower().strip()
    if not low:
        return None
    # Siglas de 2 letras no final ou sozinhas
    match = re.search(r"\b(ac|al|am|ap|ba|ce|df|es|go|ma|mg|ms|mt|pa|pb|pe|pi|pr|rj|rn|ro|rr|rs|sc|se|sp|to)\b", low)
    if match:
        return match.group(1).upper()
    # Nome completo: maior primeiro
    for nome_estado, uf in sorted(ESTADOS_BR, key=lambda x: -len(x[0])):
        if nome_estado in low:
            return uf
    return None


def _remove_state_from_phrase(phrase: str, uf: Optional[str]) -> str:
    """
    Remove menção ao estado da frase para sobrar só o nome do município.
    Se sobrar vazio (ex.: "São Paulo" cidade = nome do estado), devolve a frase original.
    """
    low = phrase.lower().strip()
    if not low:
        return phrase.strip()
    original = phrase.strip()
    # Remover sigla UF
    if uf:
        low = re.sub(rf"\b{re.escape(uf.lower())}\b", "", low)
    # Remover apenas o nome do estado que corresponde à UF detectada
    if uf:
        for nome_estado, sigla in ESTADOS_BR:
            if sigla == uf:
                low = re.sub(re.escape(nome_estado), "", low, flags=re.IGNORECASE)
                break
    low = re.sub(r"\s*,\s*", " ", re.sub(r"\s+", " ", low)).strip()
    return low if low else original


def _match_against_known(texto: str, score_minimo: int = 90) -> Optional[str]:
    """
    Se o texto bater em algum município conhecido (capitais etc.) com score alto,
    retorna esse nome canônico. Evita curitiba -> Muritiba (prioriza Curitiba).
    Comparação case-insensitive (curitiba = Curitiba).
    """
    texto = (texto or "").strip()
    if not texto or len(texto) < 3:
        return None
    try:
        from rapidfuzz import process
    except ImportError:
        return None
    # Comparar em minúsculas para curitiba bater em Curitiba com score 100
    texto_lower = texto.lower()
    known_lower = [m.lower() for m in MUNICIPIOS_CONHECIDOS]
    results = process.extract(texto_lower, known_lower, limit=1, score_cutoff=score_minimo)
    if not results:
        return None
    idx = known_lower.index(results[0][0])
    return MUNICIPIOS_CONHECIDOS[idx]


def resolve_municipality(
    texto_usuario: str,
    limite: int = 3,
    score_minimo: int = 80,
) -> List[str]:
    """
    Resolve texto digitado pelo usuário para nome(s) canônico(s) de município na base.
    Primeiro tenta contra a lista de cidades conhecidas (evita curitiba->Muritiba).
    """
    texto = (texto_usuario or "").strip()
    if not texto or len(texto) < 2:
        return []
    try:
        from rapidfuzz import process
    except ImportError:
        return []
    # Prioridade 1: cidades conhecidas (curitiba -> Curitiba, não Muritiba)
    known = _match_against_known(texto, score_minimo=90)
    if known:
        return [known]
    # Prioridade 2: municípios que existem nos dados
    municipios = _load_municipalities()
    if not municipios:
        return []
    results = process.extract(texto, municipios, limit=limite, score_cutoff=score_minimo)
    return [r[0] for r in results] if results else []


def extract_place_heuristic(pergunta: str) -> str:
    """
    Extrai candidato a 'frase de lugar' da pergunta sem usar IA (economiza quota).
    Procura 'em X', 'na X', 'no X' e pega até 8 palavras ou até palavra de parada.
    """
    if not pergunta or len(pergunta.strip()) < 4:
        return ""
    text = pergunta.strip()
    low = f" {text.lower()} "
    stop = {"por", "nos", "estratifique", "principais", "causas", "óbitos", "obitos", "mortes", "ano", "anos", "últimos", "quantos", "qual", "quais", "total", "número", "numero", "faixa", "sexo", "dengue", "covid"}
    max_words = 8

    for sep in (" em ", " na ", " no ", " no município de ", " na cidade de "):
        idx = low.find(sep)
        if idx < 0:
            continue
        start = idx + len(sep)
        rest = text[start:].strip()
        rest_low = rest.lower()
        words = rest.split()
        take = []
        for w in words[:max_words]:
            if w.lower() in stop:
                break
            take.append(w)
        if take:
            return " ".join(take).strip()[:120]
    return ""


def resolve_place(phrase: str) -> List[str]:
    """
    Ferramenta para o agente: resolve uma frase de lugar (ex.: "São Bento do Sul Santa Catarina")
    para o nome canônico do município na base. Usa estado para disambiguar (evita Catarina como município).
    Retorna lista de nomes exatos para usar em municipio_residencia (vazia se não resolver).
    """
    phrase = (phrase or "").strip()
    if not phrase or len(phrase) < 3:
        return []
    try:
        from rapidfuzz import process
    except ImportError:
        return []
    uf = _detect_uf_in_phrase(phrase)
    municipio_part = _remove_state_from_phrase(phrase, uf)
    if not municipio_part or len(municipio_part) < 2:
        return []

    # 1) Se temos UF, buscar só municípios dessa UF e dar match no trecho do município
    if uf:
        mun_uf = _load_municipalities_with_uf()
        candidatos_uf = [m for m, u in mun_uf if u == uf]
        if candidatos_uf:
            # Case-insensitive: comparar em minúsculas e devolver canônico
            part_lower = municipio_part.lower()
            cand_lower = [c.lower() for c in candidatos_uf]
            results = process.extract(part_lower, cand_lower, limit=1, score_cutoff=75)
            if results:
                idx = cand_lower.index(results[0][0])
                return [candidatos_uf[idx]]

    # 2) Cidades conhecidas (curitiba -> Curitiba)
    known = _match_against_known(municipio_part, score_minimo=88)
    if known:
        return [known]

    # 3) Qualquer município na base (fuzzy)
    municipios = _load_municipalities()
    if not municipios:
        return []
    results = process.extract(municipio_part, municipios, limit=1, score_cutoff=78)
    return [r[0] for r in results] if results else []


def get_municipalities_for_context(pergunta: str) -> str:
    """
    A partir da pergunta do usuário, tenta identificar menções a municípios
    (por fuzzy match) e retorna texto para injetar no contexto do planejador:
    'Municípios resolvidos: use exatamente na SQL (municipio_residencia): Curitiba.'
    """
    # Tokens candidatos: palavras com 4+ caracteres (reduz falsos positivos)
    import re
    palavras = re.findall(r"[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ0-9']{3,}", pergunta)
    if not palavras:
        return ""
    seen = set()
    resolvidos = []
    for p in palavras:
        if p.lower() in ("dados", "obitos", "óbitos", "numero", "número", "quantos", "total", "ano", "anos"):
            continue
        for nome in resolve_municipality(p, limite=1, score_minimo=82):
            if nome not in seen:
                seen.add(nome)
                resolvidos.append(nome)
    if not resolvidos:
        return ""
    return (
        "Municípios resolvidos (use estes valores EXATOS na SQL em municipio_residencia): "
        + ", ".join(f"'{m}'" for m in resolvidos)
        + "."
    )
