"""
Classificador de intent por palavras-chave para o agente SQL.
Usado no _plan_node para injetar hint de SQL (ex.: sazonalidade → GROUP BY mes).
"""
from typing import Dict

INTENT_KEYWORDS: Dict[str, str] = {
    "sazonalidade": "serie_temporal_mensal",
    "mês a mês": "serie_temporal_mensal",
    "mes a mes": "serie_temporal_mensal",
    "mensal": "serie_temporal_mensal",
    "por mês": "serie_temporal_mensal",
    "por mes": "serie_temporal_mensal",
    "evolução": "serie_temporal_anual",
    "por ano": "serie_temporal_anual",
    "histórico": "serie_temporal_anual",
    "historico": "serie_temporal_anual",
    "tendência": "serie_temporal_anual",
    "tendencia": "serie_temporal_anual",
    "ranking": "ranking",
    "principais": "ranking",
    "top ": "ranking",
    "mais comum": "ranking",
    "distribuição": "distribuicao",
    "distribuicao": "distribuicao",
    "por sexo": "distribuicao",
    "por faixa": "distribuicao",
    "quantos": "contagem",
    "total": "contagem",
    "quanto": "contagem",
}

INTENT_SQL_HINT: Dict[str, str] = {
    "serie_temporal_mensal": (
        "A pergunta pede SAZONALIDADE ou evolução MENSAL. "
        "A SQL DEVE usar: GROUP BY month(dt_obito) ORDER BY mes. "
        "Não filtre por município a menos que o contexto de lugar esteja preenchido."
    ),
    "serie_temporal_anual": (
        "A pergunta pede evolução ANUAL. "
        "A SQL DEVE usar: GROUP BY ano ORDER BY ano."
    ),
    "ranking": (
        "A pergunta pede um RANKING. "
        "A SQL DEVE usar: ORDER BY total DESC LIMIT N."
    ),
    "distribuicao": (
        "A pergunta pede DISTRIBUIÇÃO por categoria. "
        "A SQL DEVE usar: GROUP BY <coluna_categoria>."
    ),
    "contagem": (
        "A pergunta pede uma CONTAGEM. "
        "A SQL DEVE usar: SELECT COUNT(*) AS total."
    ),
}


def classify_intent(pergunta: str) -> str:
    """
    Retorna o intent da pergunta (serie_temporal_mensal, ranking, etc.)
    com base em palavras-chave. Ordem dos itens em INTENT_KEYWORDS define prioridade.
    """
    if not pergunta or not isinstance(pergunta, str):
        return "contagem"
    low = pergunta.lower().strip()
    for keyword, intent in INTENT_KEYWORDS.items():
        if keyword in low:
            return intent
    return "contagem"
