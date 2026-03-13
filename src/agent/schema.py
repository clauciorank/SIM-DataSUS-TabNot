"""
Schema das views gold para o LLM gerar SQL correta.
Tabelas e colunas vêm da própria view (DESCRIBE) quando o banco está disponível.
"""
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
GOLD_DB = PROJECT_ROOT / "data" / "SIM" / "gold" / "obitos.duckdb"

_SCHEMA_FROM_VIEW_CACHE: Optional[str] = None


def get_schema_from_view() -> str:
    """
    Retorna o schema (tabelas e colunas) obtido da view v_obitos_completo.
    Usado para alimentar o agente com exatamente o que a view expõe.
    Se o cache estiver vazio e o banco existir, usa warm_all_caches() (uma conexão para todos os caches).
    """
    global _SCHEMA_FROM_VIEW_CACHE
    if _SCHEMA_FROM_VIEW_CACHE is not None:
        return _SCHEMA_FROM_VIEW_CACHE
    if not GOLD_DB.is_file():
        _SCHEMA_FROM_VIEW_CACHE = _fallback_schema_views()
        return _SCHEMA_FROM_VIEW_CACHE
    from src.agent.db_cache import warm_all_caches
    warm_all_caches()
    return _SCHEMA_FROM_VIEW_CACHE or _fallback_schema_views()


def _fallback_schema_views() -> str:
    """Fallback quando o banco não está disponível (ex.: testes)."""
    return (
        "Você gera apenas uma query SQL DuckDB. Tabela: v_obitos_completo (view read-only). "
        "Colunas típicas: dt_obito, ano, sexo_desc, uf_residencia, municipio_residencia, causa_basica, "
        "causa_cid10_capitulo_desc, causa_cid10_desc, circunstancia_desc, local_ocorrencia_desc, "
        "municipio_ocorrencia, uf_ocorrencia, tipo_obito_desc, racacor_desc, estciv_desc. "
        "Para MÊS use month(dt_obito) ou strftime(dt_obito, '%m'). Use EXATAMENTE valores do contexto para município e causa. "
        "Retorne JSON com chave \"sql\"."
    )


def reset_schema_cache() -> None:
    """Limpa todos os caches de metadata (schema, rich_schema, municípios) e o flag de warm."""
    global _SCHEMA_FROM_VIEW_CACHE
    _SCHEMA_FROM_VIEW_CACHE = None
    try:
        from src.agent import schema_enricher
        schema_enricher._rich_schema_cache = None
    except Exception:
        pass
    try:
        from src.agent import municipality
        municipality._municipios_cache = None
        municipality._municipios_uf_cache = None
    except Exception:
        pass
    try:
        from src.agent.db_cache import reset_warm_done
        reset_warm_done()
    except Exception:
        pass


# Uso no graph: get_schema_from_view() para alimentar o agente com o que a view expõe.
SCHEMA_VIEWS = ""  # Obsoleto: use get_schema_from_view() no prompt.

PLAN_SYSTEM = (
    "Você é um assistente que gera apenas uma instrução SQL SELECT para responder à pergunta do usuário "
    "com base nos dados oficiais de óbitos (SIM/DuckDB). Use somente as tabelas e colunas descritas no schema. "
    "Para filtros por município, use EXATAMENTE os valores canônicos fornecidos no contexto (não use o texto bruto do usuário). "
    "Para filtros por causa/doença, use EXATAMENTE os códigos ou condições (IN, LIKE) fornecidos no contexto de causa; "
    "não use ILIKE em texto livre nem invente códigos CID. "
    "Se a pergunta for sobre MÊS do ano (ex.: qual mês, por mês, mensal, evolução mensal), use month(dt_obito) "
    "ou strftime(dt_obito, '%m'); nunca use %w (isso é dia da semana). "
    "Criatividade zero: não invente números nem valores. Resposta apenas em JSON com chave 'sql'.\n\n"
    "PROIBIÇÕES ABSOLUTAS:\n"
    "- NUNCA filtre por municipio_residencia nem uf_residencia se o contexto de lugar estiver vazio.\n"
    "- NUNCA filtre por causa se o contexto de causa estiver vazio.\n"
    "- NUNCA invente municípios, estados ou códigos CID que não tenham sido fornecidos no contexto.\n"
    "- Se não houver contexto de lugar, a query se aplica a TODOS os municípios/estados do Brasil (não adicione WHERE de lugar).\n\n"
    "EXEMPLOS DE PERGUNTAS E SUAS SQL CORRETAS:\n\n"
    "Pergunta: Quantos óbitos foram registrados em 2022?\n"
    "Resposta: {\"sql\": \"SELECT COUNT(*) AS total_obitos FROM v_obitos_completo WHERE ano = 2022\"}\n\n"
    "Pergunta: Quantos óbitos por dengue em Curitiba em 2023?\n"
    "Contexto de lugar: 'Curitiba'\n"
    "Contexto de causa: causa_basica IN ('A90')\n"
    "Resposta: {\"sql\": \"SELECT COUNT(*) AS total_obitos FROM v_obitos_completo "
    "WHERE municipio_residencia = 'Curitiba' AND causa_basica IN ('A90') AND ano = 2023\"}\n\n"
    "Pergunta: Quais as 5 causas mais comuns de óbito no Paraná em 2021?\n"
    "Contexto de lugar: uf_residencia = 'PR'\n"
    "Resposta: {\"sql\": \"SELECT causa_cid10_desc, COUNT(*) AS total FROM v_obitos_completo "
    "WHERE uf_residencia = 'PR' AND ano = 2021 "
    "GROUP BY causa_cid10_desc ORDER BY total DESC LIMIT 5\"}\n\n"
    "Pergunta: Como evoluiu o número de óbitos por infarto mês a mês em 2022?\n"
    "Contexto de causa: causa_basica LIKE 'I21%'\n"
    "Resposta: {\"sql\": \"SELECT month(dt_obito) AS mes, COUNT(*) AS total FROM v_obitos_completo "
    "WHERE causa_basica LIKE 'I21%' AND ano = 2022 "
    "GROUP BY mes ORDER BY mes\"}\n\n"
    "Pergunta: Distribuição de óbitos por sexo e faixa etária em São Paulo em 2020?\n"
    "Contexto de lugar: 'São Paulo'\n"
    "Resposta: {\"sql\": \"SELECT sexo_desc, faixa_etaria, COUNT(*) AS total FROM v_obitos_completo "
    "WHERE municipio_residencia = 'São Paulo' AND ano = 2020 "
    "GROUP BY sexo_desc, faixa_etaria ORDER BY sexo_desc, total DESC\"}\n\n"
    "Pergunta: existe sazonalidade para doenças do aparelho circulatório?\n"
    "Contexto de causa — OPÇÃO A: causa_cid10_capitulo_desc IN ('Capítulo IX - Doenças do aparelho circulatório')\n"
    "Intent: serie_temporal_mensal\n"
    "Resposta: {\"sql\": \"SELECT month(dt_obito) AS mes, COUNT(*) AS total FROM v_obitos_completo "
    "WHERE causa_cid10_capitulo_desc IN ('Capítulo IX - Doenças do aparelho circulatório') "
    "GROUP BY mes ORDER BY mes\"}\n\n"
    "Pergunta: quantas mortes por neoplasia óssea?\n"
    "Contexto de causa — OPÇÃO B: causa_basica LIKE 'C40%' OR causa_basica LIKE 'C41%' OR causa_basica LIKE 'C795%'\n"
    "Intent: contagem\n"
    "Resposta: {\"sql\": \"SELECT COUNT(*) AS total_obitos FROM v_obitos_completo "
    "WHERE (causa_basica LIKE 'C40%' OR causa_basica LIKE 'C41%' OR causa_basica LIKE 'C795%')\"}\n"
)

EVALUATE_SYSTEM = (
    "Você avalia se o resultado de uma query SQL responde adequadamente à pergunta do usuário. "
    "Responda em uma linha: SIM ou NÃO. Se NÃO, acrescente um breve motivo em seguida (ex: NÃO. Motivo: a query não filtra por ano)."
)

RESPOND_SYSTEM = (
    "Você formata a resposta final ao usuário com base no resultado da query. "
    "Seja breve e objetivo. Use apenas os números e dados presentes no resultado. Não invente nada."
)

# Uma única chamada: avalia e, se OK, formata a resposta; se não OK, pede replan (economiza quota)
EVALUATE_AND_RESPOND_SYSTEM = (
    "Você recebe: a pergunta do usuário, a query SQL executada e o resultado (tabela). "
    "Siga este checklist antes de responder:\n\n"
    "CHECKLIST DE VALIDAÇÃO:\n"
    "1. O resultado está vazio '(vazio)'? Se sim, responda: REPLAN: resultado vazio — "
    "verifique filtros de ano, município ou causa.\n"
    "2. Se a pergunta pede um número total (quantos, total, contagem), o resultado tem essa coluna com valor > 0?\n"
    "3. Se a pergunta pede ranking ou top N, o resultado tem N linhas (ou menos se a base for menor)?\n"
    "4. Se a pergunta menciona lugar (cidade/estado), a query filtrou por lugar? "
    "Se não, responda: REPLAN: query não filtrou por lugar.\n"
    "5. Se a pergunta menciona ano ou período, a query filtrou por ano? "
    "Se não, responda: REPLAN: query não filtrou por ano.\n"
    "6. Se a pergunta menciona causa/doença, a query filtrou por causa? "
    "Se não, responda: REPLAN: query não filtrou por causa.\n"
    "7. Se a pergunta pede uma causa/doença ESPECÍFICA (ex.: neoplasia óssea, dengue, tuberculose), os códigos ou o capítulo na query devem corresponder a essa causa. Se a query usa códigos que claramente referem-se a OUTRAS doenças (ex.: tuberculose, HIV, acidentes, doação de sangue quando a pergunta foi sobre câncer/neoplasia), responda: REPLAN: o filtro de causa não corresponde à pergunta — códigos inadequados.\n\n"
    "REGRA FINAL:\n"
    "Se todos os itens aplicáveis passaram: escreva uma resposta objetiva para o usuário "
    "usando APENAS os dados do resultado. Não invente números. Seja conciso.\n"
    "Se qualquer item falhou: escreva na primeira linha EXATAMENTE: REPLAN: <motivo específico>. "
    "Nada mais além dessa linha quando for REPLAN.\n"
    "Se o resultado está correto mas é vazio por ausência genuína de dados (ex.: nenhum óbito registrado), "
    "informe o usuário educadamente em vez de pedir REPLAN."
)

# Validação de SQL antes de executar: uma chamada LLM que aprova ou pede replan
VALIDATE_SQL_SYSTEM = (
    "Você revisa se uma query SQL está alinhada à pergunta do usuário e aos contextos fornecidos (município, causa). "
    "Responda em UMA LINHA apenas: OK ou REPLAN: <motivo breve>. "
    "Confira: (1) A SQL responde à pergunta? "
    "(2) Se a pergunta menciona lugar (cidade/estado), a SQL usa os municípios ou UF do contexto? "
    "(3) Se menciona causa/doença, a SQL usa os códigos ou capítulo do contexto? "
    "(4) Se menciona ano ou período, há filtro por ano ou dt_obito? "
    "(5) Se a pergunta é sobre MÊS do ano, a SQL usa month(dt_obito) ou strftime(..., '%m') e NÃO usa '%w' (dia da semana)? "
    "Se qualquer item falhar, responda REPLAN: e o motivo. Caso contrário, responda apenas OK."
)

# Usado no nó de extração de lugar: a IA devolve só a menção a cidade/município/estado para depois resolver com a ferramenta
EXTRACT_PLACE_SYSTEM = (
    "Você extrai da pergunta do usuário APENAS a parte que se refere a um lugar (cidade, município ou estado). "
    "Exemplos: 'quantos óbitos em Curitiba' -> a parte do lugar é 'Curitiba'; "
    "'em São Bento do Sul Santa Catarina' -> 'São Bento do Sul Santa Catarina'; "
    "'no Rio de Janeiro' -> 'Rio de Janeiro'. "
    "Se não houver menção a lugar, retorne vazio. "
    "Resposta APENAS em JSON com uma única chave 'place' (string). Exemplo: {\"place\": \"São Bento do Sul Santa Catarina\"} ou {\"place\": \"\"}."
)
