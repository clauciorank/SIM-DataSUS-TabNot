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
        "Colunas típicas: dt_obito, dt_obito_mes, ano, sexo_desc, uf_residencia, municipio_residencia, causa_basica, "
        "causa_cid10_capitulo_desc, causa_cid10_desc, circunstancia_desc, local_ocorrencia_desc, "
        "municipio_ocorrencia, uf_ocorrencia, tipo_obito_desc, racacor_desc, estciv_desc. "
        "dt_obito_mes é o primeiro dia do mês do óbito; para agregações mensais use GROUP BY dt_obito_mes ou strftime(dt_obito_mes, '%Y-%m'). "
        "Use EXATAMENTE valores do contexto para município e causa. "
        "Retorne JSON com chave \"sql\"."
    )


PLAN_SYSTEM = (
    "Você é um assistente que gera apenas uma instrução SQL SELECT para responder à pergunta do usuário "
    "com base nos dados oficiais de óbitos (SIM/DuckDB). Use somente as tabelas e colunas descritas no schema. "
    "Para filtros por município, use EXATAMENTE os valores canônicos fornecidos no contexto (não use o texto bruto do usuário). "
    "Para filtros por causa/doença, use os códigos ou condições (IN, LIKE) fornecidos no contexto de causa; "
    "não use ILIKE em texto livre cheque se os códigos da CID fornecidos fazem sentido de serem utilizados, se não utilize o que você encontrar em sua base de conhecimento o formato deve ser sem ponto ex: I219 e não I21.9. "
    "Se a pergunta for sobre MÊS do ano (ex.: qual mês, por mês, mensal, evolução mensal), use a coluna dt_obito_mes "
    "(primeiro dia do mês) com GROUP BY dt_obito_mes ou strftime(dt_obito_mes, '%Y-%m'); nunca use %w (dia da semana). "
    "Criatividade zero: não invente números nem valores. Resposta apenas em JSON com chave 'sql'.\n\n"
    "Fique atento se a palavra estado ou unidade federativa estiver na pergunta, se estiver, use a coluna uf_residencia para filtrar."
    "REGRAS DE ESCOPO (MUITO IMPORTANTE):\n"
    "- Se a pergunta NÃO menciona ano ou período: a query NÃO deve filtrar por ano (abrange todos os anos da base).\n"
    "- Se a pergunta NÃO menciona lugar (cidade/estado): a query NÃO deve filtrar por município nem UF (abrange todo o Brasil).\n"
    "- Se a pergunta NÃO menciona causa/doença específica: a query NÃO deve filtrar por causa.\n"
    "- Siga EXATAMENTE o que o usuário pediu. Não adicione filtros que ele não solicitou.\n\n"
    "PROIBIÇÕES ABSOLUTAS:\n"
    "- NUNCA filtre por municipio_residencia nem uf_residencia se o contexto de lugar estiver vazio.\n"
    "- NUNCA filtre por causa se o contexto de causa estiver vazio.\n"
    "- NUNCA invente municípiose e estados que não tenham sido fornecidos no contexto.\n"
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
    "Resposta: {\"sql\": \"SELECT strftime(dt_obito_mes, '%Y-%m') AS periodo, COUNT(*) AS total FROM v_obitos_completo "
    "WHERE causa_basica LIKE 'I21%' AND ano = 2022 "
    "GROUP BY dt_obito_mes ORDER BY 1\"}\n\n"
    "Pergunta: Distribuição de óbitos por sexo e faixa etária em São Paulo em 2020?\n"
    "Contexto de lugar: 'São Paulo'\n"
    "Resposta: {\"sql\": \"SELECT sexo_desc, faixa_etaria, COUNT(*) AS total FROM v_obitos_completo "
    "WHERE municipio_residencia = 'São Paulo' AND ano = 2020 "
    "GROUP BY sexo_desc, faixa_etaria ORDER BY sexo_desc, total DESC\"}\n\n"
    "Pergunta: existe sazonalidade para doenças do aparelho circulatório?\n"
    "Contexto de causa — OPÇÃO A: causa_cid10_capitulo_desc IN ('Capítulo IX - Doenças do aparelho circulatório')\n"
    "Intent: serie_temporal_mensal\n"
    "Resposta: {\"sql\": \"SELECT strftime(dt_obito_mes, '%Y-%m') AS periodo, COUNT(*) AS total FROM v_obitos_completo "
    "WHERE causa_cid10_capitulo_desc IN ('Capítulo IX - Doenças do aparelho circulatório') "
    "GROUP BY dt_obito_mes ORDER BY 1\"}\n\n"
    "Pergunta: quantas mortes por neoplasia óssea?\n"
    "Contexto de causa — OPÇÃO B: causa_basica LIKE 'C40%' OR causa_basica LIKE 'C41%' OR causa_basica LIKE 'C795%'\n"
    "Intent: contagem\n"
    "Resposta: {\"sql\": \"SELECT COUNT(*) AS total_obitos FROM v_obitos_completo "
    "WHERE (causa_basica LIKE 'C40%' OR causa_basica LIKE 'C41%' OR causa_basica LIKE 'C795%')\"}\n"
)

FORMAT_RESPONSE_SYSTEM = (
    "Você recebe a pergunta do usuário e o resultado de uma consulta SQL sobre dados de óbitos (SIM/DataSUS). "
    "Escreva uma resposta objetiva e concisa usando APENAS os dados presentes no resultado.\n\n"
    "REGRAS:\n"
    "- Não invente números nem dados que não estejam no resultado.\n"
    "- Se o resultado tiver muitas linhas, destaque os principais (top 5-10).\n"
    "- Para contagens, mencione o valor exato.\n"
    "- Para rankings, liste os itens em ordem.\n"
    "- Para séries temporais, descreva a tendência.\n"
    "- Seja educado e breve. Use texto corrido.\n"
    "- Se a pergunta pediu algo que o resultado não responde diretamente, "
    "informe o que os dados mostram sem inventar."
)


