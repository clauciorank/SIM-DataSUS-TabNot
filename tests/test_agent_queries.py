"""
Golden set de testes para o agente SQL SIM.
Execute com: pytest tests/test_agent_queries.py -v
Requer: banco DuckDB em data/SIM/gold/obitos.duckdb e llm_config configurado (ex.: GEMINI_API_KEY).
"""
import os
import sys
from pathlib import Path

# Raiz do projeto
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Configure via variável de ambiente ou diretamente aqui para testes
LLM_CONFIG = {
    "provider": "gemini",
    "model": os.getenv("GEMINI_MODEL", "gemini-2.0-flash-lite"),
    "api_key": os.getenv("GEMINI_API_KEY", ""),
}

GOLDEN_SET = [
    # --- Perguntas simples (devem funcionar na 1ª tentativa) ---
    {
        "id": "contagem_simples_ano",
        "pergunta": "Quantos óbitos foram registrados em 2022?",
        "sql_deve_conter": ["COUNT", "2022"],
        "resultado_nao_pode_ser": ["(vazio)", "Erro"],
        "descricao": "Contagem total sem filtros além do ano",
    },
    {
        "id": "contagem_municipio_ano",
        "pergunta": "Quantos óbitos ocorreram em Curitiba em 2021?",
        "sql_deve_conter": ["Curitiba", "2021"],
        "resultado_nao_pode_ser": ["Erro"],
        "descricao": "Contagem com filtro de município e ano",
    },
    {
        "id": "causa_dengue",
        "pergunta": "Quantos óbitos por dengue houve em 2023?",
        "sql_deve_conter": ["A90", "2023"],
        "resultado_nao_pode_ser": ["Erro"],
        "descricao": "Filtragem por causa usando código CID",
    },
    {
        "id": "ranking_causas_uf",
        "pergunta": "Quais as 5 principais causas de morte no Paraná em 2022?",
        "sql_deve_conter": ["LIMIT 5", "ORDER BY", "PR", "2022"],
        "resultado_nao_pode_ser": ["Erro"],
        "descricao": "Ranking com filtro de estado",
    },
    {
        "id": "serie_mensal",
        "pergunta": "Como evoluíram os óbitos mês a mês em 2022 no Brasil?",
        "sql_deve_conter": ["2022", "GROUP BY"],
        "sql_nao_pode_conter": ["%w"],
        "resultado_nao_pode_ser": ["Erro"],
        "descricao": "Série temporal mensal — teste do bug %w vs %m",
    },
    {
        "id": "distribuicao_sexo",
        "pergunta": "Qual a distribuição de óbitos por sexo em 2021?",
        "sql_deve_conter": ["sexo", "GROUP BY", "2021"],
        "resultado_nao_pode_ser": ["Erro"],
        "descricao": "Agrupamento por variável categórica",
    },
    {
        "id": "filtro_estado_uf",
        "pergunta": "Quantos óbitos houve no PR em 2020?",
        "sql_deve_conter": ["PR", "2020"],
        "resultado_nao_pode_ser": ["Erro"],
        "descricao": "Filtro por sigla de UF",
    },
    # --- Perguntas que o guardrail deve rejeitar ---
    {
        "id": "guardrail_futebol",
        "pergunta": "Quem ganhou a Copa do Mundo de 2022?",
        "resposta_deve_conter": ["não parece ser sobre dados"],
        "sql_deve_ser_vazia": True,
        "descricao": "Guardrail — pergunta off-topic",
    },
]


def _run_test(caso):
    from src.agent.graph import run_agent
    return run_agent(caso["pergunta"], LLM_CONFIG)


def test_agente_contagem_simples():
    """Contagem simples por ano."""
    import pytest
    if not LLM_CONFIG.get("api_key"):
        pytest.skip("GEMINI_API_KEY não configurado")
    caso = next(c for c in GOLDEN_SET if c["id"] == "contagem_simples_ano")
    resultado = _run_test(caso)
    sql = resultado.get("sql_planejada", "")
    execucao = resultado.get("resultado_execucao", "")
    resposta = resultado.get("resposta_final", "")
    for trecho in caso.get("sql_deve_conter", []):
        assert trecho.upper() in sql.upper(), f"SQL não contém '{trecho}': {sql}"
    for trecho in caso.get("resultado_nao_pode_ser", []):
        assert trecho not in execucao, f"Resultado contém '{trecho}': {execucao}"
    assert resposta, "Resposta final vazia"


def test_agente_guardrail():
    """Guardrail rejeita pergunta off-topic."""
    caso = next(c for c in GOLDEN_SET if c["id"] == "guardrail_futebol")
    resultado = _run_test(caso)
    sql = resultado.get("sql_planejada", "")
    resposta = resultado.get("resposta_final", "")
    assert sql == "", f"SQL deveria estar vazia: {sql}"
    for trecho in caso.get("resposta_deve_conter", []):
        assert trecho.lower() in resposta.lower(), f"Resposta deveria conter '{trecho}': {resposta}"


def _run_one_caso(caso):
    """Executa run_agent e verifica expectativas do caso (para testes parametrizados)."""
    from src.agent.graph import run_agent
    resultado = run_agent(caso["pergunta"], LLM_CONFIG)
    sql = resultado.get("sql_planejada", "")
    execucao = resultado.get("resultado_execucao", "")
    resposta = resultado.get("resposta_final", "")

    if caso.get("sql_deve_ser_vazia"):
        assert sql == "", f"[{caso['id']}] SQL deveria estar vazia: {sql}"
        for trecho in caso.get("resposta_deve_conter", []):
            assert trecho.lower() in resposta.lower(), (
                f"[{caso['id']}] Resposta deveria conter '{trecho}': {resposta}"
            )
        return

    for trecho in caso.get("sql_deve_conter", []):
        assert trecho.upper() in sql.upper(), (
            f"[{caso['id']}] SQL não contém '{trecho}'.\nSQL: {sql}"
        )
    for trecho in caso.get("sql_nao_pode_conter", []):
        assert trecho not in sql, (
            f"[{caso['id']}] SQL não deveria conter '{trecho}'.\nSQL: {sql}"
        )
    for trecho in caso.get("resultado_nao_pode_ser", []):
        assert trecho not in execucao, (
            f"[{caso['id']}] Resultado contém '{trecho}'.\nResultado: {execucao}"
        )
    assert resposta, f"[{caso['id']}] Resposta final vazia."


def test_agente_parametrized():
    """Golden set: um teste por caso (pula se GEMINI_API_KEY não estiver definido)."""
    import pytest
    if not LLM_CONFIG.get("api_key"):
        pytest.skip("GEMINI_API_KEY não configurado")

    for caso in GOLDEN_SET:
        _run_one_caso(caso)
