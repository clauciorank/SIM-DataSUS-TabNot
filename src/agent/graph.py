"""
Grafo LangGraph: planejar -> executar -> avaliar+responder (1 chamada) -> (fim | replanejar).
Suporta Gemini (API Google) e Ollama (local); modelo e provedor vêm da configuração.
"""
import json
import re
import time
from pathlib import Path
from typing import Any, Dict, Literal, TypedDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
GOLD_DB = PROJECT_ROOT / "data" / "SIM" / "gold" / "obitos.duckdb"

MAX_ROWS = 500
MAX_TENTATIVAS = 3

# Throttle só para Gemini (free tier)
_MIN_GEMINI_INTERVAL_SEC = 13
_last_gemini_call_time: float = 0


class AgentState(TypedDict, total=False):
    pergunta: str
    place_phrase: str
    municipios_contexto: str
    feedback_avaliacao: str
    tentativas: int
    sql_planejada: str
    resultado_execucao: str
    resposta_final: str
    avaliacao_ok: bool
    llm_config: Dict[str, Any]


def _throttle_gemini() -> None:
    global _last_gemini_call_time
    elapsed = time.monotonic() - _last_gemini_call_time
    if elapsed < _MIN_GEMINI_INTERVAL_SEC:
        time.sleep(_MIN_GEMINI_INTERVAL_SEC - elapsed)
    _last_gemini_call_time = time.monotonic()


def _gemini_call(api_key: str, model_id: str, system: str, user_content: str, temperature: float = 0.1) -> str:
    _throttle_gemini()
    import google.generativeai as genai
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(
        model_id or "gemini-3.1-flash-lite-preview",
        generation_config={"temperature": temperature, "max_output_tokens": 2048},
    )
    full = f"{system}\n\n---\n\n{user_content}"
    response = model.generate_content(full)
    if not response or not response.text:
        return ""
    return response.text.strip()


def _ollama_call(base_url: str, model_id: str, system: str, user_content: str, temperature: float = 0.1) -> str:
    """Chama Ollama local e retorna o texto da resposta."""
    import urllib.request
    import json as _json
    base = (base_url or "http://localhost:11434").rstrip("/")
    url = f"{base}/api/generate"
    prompt = f"{system}\n\n---\n\n{user_content}"
    payload = {
        "model": model_id or "llama3.2",
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": temperature},
    }
    req = urllib.request.Request(url, data=_json.dumps(payload).encode(), method="POST")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = _json.loads(resp.read().decode())
        return (data.get("response") or "").strip()
    except Exception as e:
        return f"Erro Ollama: {e!s}"


def _llm_call(llm_config: Dict[str, Any], system: str, user_content: str, temperature: float = 0.1) -> str:
    """Despacha para Gemini ou Ollama conforme llm_config (provider, model, api_key, ollama_base_url)."""
    if not llm_config:
        return ""
    provider = (llm_config.get("provider") or "gemini").strip().lower()
    model = (llm_config.get("model") or "").strip()
    if provider == "ollama":
        base = (llm_config.get("ollama_base_url") or "http://localhost:11434").strip()
        return _ollama_call(base, model, system, user_content, temperature)
    api_key = (llm_config.get("api_key") or "").strip()
    if not api_key:
        return ""
    return _gemini_call(api_key, model, system, user_content, temperature)


def _safe_sql(sql: str) -> bool:
    """Permite apenas SELECT e WITH (read-only)."""
    normalized = re.sub(r"\s+", " ", sql.strip().upper())
    if not normalized:
        return False
    for forbidden in ("INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE", "GRANT"):
        if forbidden in normalized:
            return False
    return normalized.startswith("SELECT") or normalized.startswith("WITH")


def _plan_node(state: AgentState, llm_config: Dict[str, Any]) -> AgentState:
    from src.agent.schema import SCHEMA_VIEWS, PLAN_SYSTEM
    from src.agent.cause_context import get_cause_context_for_plan
    pergunta = state.get("pergunta", "")
    municipios_contexto = state.get("municipios_contexto", "")
    feedback = state.get("feedback_avaliacao", "")
    user_msg = f"Pergunta do usuário: {pergunta}\n\n"
    if municipios_contexto:
        user_msg += f"Contexto (use estes valores exatos na SQL): {municipios_contexto}\n\n"
    causas_contexto = get_cause_context_for_plan(pergunta)
    if causas_contexto:
        user_msg += f"Contexto de causa/doença: {causas_contexto}\n\n"
    if feedback:
        user_msg += f"Avaliação anterior pediu ajuste: {feedback}\n\n"
    user_msg += "Gere apenas um JSON com chave 'sql' contendo a query DuckDB."
    full_system = f"{PLAN_SYSTEM}\n\n{SCHEMA_VIEWS}"
    out = _llm_call(llm_config, full_system, user_msg, temperature=0.1)
    sql = ""
    try:
        # Extrair JSON da resposta (pode vir com markdown)
        if "```" in out:
            for block in re.findall(r"```(?:json)?\s*([\s\S]*?)```", out):
                data = json.loads(block.strip())
                if "sql" in data:
                    sql = data["sql"].strip()
                    break
        if not sql:
            data = json.loads(out)
            sql = (data.get("sql") or "").strip()
    except (json.JSONDecodeError, KeyError):
        pass
    return {"sql_planejada": sql or ""}


def _execute_node(state: AgentState) -> AgentState:
    sql = (state.get("sql_planejada") or "").strip()
    if not sql:
        return {"resultado_execucao": "Erro: nenhuma query gerada."}
    if not _safe_sql(sql):
        return {"resultado_execucao": "Erro: apenas consultas SELECT são permitidas."}
    try:
        import duckdb
        con = duckdb.connect(str(GOLD_DB), read_only=True)
        try:
            df = con.execute(sql).fetchdf()
            if df is not None and len(df) > MAX_ROWS:
                df = df.head(MAX_ROWS)
            result_str = df.to_string() if df is not None and not df.empty else "(vazio)"
        finally:
            con.close()
    except Exception as e:
        result_str = f"Erro ao executar: {e!s}"
    return {"resultado_execucao": result_str}


def _evaluate_and_respond_node(state: AgentState, llm_config: Dict[str, Any]) -> AgentState:
    """
    Uma única chamada LLM: avalia o resultado e, se OK, formata a resposta;
    se não OK, devolve REPLAN: motivo (economiza 1 chamada por pergunta).
    """
    from src.agent.schema import EVALUATE_AND_RESPOND_SYSTEM
    pergunta = state.get("pergunta", "")
    sql = state.get("sql_planejada", "")
    resultado = (state.get("resultado_execucao") or "")[:2000]
    user_msg = (
        f"Pergunta: {pergunta}\n\nQuery executada:\n{sql}\n\nResultado (amostra):\n{resultado}\n\n"
        "O resultado responde à pergunta? Se sim, escreva a resposta para o usuário. Se não, escreva REPLAN: e o motivo."
    )
    out = _llm_call(llm_config, EVALUATE_AND_RESPOND_SYSTEM, user_msg, temperature=0.2)
    out = (out or "").strip()
    replan_prefix = "REPLAN:"
    if out.upper().startswith(replan_prefix.upper()):
        feedback = out[len(replan_prefix) :].strip() or "Replanejar."
        return {
            "avaliacao_ok": False,
            "feedback_avaliacao": feedback,
            "resposta_final": "",
            "tentativas": state.get("tentativas", 0) + 1,
        }
    return {
        "avaliacao_ok": True,
        "feedback_avaliacao": "",
        "resposta_final": out or "Não foi possível formatar a resposta.",
        "tentativas": state.get("tentativas", 0) + 1,
    }


def _extract_and_resolve_place_node(state: AgentState) -> AgentState:
    """
    Extrai menção a lugar por heurística (sem IA, economiza quota) e resolve com resolve_place
    para nome canônico do município (evita confundir estado com município).
    """
    from src.agent.municipality import extract_place_heuristic, resolve_place, get_municipalities_for_context

    pergunta = state.get("pergunta", "")
    place_phrase = extract_place_heuristic(pergunta)

    if place_phrase:
        resolvidos = resolve_place(place_phrase)
        if resolvidos:
            municipios_contexto = (
                "Municípios resolvidos (use estes valores EXATOS na SQL em municipio_residencia): "
                + ", ".join(f"'{m}'" for m in resolvidos)
                + "."
            )
            return {"place_phrase": place_phrase, "municipios_contexto": municipios_contexto}
    municipios_contexto = get_municipalities_for_context(pergunta)
    return {"place_phrase": place_phrase or "", "municipios_contexto": municipios_contexto}


def _route_after_evaluate(state: AgentState) -> Literal["respond", "plan"]:
    ok = state.get("avaliacao_ok", False)
    tentativas = state.get("tentativas", 0)
    if ok or tentativas >= MAX_TENTATIVAS:
        return "respond"
    return "plan"


def _respond_node(state: AgentState) -> AgentState:
    """No-op: resposta_final já foi preenchida por _evaluate_and_respond_node."""
    return {}


def _run_agent_fallback(pergunta: str, llm_config: Dict[str, Any]) -> dict:
    """
    Fluxo extrair_lugar -> planejar -> executar -> avaliar em Python puro (sem LangGraph).
    """
    state: AgentState = {
        "pergunta": pergunta,
        "place_phrase": "",
        "municipios_contexto": "",
        "tentativas": 0,
        "sql_planejada": "",
        "resultado_execucao": "",
        "resposta_final": "",
        "feedback_avaliacao": "",
        "avaliacao_ok": False,
        "llm_config": llm_config or {},
    }
    state = {**state, **_extract_and_resolve_place_node(state)}

    while True:
        state = {**state, **_plan_node(state, state.get("llm_config") or {})}
        state = {**state, **_execute_node(state)}
        state = {**state, **_evaluate_and_respond_node(state, state.get("llm_config") or {})}
        if state.get("avaliacao_ok") or state.get("tentativas", 0) >= MAX_TENTATIVAS:
            break
    return {
        "resposta_final": state.get("resposta_final", ""),
        "sql_planejada": state.get("sql_planejada", ""),
        "resultado_execucao": state.get("resultado_execucao", ""),
    }


def run_agent(pergunta: str, llm_config: Dict[str, Any]) -> dict:
    """
    Executa o grafo e retorna dict com resposta_final, sql_planejada, resultado_execucao.
    llm_config: dict com provider ('gemini'|'ollama'), model, api_key (Gemini), ollama_base_url (Ollama).
    """
    try:
        from langgraph.graph import StateGraph, START, END
    except ImportError:
        return _run_agent_fallback(pergunta, llm_config)

    initial: AgentState = {
        "pergunta": pergunta,
        "place_phrase": "",
        "municipios_contexto": "",
        "tentativas": 0,
        "sql_planejada": "",
        "resultado_execucao": "",
        "resposta_final": "",
        "feedback_avaliacao": "",
        "avaliacao_ok": False,
        "llm_config": llm_config or {},
    }

    builder = StateGraph(AgentState)

    def extract_and_resolve_place(state: AgentState) -> AgentState:
        return _extract_and_resolve_place_node(state)

    def plan(state: AgentState) -> AgentState:
        return _plan_node(state, state.get("llm_config") or {})

    def execute(state: AgentState) -> AgentState:
        return _execute_node(state)

    def evaluate_and_respond(state: AgentState) -> AgentState:
        return _evaluate_and_respond_node(state, state.get("llm_config") or {})

    def respond(state: AgentState) -> AgentState:
        return _respond_node(state)

    builder.add_node("extract_place", extract_and_resolve_place)
    builder.add_node("plan", plan)
    builder.add_node("execute", execute)
    builder.add_node("evaluate_and_respond", evaluate_and_respond)
    builder.add_node("respond", respond)

    builder.add_edge(START, "extract_place")
    builder.add_edge("extract_place", "plan")
    builder.add_edge("plan", "execute")
    builder.add_edge("execute", "evaluate_and_respond")
    builder.add_conditional_edges(
        "evaluate_and_respond",
        _route_after_evaluate,
        {"respond": "respond", "plan": "plan"},
    )
    builder.add_edge("respond", END)

    graph = builder.compile()
    final = graph.invoke(initial)

    return {
        "resposta_final": final.get("resposta_final", ""),
        "sql_planejada": final.get("sql_planejada", ""),
        "resultado_execucao": final.get("resultado_execucao", ""),
    }
