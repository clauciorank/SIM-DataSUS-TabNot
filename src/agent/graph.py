"""
Grafo LangGraph: plan -> execute -> check_result (determinístico) -> format_response (LLM) -> respond.
Validação determinística: confia no DuckDB (EXPLAIN + execução). LLM só gera SQL e formata resposta.
Suporta Gemini, Anthropic, OpenAI, Ollama e API genérica (OpenAI-compatible); modelo e provedor vêm da configuração.
"""
import json
import logging
import re
import time
from pathlib import Path
from typing import Any, Dict, Literal, TypedDict

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
GOLD_DB = PROJECT_ROOT / "data" / "SIM" / "gold" / "obitos.duckdb"

MAX_ROWS = 500
MAX_TENTATIVAS = 2

class AgentState(TypedDict, total=False):
    pergunta: str
    place_phrase: str
    municipios_contexto: str
    causas_contexto: str
    feedback_avaliacao: str
    historico_falhas: list  # lista de {"sql": str, "erro": str} para não repetir erros no plan
    tentativas: int
    plan_attempts: int  # contagem de vezes que passamos por plan (limita loop execute -> plan)
    sql_planejada: str
    resultado_execucao: str
    resposta_final: str
    avaliacao_ok: bool
    sql_validation_ok: bool  # True se execute (EXPLAIN+query) OK; False para replan
    llm_error: str  # Erro fatal da API (ex.: 403 Forbidden) — interrompe tentativas
    llm_config: Dict[str, Any]


def _gemini_call(api_key: str, model_id: str, system: str, user_content: str, temperature: float = 0.1) -> str:
    import google.generativeai as genai
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(
        model_id or "gemini-3.1-flash-lite-preview",
        generation_config={"temperature": temperature, "max_output_tokens": 2048},
    )
    full = f"{system}\n\n---\n\n{user_content}"
    response = model.generate_content(full)
    if getattr(response, "usage_metadata", None):
        um = response.usage_metadata
        logger.info(
            "gemini tokens: prompt=%s output=%s total=%s",
            getattr(um, "prompt_token_count", None),
            getattr(um, "candidates_token_count", None) or getattr(um, "output_token_count", None),
            getattr(um, "total_token_count", None),
        )
    if not response or not response.text:
        return ""
    return response.text.strip()


def _anthropic_call(api_key: str, model_id: str, system: str, user_content: str, temperature: float = 0.1) -> str:
    """Chama Anthropic Claude (chat completions)."""
    api_key = (api_key or "").strip()
    if not api_key:
        return "Erro Anthropic: Chave API não configurada. Configure em Configurações."
    try:
        from anthropic import Anthropic
        client = Anthropic(api_key=api_key)
        model = (model_id or "claude-3-5-sonnet-20241022").strip()
        msg = client.messages.create(
            model=model,
            max_tokens=2048,
            system=system,
            messages=[{"role": "user", "content": user_content}],
            temperature=temperature,
        )
        if getattr(msg, "usage", None):
            logger.info(
                "anthropic tokens: input=%s output=%s",
                getattr(msg.usage, "input_tokens", None),
                getattr(msg.usage, "output_tokens", None),
            )
        text = (msg.content or [])
        if text and hasattr(text[0], "text"):
            return text[0].text.strip()
        return ""
    except Exception as e:
        return f"Erro Anthropic: {e!s}"


def _openai_call(api_key: str, model_id: str, system: str, user_content: str, temperature: float = 0.1) -> str:
    """Chama OpenAI (chat completions)."""
    api_key = (api_key or "").strip()
    if not api_key:
        return "Erro OpenAI: Chave API não configurada. Configure em Configurações."
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        model = (model_id or "gpt-4o-mini").strip()
        completion = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_content},
            ],
            temperature=temperature,
            max_tokens=2048,
        )
        if getattr(completion, "usage", None):
            u = completion.usage
            logger.info(
                "openai tokens: prompt=%s completion=%s",
                getattr(u, "prompt_tokens", None),
                getattr(u, "completion_tokens", None),
            )
        choice = (completion.choices or [None])[0]
        if not choice or not getattr(choice, "message", None):
            return ""
        return (getattr(choice.message, "content", None) or "").strip()
    except Exception as e:
        return f"Erro OpenAI: {e!s}"


def _generic_call(base_url: str, model_id: str, api_key: str, system: str, user_content: str, temperature: float = 0.1) -> str:
    """Chama API compatível com OpenAI (ex.: LM Studio, Together, Groq)."""
    base = (base_url or "https://api.openai.com/v1").rstrip("/")
    model = (model_id or "gpt-4o").strip()
    try:
        from openai import OpenAI
        client = OpenAI(base_url=base, api_key=(api_key or "dummy").strip())
        completion = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_content},
            ],
            temperature=temperature,
            max_tokens=2048,
        )
        choice = (completion.choices or [None])[0]
        if not choice or not getattr(choice, "message", None):
            return ""
        return (getattr(choice.message, "content", None) or "").strip()
    except Exception as e:
        return f"Erro API genérica: {e!s}"


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
        prompt_tokens = data.get("prompt_eval_count")
        eval_tokens = data.get("eval_count")
        if prompt_tokens is not None or eval_tokens is not None:
            logger.info(
                "ollama tokens: prompt=%s output=%s",
                prompt_tokens,
                eval_tokens,
            )
        return (data.get("response") or "").strip()
    except Exception as e:
        return f"Erro Ollama: {e!s}"


def _llm_call(llm_config: Dict[str, Any], system: str, user_content: str, temperature: float = 0.1) -> str:
    """Despacha para Gemini, Anthropic, OpenAI, generic ou Ollama conforme llm_config."""
    if not llm_config:
        return ""
    provider = (llm_config.get("provider") or "gemini").strip().lower()
    model = (llm_config.get("model") or "").strip()
    model_display = model or "default"
    t0 = time.perf_counter()
    out = ""
    if provider == "ollama":
        base = (llm_config.get("ollama_base_url") or "http://localhost:11434").strip()
        out = _ollama_call(base, model or "llama3.2", system, user_content, temperature)
    elif provider == "anthropic":
        api_key = (llm_config.get("api_key") or "").strip()
        if not api_key:
            return "Erro: Chave API Anthropic não configurada."
        out = _anthropic_call(api_key, model or "claude-3-5-sonnet-20241022", system, user_content, temperature)
    elif provider == "openai":
        api_key = (llm_config.get("api_key") or "").strip()
        if not api_key:
            return "Erro: Chave API OpenAI não configurada."
        out = _openai_call(api_key, model or "gpt-4o-mini", system, user_content, temperature)
    elif provider == "generic":
        base_url = (llm_config.get("generic_base_url") or "https://api.openai.com/v1").strip()
        api_key = (llm_config.get("api_key") or "").strip()
        out = _generic_call(
            base_url,
            model or llm_config.get("generic_model") or "gpt-4o",
            api_key,
            system,
            user_content,
            temperature,
        )
    else:
        api_key = (llm_config.get("api_key") or "").strip()
        if not api_key:
            return "Erro: Chave API Gemini não configurada."
        out = _gemini_call(api_key, model or "gemini-3.1-flash-lite-preview", system, user_content, temperature)
    elapsed = time.perf_counter() - t0
    logger.info("llm_call %s %s %.1fs -> %d chars", provider, model_display, elapsed, len(out or ""))
    logger.debug("llm_call user_content (first 300 chars): %s", (user_content or "")[:300])
    return out


def _safe_sql(sql: str) -> bool:
    """Permite apenas SELECT e WITH (read-only)."""
    normalized = re.sub(r"\s+", " ", sql.strip().upper())
    if not normalized:
        return False
    for forbidden in ("INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE", "GRANT"):
        if forbidden in normalized:
            return False
    return normalized.startswith("SELECT") or normalized.startswith("WITH")


def _extract_sql_from_plan_output(out: str) -> str:
    """
    Extrai a query SQL da resposta do LLM (plan). Compatível com Gemini, Anthropic, OpenAI, Ollama e outros.
    Aceita: JSON puro, blocos ```json/```sql, ou texto com objeto {"sql": "..."}.
    """
    if not (out or "").strip():
        return ""
    out = out.strip()
    sql = ""

    # 1) Bloco ```json ... ```
    for block in re.findall(r"```(?:json)?\s*([\s\S]*?)```", out):
        try:
            data = json.loads(block.strip())
            if isinstance(data.get("sql"), str):
                sql = data["sql"].strip()
                if sql:
                    return sql
        except (json.JSONDecodeError, TypeError):
            continue

    # 2) Bloco ```sql ... ``` (Llama às vezes devolve assim)
    for block in re.findall(r"```sql\s*([\s\S]*?)```", out, re.IGNORECASE):
        cand = block.strip()
        if cand.upper().startswith("SELECT") or "SELECT" in cand.upper():
            return cand
    # Qualquer ``` com conteúdo que pareça SQL
    for block in re.findall(r"```\s*([\s\S]*?)```", out):
        cand = block.strip()
        if cand.upper().startswith("SELECT") or (cand.upper().startswith("WITH") and "SELECT" in cand.upper()):
            return cand

    # 3) JSON na linha / texto inteiro
    try:
        data = json.loads(out)
        if isinstance(data.get("sql"), str):
            sql = data["sql"].strip()
            if sql:
                return sql
    except (json.JSONDecodeError, TypeError):
        pass

    # 4) Regex: "sql": "..." (valor pode ter \", \n, etc.)
    m = re.search(r'"sql"\s*:\s*"((?:[^"\\]|\\.)*)"', out)
    if m:
        raw = m.group(1)
        sql = raw.replace('\\"', '"').replace("\\n", "\n").replace("\\\\", "\\").strip()
        if sql.upper().startswith(("SELECT", "WITH")):
            return sql
    # 5) Regex com aspas simples (alguns modelos)
    m = re.search(r"'sql'\s*:\s*'((?:[^'\\]|\\.)*)'", out)
    if m:
        sql = m.group(1).replace("\\'", "'").replace("\\n", "\n").strip()
        if sql.upper().startswith(("SELECT", "WITH")):
            return sql

    # 6) Primeiro objeto JSON no texto (ex.: "Aqui está: {\"sql\": \"SELECT ...\"}")
    start = out.find("{")
    if start >= 0:
        depth = 0
        for i in range(start, len(out)):
            if out[i] == "{":
                depth += 1
            elif out[i] == "}":
                depth -= 1
                if depth == 0:
                    try:
                        data = json.loads(out[start : i + 1])
                        if isinstance(data.get("sql"), str):
                            sql = data["sql"].strip()
                            if sql.upper().startswith(("SELECT", "WITH")):
                                return sql
                    except (json.JSONDecodeError, TypeError):
                        pass
                    break

    return ""


def _plan_node(state: AgentState, llm_config: Dict[str, Any]) -> AgentState:
    from src.agent.schema import get_schema_from_view, PLAN_SYSTEM
    from src.agent.cause_context import get_cause_context_for_plan
    from src.agent.schema_enricher import build_rich_schema

    pergunta = state.get("pergunta", "")
    municipios_contexto = state.get("municipios_contexto", "")
    causas_contexto = state.get("causas_contexto", "")
    if not causas_contexto:
        causas_contexto = get_cause_context_for_plan(pergunta)
    feedback = state.get("feedback_avaliacao", "")
    historico = state.get("historico_falhas") or []

    user_msg = f"Pergunta do usuário: {pergunta}\n\n"
    if municipios_contexto:
        user_msg += f"Contexto de lugar (use EXATAMENTE na SQL): {municipios_contexto}\n\n"
    if causas_contexto:
        user_msg += f"Contexto de causa/doença (use EXATAMENTE na SQL): {causas_contexto}\n\n"

    if historico:
        user_msg += "TENTATIVAS ANTERIORES QUE FALHARAM (não repita estes erros):\n"
        for i, h in enumerate(historico, 1):
            user_msg += f"\nTentativa {i}:\nSQL:\n{h.get('sql', '')}\nMotivo da falha: {h.get('erro', '')}\n"
        user_msg += "\n"
    elif feedback:
        user_msg += f"Ajuste necessário: {feedback}\n\n"

    from src.agent.intent import classify_intent, INTENT_SQL_HINT
    intent = classify_intent(pergunta)
    hint = INTENT_SQL_HINT.get(intent, "")
    if hint:
        user_msg += f"\nINTENT DETECTADO: {hint}\n\n"

    user_msg += "Gere apenas um JSON com chave 'sql' contendo a query DuckDB."
    full_system = f"{PLAN_SYSTEM}\n\n{get_schema_from_view()}"
    rich_schema = build_rich_schema()
    if rich_schema:
        full_system += f"\n\nDADOS REAIS DA BASE (use para calibrar filtros):\n{rich_schema}"

    plan_attempts = state.get("plan_attempts", 0) + 1
    logger.info("plan node attempt=%d historico_falhas=%d", plan_attempts, len(historico))
    out = _llm_call(llm_config, full_system, user_msg, temperature=0.1)
    out_stripped = (out or "").strip()
    # Erro fatal da API (403, rede, etc.) — não repete tentativas
    if out_stripped.startswith(("Erro Anthropic:", "Erro OpenAI:", "Erro API genérica:", "Erro Ollama:", "Erro ao conectar", "Erro:")):
        logger.warning("plan node erro fatal da API: %s", out_stripped[:200])
        return {"sql_planejada": "", "plan_attempts": plan_attempts, "llm_error": out_stripped}
    sql = _extract_sql_from_plan_output(out or "")
    if not sql:
        logger.warning(
            "plan node LLM retornou sem SQL válida. Raw (first 600 chars): %s",
            out_stripped[:600],
        )
    logger.info("plan node -> sql len=%d", len(sql or ""))
    logger.debug("plan node sql: %s", (sql or "")[:500])
    return {"sql_planejada": sql or "", "plan_attempts": plan_attempts}


def _execute_node(state: AgentState) -> AgentState:
    """
    Uma única conexão: valida com EXPLAIN e, se OK, executa a query.
    Evita duas conexões (validate_sql + execute) por request.
    """
    sql = (state.get("sql_planejada") or "").strip()
    if not sql:
        logger.warning("execute: nenhuma query")
        return {"resultado_execucao": "Erro: nenhuma query gerada.", "sql_validation_ok": False}
    if not _safe_sql(sql):
        logger.warning("execute: query não permitida")
        return {"resultado_execucao": "Erro: apenas consultas SELECT são permitidas.", "sql_validation_ok": False}
    try:
        import duckdb
        con = duckdb.connect(str(GOLD_DB), read_only=True)
        try:
            con.execute(f"EXPLAIN {sql}")
        except Exception as e:
            logger.warning("execute EXPLAIN falhou: %s", e)
            historico = list(state.get("historico_falhas") or [])
            historico.append({"sql": sql, "erro": f"Erro ao validar SQL no banco: {e!s}"[:300]})
            return {
                "resultado_execucao": "",
                "sql_validation_ok": False,
                "feedback_avaliacao": f"Erro ao validar SQL no banco: {e!s}"[:500],
                "historico_falhas": historico,
            }
        try:
            df = con.execute(sql).fetchdf()
            if df is not None and len(df) > MAX_ROWS:
                df = df.head(MAX_ROWS)
            result_str = df.to_string() if df is not None and not df.empty else "(vazio)"
            rows = len(df) if df is not None else 0
            logger.info("execute OK rows=%d", rows)
            return {"resultado_execucao": result_str, "sql_validation_ok": True}
        finally:
            con.close()
    except Exception as e:
        result_str = f"Erro ao executar: {e!s}"
        logger.warning("execute erro: %s", e)
        return {"resultado_execucao": result_str, "sql_validation_ok": False}


def _check_result_node(state: AgentState) -> AgentState:
    """
    Verificação determinística do resultado (sem LLM).
    Se o DuckDB executou sem erro e retornou dados, confia no resultado.
    Só pede replan se o resultado estiver vazio.
    """
    resultado = (state.get("resultado_execucao") or "").strip()
    if not resultado or resultado == "(vazio)":
        historico = list(state.get("historico_falhas") or [])
        historico.append({
            "sql": state.get("sql_planejada", ""),
            "erro": "Resultado vazio — tente filtros mais amplos ou remova filtros desnecessários",
        })
        logger.warning("check_result: resultado vazio, pedindo replan")
        return {
            "avaliacao_ok": False,
            "feedback_avaliacao": "Resultado vazio — tente filtros mais amplos ou remova filtros desnecessários",
            "historico_falhas": historico,
            "tentativas": state.get("tentativas", 0) + 1,
        }
    logger.info("check_result: OK, resultado tem dados")
    return {
        "avaliacao_ok": True,
        "tentativas": state.get("tentativas", 0) + 1,
    }


def _format_response_node(state: AgentState, llm_config: Dict[str, Any]) -> AgentState:
    """
    Formata o resultado em linguagem natural para o usuário (1 chamada LLM).
    Não faz validação nem REPLAN — apenas formata os dados.
    """
    from src.agent.schema import FORMAT_RESPONSE_SYSTEM
    pergunta = state.get("pergunta", "")
    sql = state.get("sql_planejada", "")
    resultado = (state.get("resultado_execucao") or "")[:2000]
    user_msg = (
        f"Pergunta do usuário: {pergunta}\n\n"
        f"Query SQL executada:\n{sql}\n\n"
        f"Resultado:\n{resultado}\n\n"
        "Escreva a resposta para o usuário com base nos dados acima."
    )
    logger.info("format_response: chamando LLM")
    out = _llm_call(llm_config, FORMAT_RESPONSE_SYSTEM, user_msg, temperature=0.2)
    resposta = (out or "").strip()
    if not resposta:
        resposta = "Não foi possível formatar a resposta."
    logger.info("format_response OK resposta len=%d", len(resposta))
    return {"resposta_final": resposta}


UF_SIGLAS = frozenset(
    {"AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT",
     "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"}
)


def _extract_and_resolve_place_node(state: AgentState) -> AgentState:
    """
    Extrai menção a lugar por heurística (sem IA, economiza quota) e resolve com resolve_place
    para nome canônico do município (evita confundir estado com município).
    Se a frase for sigla de UF, usa uf_residencia diretamente.
    """
    from src.agent.municipality import extract_place_heuristic, resolve_place

    pergunta = state.get("pergunta", "")
    place_phrase = extract_place_heuristic(pergunta)

    logger.info("extract_place place_phrase=%r", (place_phrase or "")[:80])

    # Sigla de UF: filtro por estado, não por município
    if place_phrase and place_phrase.upper() in UF_SIGLAS:
        uf = place_phrase.upper()
        municipios_contexto = (
            f"Estado resolvido (use EXATAMENTE na SQL em uf_residencia): '{uf}'."
        )
        logger.info("extract_place UF resolvida: %s", uf)
        return {"place_phrase": place_phrase, "municipios_contexto": municipios_contexto}

    if place_phrase:
        resolvidos = resolve_place(place_phrase)
        if resolvidos:
            municipios_contexto = (
                "Municípios resolvidos (use estes valores EXATOS na SQL em municipio_residencia): "
                + ", ".join(f"'{m}'" for m in resolvidos)
                + "."
            )
            logger.info("extract_place municipios resolvidos: %s", resolvidos[:5] if len(resolvidos) > 5 else resolvidos)
            return {"place_phrase": place_phrase, "municipios_contexto": municipios_contexto}
    # Sem menção explícita a lugar (em/na/no + nome): NÃO inferir município por fuzzy em palavras soltas
    # (evita "cardiovasculares"/"mensal" etc. baterem em Japaratinga ou outro município por acaso)
    logger.info("extract_place: sem lugar explícito, municipios_contexto vazio")
    return {"place_phrase": place_phrase or "", "municipios_contexto": ""}


def _format_dual_cause_context(
    pergunta: str,
    contexto_codigo: str,
    contexto_capitulo: str,
) -> str:
    """
    Monta instrução para o LLM com OPÇÃO A (capítulo) e OPÇÃO B (códigos).
    O LLM escolhe UMA conforme a pergunta (grupos amplos → A; doença específica → B).
    """
    if not contexto_codigo and not contexto_capitulo:
        return ""
    partes = [
        "FILTRO DE CAUSA — escolha UMA das opções abaixo conforme a pergunta:\n"
    ]
    if contexto_capitulo:
        partes.append(
            "  OPÇÃO A (use quando a pergunta é sobre um GRUPO AMPLO de doenças, "
            "ex.: doenças cardiovasculares, doenças do aparelho circulatório, neoplasias, causas externas):\n"
            f"  {contexto_capitulo}\n"
        )
    if contexto_codigo:
        partes.append(
            "  OPÇÃO B (use quando a pergunta é sobre uma doença ESPECÍFICA, "
            "ex.: infarto, dengue, pneumonia, neoplasia óssea):\n"
            f"  {contexto_codigo}\n"
        )
    partes.append(
        "  REGRA: Termos genéricos (doenças do aparelho X, neoplasias, causas externas) → use OPÇÃO A. "
        "Doença específica → use OPÇÃO B. Nunca misture as duas. Nunca use ILIKE em texto livre. Nunca invente códigos."
    )
    return "\n".join(partes)


def _resolve_cause_node(state: AgentState) -> AgentState:
    """
    Resolve causa/doença: busca sempre os dois candidatos (capítulo e códigos),
    formata como OPÇÃO A / OPÇÃO B e deixa o LLM escolher no plan.
    """
    from src.agent.cid10_resolver import (
        extract_cause_phrase_heuristic,
        format_causas_for_sql,
        search_cid10,
        search_cid10_chapters,
    )
    from src.agent.cause_context import get_cause_context_for_plan

    pergunta = state.get("pergunta", "")
    termo = extract_cause_phrase_heuristic(pergunta)
    termo = (termo or "").strip().rstrip("?.,;:!").strip() if termo else ""

    contexto_codigo = ""
    contexto_capitulo = ""

    if termo:
        code_matches = search_cid10(termo, limit=15, min_score=68)
        if code_matches:
            contexto_codigo = format_causas_for_sql(code_matches, by_chapter=False)
        chapter_matches = search_cid10_chapters(termo, limit=3)
        if chapter_matches:
            contexto_capitulo = format_causas_for_sql(
                [{"capitulo_descricao": c} for c in chapter_matches],
                by_chapter=True,
            )

    if not contexto_codigo and not contexto_capitulo:
        fallback = get_cause_context_for_plan(pergunta)
        causas_contexto = fallback or ""
    else:
        causas_contexto = _format_dual_cause_context(pergunta, contexto_codigo, contexto_capitulo)

    logger.info("resolve_cause causas_contexto len=%d", len(causas_contexto or ""))
    return {"causas_contexto": causas_contexto or ""}


def _route_after_execute(state: AgentState) -> Literal["check_result", "plan", "give_up"]:
    """Após execute: se sql_validation_ok -> check_result; senão, replan ou give_up."""
    if state.get("sql_validation_ok") is True:
        return "check_result"
    if state.get("llm_error"):
        return "give_up"
    if state.get("plan_attempts", 0) >= MAX_TENTATIVAS:
        return "give_up"
    return "plan"


def _route_after_check(state: AgentState) -> Literal["format_response", "plan", "give_up"]:
    """Após check_result: se dados OK -> format_response; se vazio -> replan ou give_up."""
    if state.get("avaliacao_ok"):
        return "format_response"
    if state.get("tentativas", 0) >= MAX_TENTATIVAS:
        return "give_up"
    return "plan"


def _give_up_node(state: AgentState) -> AgentState:
    """Preenche resposta_final quando esgotamos tentativas ou houve erro fatal da API (ex.: 403)."""
    from src.agent.messages import MSG_NAO_CONSEGUIU_CONSULTA
    llm_error = (state.get("llm_error") or "").strip()
    if llm_error:
        logger.warning("give_up: erro da API -> %s", llm_error[:150])
        msg = llm_error
        if "403" in llm_error:
            msg += " Verifique a chave API em Configurações e se o modelo está disponível."
        return {"resposta_final": msg}
    logger.warning("give_up: esgotadas tentativas (plan_attempts=%s)", state.get("plan_attempts", 0))
    return {"resposta_final": MSG_NAO_CONSEGUIU_CONSULTA}


def _respond_node(state: AgentState) -> AgentState:
    """
    Resposta final já foi preenchida por _evaluate_and_respond_node (ou _give_up_node).
    Se esgotamos tentativas e resposta_final ainda está vazia, usa mensagem centralizada.
    """
    from src.agent.messages import MSG_NAO_CONSEGUIU_CONSULTA
    resposta = state.get("resposta_final", "")
    tentativas = state.get("tentativas", 0)
    if not resposta and tentativas >= MAX_TENTATIVAS:
        return {"resposta_final": MSG_NAO_CONSEGUIU_CONSULTA}
    return {}


def _run_agent_fallback(pergunta: str, llm_config: Dict[str, Any]) -> dict:
    """
    Fluxo em Python puro (sem LangGraph):
    extract_place -> resolve_cause -> plan -> execute -> check_result -> format_response.
    Guardrail já foi checado em run_agent antes de chegar aqui.
    """
    logger.info("run_agent_fallback start")
    from src.agent.messages import MSG_NAO_CONSEGUIU_CONSULTA
    state: AgentState = {
        "pergunta": pergunta,
        "place_phrase": "",
        "municipios_contexto": "",
        "causas_contexto": "",
        "historico_falhas": [],
        "tentativas": 0,
        "sql_planejada": "",
        "resultado_execucao": "",
        "resposta_final": "",
        "feedback_avaliacao": "",
        "avaliacao_ok": False,
        "sql_validation_ok": False,
        "llm_config": llm_config or {},
    }
    from src.agent.db_cache import warm_all_caches
    warm_all_caches()

    state = {**state, **_extract_and_resolve_place_node(state)}
    state = {**state, **_resolve_cause_node(state)}

    plan_calls = 0
    while True:
        plan_calls += 1
        state = {**state, **_plan_node(state, state.get("llm_config") or {})}
        if state.get("llm_error"):
            state["resposta_final"] = state["llm_error"]
            if "403" in state["llm_error"]:
                state["resposta_final"] += " Verifique a chave API em Configurações e se o modelo está disponível."
            break
        state = {**state, **_execute_node(state)}
        if not state.get("sql_validation_ok"):
            if plan_calls >= MAX_TENTATIVAS:
                state["resposta_final"] = MSG_NAO_CONSEGUIU_CONSULTA
                state["tentativas"] = MAX_TENTATIVAS
                break
            continue
        state = {**state, **_check_result_node(state)}
        if state.get("avaliacao_ok"):
            state = {**state, **_format_response_node(state, state.get("llm_config") or {})}
            break
        if state.get("tentativas", 0) >= MAX_TENTATIVAS:
            break
    resposta_final = state.get("resposta_final", "")
    if not resposta_final and state.get("tentativas", 0) >= MAX_TENTATIVAS:
        resposta_final = MSG_NAO_CONSEGUIU_CONSULTA
    logger.info("run_agent_fallback end resposta_len=%d plan_calls=%d", len(resposta_final or ""), plan_calls)
    return {
        "resposta_final": resposta_final,
        "sql_planejada": state.get("sql_planejada", ""),
        "resultado_execucao": state.get("resultado_execucao", ""),
    }


def run_agent(pergunta: str, llm_config: Dict[str, Any]) -> dict:
    """
    Executa o grafo e retorna dict com resposta_final, sql_planejada, resultado_execucao.
    llm_config: dict com provider (gemini|anthropic|openai|generic|ollama), model, api_key, ollama_base_url, generic_base_url, generic_model.
    Perguntas fora do tema (óbitos/SIM) são rejeitadas pelo guardrail sem consumir o agente.
    """
    provider = (llm_config or {}).get("provider") or "gemini"
    logger.info("run_agent start pergunta_len=%d provider=%s", len(pergunta or ""), provider)

    from src.agent.guardrail import is_on_topic, get_reject_message
    if not is_on_topic(pergunta):
        logger.info("run_agent guardrail rejeitou (off-topic)")
        return {
            "resposta_final": get_reject_message(),
            "sql_planejada": "",
            "resultado_execucao": "",
        }
    try:
        from langgraph.graph import StateGraph, START, END
    except ImportError:
        logger.info("run_agent LangGraph indisponível, usando fallback")
        return _run_agent_fallback(pergunta, llm_config)

    from src.agent.db_cache import warm_all_caches
    warm_all_caches()

    initial: AgentState = {
        "pergunta": pergunta,
        "place_phrase": "",
        "municipios_contexto": "",
        "causas_contexto": "",
        "historico_falhas": [],
        "tentativas": 0,
        "plan_attempts": 0,
        "sql_planejada": "",
        "resultado_execucao": "",
        "resposta_final": "",
        "feedback_avaliacao": "",
        "avaliacao_ok": False,
        "sql_validation_ok": False,
        "llm_error": "",
        "llm_config": llm_config or {},
    }

    builder = StateGraph(AgentState)

    def extract_and_resolve_place(state: AgentState) -> AgentState:
        return _extract_and_resolve_place_node(state)

    def resolve_cause(state: AgentState) -> AgentState:
        return _resolve_cause_node(state)

    def plan(state: AgentState) -> AgentState:
        return _plan_node(state, state.get("llm_config") or {})

    def execute(state: AgentState) -> AgentState:
        return _execute_node(state)

    def check_result(state: AgentState) -> AgentState:
        return _check_result_node(state)

    def format_response(state: AgentState) -> AgentState:
        return _format_response_node(state, state.get("llm_config") or {})

    def give_up(state: AgentState) -> AgentState:
        return _give_up_node(state)

    def respond(state: AgentState) -> AgentState:
        return _respond_node(state)

    builder.add_node("extract_place", extract_and_resolve_place)
    builder.add_node("resolve_cause", resolve_cause)
    builder.add_node("plan", plan)
    builder.add_node("execute", execute)
    builder.add_node("check_result", check_result)
    builder.add_node("format_response", format_response)
    builder.add_node("give_up", give_up)
    builder.add_node("respond", respond)

    builder.add_edge(START, "extract_place")
    builder.add_edge("extract_place", "resolve_cause")
    builder.add_edge("resolve_cause", "plan")
    builder.add_edge("plan", "execute")
    builder.add_conditional_edges(
        "execute",
        _route_after_execute,
        {"check_result": "check_result", "plan": "plan", "give_up": "give_up"},
    )
    builder.add_conditional_edges(
        "check_result",
        _route_after_check,
        {"format_response": "format_response", "plan": "plan", "give_up": "give_up"},
    )
    builder.add_edge("format_response", "respond")
    builder.add_edge("give_up", "respond")
    builder.add_edge("respond", END)

    graph = builder.compile()
    final = graph.invoke(initial)

    logger.info(
        "run_agent end resposta_len=%d sql_len=%d",
        len(final.get("resposta_final") or ""),
        len(final.get("sql_planejada") or ""),
    )
    return {
        "resposta_final": final.get("resposta_final", ""),
        "sql_planejada": final.get("sql_planejada", ""),
        "resultado_execucao": final.get("resultado_execucao", ""),
    }
