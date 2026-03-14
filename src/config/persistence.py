"""
Persistência de configurações em SQLite.
Chaves API ficam criptografadas em data/llm_secrets.enc (por provedor).
"""
import json
import sqlite3
from pathlib import Path

from src.config.secrets import load_api_keys, save_api_key, KEY_PROVIDERS

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "config.db"

DEFAULT_ANOS = list(range(2019, 2025))
DEFAULT_UFS = ["Paraná"]

# Modelos padrão por provedor
DEFAULT_LLM_MODEL_GEMINI = "gemini-3.1-flash-lite-preview"
DEFAULT_LLM_MODEL_ANTHROPIC = "claude-3-5-sonnet-20241022"
DEFAULT_LLM_MODEL_OPENAI = "gpt-4o-mini"
DEFAULT_LLM_OLLAMA_BASE_URL = "http://localhost:11434"


def _get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


def load_config() -> dict:
    try:
        conn = _get_conn()
        cur = conn.execute(
            "SELECT key, value FROM config WHERE key IN ('filtro_anos', 'filtro_ufs')"
        )
        rows = dict(cur.fetchall())
        conn.close()
        result = {}
        if "filtro_anos" in rows:
            result["filtro_anos"] = json.loads(rows["filtro_anos"])
        else:
            result["filtro_anos"] = DEFAULT_ANOS.copy()
        if "filtro_ufs" in rows:
            result["filtro_ufs"] = json.loads(rows["filtro_ufs"])
        else:
            result["filtro_ufs"] = DEFAULT_UFS.copy()
        return result
    except Exception:
        return {"filtro_anos": DEFAULT_ANOS.copy(), "filtro_ufs": DEFAULT_UFS.copy()}


def save_config(filtro_anos: list, filtro_ufs: list) -> None:
    conn = _get_conn()
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", ("filtro_anos", json.dumps(filtro_anos)))
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", ("filtro_ufs", json.dumps(filtro_ufs)))
    conn.commit()
    conn.close()


def _default_model_for_provider(provider: str) -> str:
    if provider == "gemini":
        return DEFAULT_LLM_MODEL_GEMINI
    if provider == "anthropic":
        return DEFAULT_LLM_MODEL_ANTHROPIC
    if provider == "openai":
        return DEFAULT_LLM_MODEL_OPENAI
    if provider == "ollama":
        return "llama3.2"
    if provider == "generic":
        return "gpt-4o"
    return ""


def load_llm_config() -> dict:
    """
    Retorna dict com: api_key (do provedor atual), provider, model, ollama_base_url, generic_base_url, generic_model.
    Chaves vêm do store criptografado por provedor.
    """
    try:
        conn = _get_conn()
        cur = conn.execute(
            "SELECT key, value FROM config WHERE key IN ("
            "'llm_provider', 'llm_model', 'llm_ollama_base_url', 'llm_generic_base_url', 'llm_generic_model')"
        )
        rows = dict(cur.fetchall())
        conn.close()
        provider = (rows.get("llm_provider") or "gemini").strip() or "gemini"
        if provider not in ("gemini", "anthropic", "openai", "ollama", "generic"):
            provider = "gemini"
        default_model = _default_model_for_provider(provider)
        model = (rows.get("llm_model") or "").strip() or default_model
        ollama_base_url = (rows.get("llm_ollama_base_url") or "").strip() or DEFAULT_LLM_OLLAMA_BASE_URL
        generic_base_url = (rows.get("llm_generic_base_url") or "").strip()
        generic_model = (rows.get("llm_generic_model") or "").strip()

        api_keys = load_api_keys()
        api_key = (api_keys.get(provider) or "").strip()

        return {
            "api_key": api_key,
            "provider": provider,
            "model": model,
            "ollama_base_url": ollama_base_url,
            "generic_base_url": generic_base_url,
            "generic_model": generic_model,
        }
    except Exception:
        return {
            "api_key": "",
            "provider": "gemini",
            "model": DEFAULT_LLM_MODEL_GEMINI,
            "ollama_base_url": DEFAULT_LLM_OLLAMA_BASE_URL,
            "generic_base_url": "",
            "generic_model": "",
        }


def save_llm_config(
    provider: str = "gemini",
    api_key: str = "",
    model: str = "",
    ollama_base_url: str = "",
    generic_base_url: str = "",
    generic_model: str = "",
) -> None:
    provider = (provider or "gemini").strip() or "gemini"
    if provider not in ("gemini", "anthropic", "openai", "ollama", "generic"):
        provider = "gemini"
    default_model = _default_model_for_provider(provider)
    model = (model or "").strip() or default_model
    ollama_base_url = (ollama_base_url or "").strip() or DEFAULT_LLM_OLLAMA_BASE_URL

    if provider in KEY_PROVIDERS and api_key:
        try:
            save_api_key(provider, api_key)
        except Exception:
            pass

    conn = _get_conn()
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", ("llm_provider", provider))
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", ("llm_model", model))
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", ("llm_ollama_base_url", ollama_base_url))
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", ("llm_generic_base_url", (generic_base_url or "").strip()))
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", ("llm_generic_model", (generic_model or "").strip()))
    conn.commit()
    conn.close()
