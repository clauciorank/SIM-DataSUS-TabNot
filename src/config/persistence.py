"""
Persistência de configurações em SQLite.
As configurações sobrevivem ao fechamento do app.
"""
import json
import sqlite3
from pathlib import Path

# Diretório de dados do projeto
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "config.db"

# Valores padrão
DEFAULT_ANOS = list(range(2019, 2025))  # 2019-2024
DEFAULT_UFS = ["Paraná"]


def _get_conn():
    """Retorna conexão com o banco, criando diretório e tabela se necessário."""
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
    """
    Carrega configurações do SQLite.
    Retorna dict com 'filtro_anos' e 'filtro_ufs'.
    """
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
        return {
            "filtro_anos": DEFAULT_ANOS.copy(),
            "filtro_ufs": DEFAULT_UFS.copy(),
        }


def save_config(filtro_anos: list, filtro_ufs: list) -> None:
    """Salva configurações no SQLite."""
    conn = _get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
        ("filtro_anos", json.dumps(filtro_anos)),
    )
    conn.execute(
        "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
        ("filtro_ufs", json.dumps(filtro_ufs)),
    )
    conn.commit()
    conn.close()


# Modelo padrão Gemini (melhor quota no free tier)
DEFAULT_LLM_MODEL_GEMINI = "gemini-3.1-flash-lite-preview"
DEFAULT_LLM_OLLAMA_BASE_URL = "http://localhost:11434"


def load_llm_config() -> dict:
    """
    Carrega configuração do LLM.
    Retorna dict com: api_key, provider ('gemini'|'ollama'), model, ollama_base_url.
    """
    try:
        conn = _get_conn()
        cur = conn.execute(
            "SELECT key, value FROM config WHERE key IN ('llm_api_key', 'llm_provider', 'llm_model', 'llm_ollama_base_url')"
        )
        rows = dict(cur.fetchall())
        conn.close()
        provider = (rows.get("llm_provider") or "gemini").strip() or "gemini"
        return {
            "api_key": (rows.get("llm_api_key") or "").strip(),
            "provider": provider,
            "model": (rows.get("llm_model") or "").strip() or (DEFAULT_LLM_MODEL_GEMINI if provider == "gemini" else ""),
            "ollama_base_url": (rows.get("llm_ollama_base_url") or "").strip() or DEFAULT_LLM_OLLAMA_BASE_URL,
        }
    except Exception:
        return {
            "api_key": "",
            "provider": "gemini",
            "model": DEFAULT_LLM_MODEL_GEMINI,
            "ollama_base_url": DEFAULT_LLM_OLLAMA_BASE_URL,
        }


def save_llm_config(
    api_key: str,
    provider: str = "gemini",
    model: str = "",
    ollama_base_url: str = "",
) -> None:
    """Salva configuração do LLM no SQLite."""
    conn = _get_conn()
    provider = (provider or "gemini").strip() or "gemini"
    model = (model or "").strip() or (DEFAULT_LLM_MODEL_GEMINI if provider == "gemini" else "")
    ollama_base_url = (ollama_base_url or "").strip() or DEFAULT_LLM_OLLAMA_BASE_URL
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", ("llm_api_key", (api_key or "").strip()))
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", ("llm_provider", provider))
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", ("llm_model", model))
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", ("llm_ollama_base_url", ollama_base_url))
    conn.commit()
    conn.close()
