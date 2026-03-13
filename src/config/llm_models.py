"""
Listagem de modelos disponíveis: Gemini (API Google), Groq e Ollama (local).
Usado na página de configurações para popular o seletor de modelo.
"""
from typing import List, Tuple

# Modelos Groq conhecidos (Llama via Groq)
GROQ_FALLBACK_MODELS: List[Tuple[str, str]] = [
    ("llama-3.3-70b-versatile", "Llama 3.3 70B Versatile (Groq)"),
    ("llama-3.1-8b-instant", "Llama 3.1 8B Instant (Groq)"),
    ("llama-3.1-70b-versatile", "Llama 3.1 70B Versatile (Groq)"),
]

# Modelos Gemini conhecidos (fallback quando a API não estiver disponível); melhor quota primeiro
GEMINI_FALLBACK_MODELS: List[Tuple[str, str]] = [
    ("gemini-3.1-flash-lite-preview", "Gemini 3.1 Flash Lite (melhor quota)"),
    ("gemini-2.5-flash-lite", "Gemini 2.5 Flash Lite"),
    ("gemini-2.5-flash", "Gemini 2.5 Flash"),
    ("gemini-2.0-flash", "Gemini 2.0 Flash"),
]


def fetch_gemini_models(api_key: str) -> List[Tuple[str, str]]:
    """
    Lista modelos disponíveis na API Gemini (Google).
    Retorna lista de (model_id, display_name). Em falha, retorna GEMINI_FALLBACK_MODELS.
    """
    if not (api_key or "").strip():
        return list(GEMINI_FALLBACK_MODELS)
    try:
        import urllib.request
        import json
        url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key.strip()}"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        models = data.get("models") or []
        out = []
        seen = set()
        for m in models:
            name = m.get("name") or ""
            if not name.startswith("models/"):
                continue
            model_id = name.replace("models/", "")
            if model_id in seen:
                continue
            seen.add(model_id)
            display = m.get("displayName") or model_id
            out.append((model_id, display))
        if out:
            # Ordenar: preferir flash-lite e 3.1 no topo
            def key(x):
                id_ = x[0].lower()
                if "3.1" in id_ and "lite" in id_:
                    return (0, id_)
                if "lite" in id_:
                    return (1, id_)
                return (2, id_)
            out.sort(key=key)
            return out
    except Exception:
        pass
    return list(GEMINI_FALLBACK_MODELS)


def fetch_ollama_models(base_url: str) -> List[Tuple[str, str]]:
    """
    Lista modelos disponíveis no Ollama (local).
    Retorna lista de (model_id, display_name). Em falha, retorna lista vazia ou padrão.
    """
    base = (base_url or "http://localhost:11434").rstrip("/")
    try:
        import urllib.request
        import json
        url = f"{base}/api/tags"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
        models = data.get("models") or []
        out = []
        for m in models:
            name = m.get("name")
            if not name:
                continue
            # nome pode vir como "llama3.2:latest"
            display = name
            out.append((name, display))
        if out:
            return out
    except Exception:
        pass
    return []
