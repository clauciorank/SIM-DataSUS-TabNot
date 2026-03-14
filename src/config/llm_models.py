"""
Listagem de modelos de texto (chat) por provedor: Gemini, Anthropic, OpenAI, Ollama.
Apenas modelos para geração de texto; exclui embedding, imagen, nano, etc.
"""
from typing import List, Tuple

# Gemini: apenas modelos de chat, modernos ou free tier
GEMINI_FALLBACK_MODELS: List[Tuple[str, str]] = [
    ("gemini-3.1-flash-lite-preview", "Gemini 3.1 Flash Lite (melhor quota)"),
    ("gemini-2.5-flash-lite", "Gemini 2.5 Flash Lite"),
    ("gemini-2.5-flash", "Gemini 2.5 Flash"),
    ("gemini-2.0-flash", "Gemini 2.0 Flash"),
    ("gemini-1.5-flash", "Gemini 1.5 Flash"),
    ("gemini-1.5-pro", "Gemini 1.5 Pro"),
]

# Substrings que indicam modelo não-texto (Gemini)
GEMINI_EXCLUDE = ("embedding", "imagen", "nano", "embed", "pulse", "codey")

# Anthropic Claude (texto)
ANTHROPIC_FALLBACK_MODELS: List[Tuple[str, str]] = [
    ("claude-3-5-sonnet-20241022", "Claude 3.5 Sonnet"),
    ("claude-3-5-haiku-20241022", "Claude 3.5 Haiku"),
    ("claude-3-opus-20240229", "Claude 3 Opus"),
    ("claude-3-sonnet-20240229", "Claude 3 Sonnet"),
    ("claude-3-haiku-20240307", "Claude 3 Haiku"),
]

# OpenAI (apenas chat)
OPENAI_FALLBACK_MODELS: List[Tuple[str, str]] = [
    ("gpt-4o", "GPT-4o"),
    ("gpt-4o-mini", "GPT-4o Mini"),
    ("gpt-4-turbo", "GPT-4 Turbo"),
    ("gpt-3.5-turbo", "GPT-3.5 Turbo"),
]

# Ollama: excluir modelos não-texto por nome
OLLAMA_EXCLUDE_SUBSTRINGS = ("nano", "banana", "embed", "vision", ":vision")


def _gemini_is_text_model(model_id: str) -> bool:
    id_lower = model_id.lower()
    if any(x in id_lower for x in GEMINI_EXCLUDE):
        return False
    if not id_lower.startswith("gemini-"):
        return False
    return True


def fetch_gemini_models(api_key: str) -> List[Tuple[str, str]]:
    """
    Lista modelos Gemini de texto (chat). Exclui embedding, imagen, nano.
    """
    if not (api_key or "").strip():
        return list(GEMINI_FALLBACK_MODELS)
    try:
        import urllib.request
        import json as _json
        url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key.strip()}"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = _json.loads(resp.read().decode())
        models = data.get("models") or []
        out = []
        seen = set()
        for m in models:
            name = m.get("name") or ""
            if not name.startswith("models/"):
                continue
            model_id = name.replace("models/", "")
            if model_id in seen or not _gemini_is_text_model(model_id):
                continue
            seen.add(model_id)
            display = m.get("displayName") or model_id
            out.append((model_id, display))
        if out:
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


def fetch_anthropic_models(api_key: str) -> List[Tuple[str, str]]:
    """Lista modelos Claude (texto). API não expõe listagem; usamos fallback."""
    if not (api_key or "").strip():
        return list(ANTHROPIC_FALLBACK_MODELS)
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key.strip())
        # Anthropic não tem listagem pública de modelos; usar fallback
        return list(ANTHROPIC_FALLBACK_MODELS)
    except Exception:
        return list(ANTHROPIC_FALLBACK_MODELS)


def fetch_openai_models(api_key: str) -> List[Tuple[str, str]]:
    """Lista modelos OpenAI de chat. Exclui embedding, whisper, etc."""
    if not (api_key or "").strip():
        return list(OPENAI_FALLBACK_MODELS)
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key.strip())
        models = list(client.models.list())
        out = []
        for m in models:
            mid = getattr(m, "id", None) or ""
            if not mid:
                continue
            mid_lower = mid.lower()
            if "embed" in mid_lower or "whisper" in mid_lower or "davinci" in mid_lower and "gpt" not in mid_lower:
                continue
            if mid_lower.startswith("gpt-4") or mid_lower.startswith("gpt-3.5"):
                out.append((mid, mid))
        if out:
            out.sort(key=lambda x: (0 if x[0].startswith("gpt-4") else 1, x[0]))
            return out
    except Exception:
        pass
    return list(OPENAI_FALLBACK_MODELS)


def fetch_ollama_models(base_url: str) -> List[Tuple[str, str]]:
    """
    Lista modelos Ollama (local). Exclui nomes com nano, banana, embed, vision.
    """
    base = (base_url or "http://localhost:11434").rstrip("/")
    try:
        import urllib.request
        import json as _json
        url = f"{base}/api/tags"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = _json.loads(resp.read().decode())
        models = data.get("models") or []
        out = []
        for m in models:
            name = m.get("name") or ""
            if not name:
                continue
            name_lower = name.lower()
            if any(x in name_lower for x in OLLAMA_EXCLUDE_SUBSTRINGS):
                continue
            out.append((name, name))
        return out
    except Exception:
        pass
    return []
