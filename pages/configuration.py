import streamlit as st
from src.config.persistence import (
    load_llm_config,
    save_llm_config,
    DEFAULT_LLM_MODEL_GEMINI,
    DEFAULT_LLM_MODEL_ANTHROPIC,
    DEFAULT_LLM_MODEL_OPENAI,
    DEFAULT_LLM_OLLAMA_BASE_URL,
)
from src.config.secrets import load_api_keys
from src.config.llm_models import (
    fetch_gemini_models,
    fetch_ollama_models,
    GEMINI_FALLBACK_MODELS,
)

st.title("Configurações (modelo de linguagem)")
st.caption("Configure o provedor e o modelo para usar a aba **Consultar com IA**.")
st.markdown("---")

llm_config = load_llm_config()
provider = (llm_config.get("provider") or "gemini").strip().lower()
api_key_current = (llm_config.get("api_key") or "").strip()
# Chaves por provedor (para placeholder e mensagem "já configurada" em cada aba)
api_keys_by_provider = load_api_keys()
def _has_key(p: str) -> bool:
    return bool((api_keys_by_provider.get(p) or "").strip())
model_current = (llm_config.get("model") or "").strip()
ollama_base_current = (llm_config.get("ollama_base_url") or DEFAULT_LLM_OLLAMA_BASE_URL).strip()

PROVIDER_OPTIONS = ["gemini", "anthropic", "openai", "ollama", "generic"]
PROVIDER_LABELS = {
    "gemini": "Gemini (Google – nuvem)",
    "anthropic": "Anthropic (Claude – nuvem)",
    "openai": "OpenAI (GPT – nuvem)",
    "ollama": "Ollama (local)",
    "generic": "Outro (API compatível com OpenAI)",
}
provider_choice = st.radio(
    "Provedor",
    options=PROVIDER_OPTIONS,
    format_func=lambda x: PROVIDER_LABELS.get(x, x),
    index=PROVIDER_OPTIONS.index(provider) if provider in PROVIDER_OPTIONS else 0,
    key="config_llm_provider",
    horizontal=True,
)

api_key_to_use = ""
model_choice = model_current or DEFAULT_LLM_MODEL_GEMINI
ollama_base_to_save = llm_config.get("ollama_base_url") or ""
generic_base_url = (llm_config.get("generic_base_url") or "").strip()
generic_model = (llm_config.get("generic_model") or "").strip()
generic_base_to_save = generic_base_url
generic_model_to_save = generic_model

if provider_choice == "gemini":
    has_gemini_key = _has_key("gemini")
    api_key_input = st.text_input(
        "Chave API Gemini",
        value="",
        type="password",
        placeholder="Cole aqui a chave do Google AI Studio" if not has_gemini_key else "••••••••••••",
        key="config_llm_api_key",
        help="Obtenha em https://aistudio.google.com/apikey",
    )
    api_key_to_use = api_key_input.strip() or (api_keys_by_provider.get("gemini") or "").strip()
    try:
        gemini_models = fetch_gemini_models(api_key_to_use) if api_key_to_use else list(GEMINI_FALLBACK_MODELS)
    except Exception:
        gemini_models = list(GEMINI_FALLBACK_MODELS)
    model_default = model_current or DEFAULT_LLM_MODEL_GEMINI
    default_idx = next((i for i, (mid, _) in enumerate(gemini_models) if mid == model_default), 0)
    model_choice = st.selectbox(
        "Modelo Gemini",
        options=[mid for mid, _ in gemini_models],
        format_func=lambda mid: next((d for m, d in gemini_models if m == mid), mid),
        index=min(default_idx, len(gemini_models) - 1) if gemini_models else 0,
        key="config_llm_model",
        help="Modelos de texto (chat)",
    )
    if has_gemini_key and not api_key_input.strip():
        st.info("Uma chave API já está configurada. Digite uma nova acima para substituir e clique em Salvar.")

elif provider_choice == "anthropic":
    has_anthropic_key = _has_key("anthropic")
    api_key_input = st.text_input(
        "Chave API Anthropic",
        value="",
        type="password",
        placeholder="Cole a chave Anthropic" if not has_anthropic_key else "••••••••••••",
        key="config_llm_api_key_anthropic",
        help="Obtenha em https://console.anthropic.com/",
    )
    api_key_to_use = api_key_input.strip() or (api_keys_by_provider.get("anthropic") or "").strip()
    try:
        from src.config.llm_models import fetch_anthropic_models, ANTHROPIC_FALLBACK_MODELS
        anthropic_models = fetch_anthropic_models(api_key_to_use) if api_key_to_use else list(ANTHROPIC_FALLBACK_MODELS)
    except Exception:
        from src.config.llm_models import ANTHROPIC_FALLBACK_MODELS
        anthropic_models = list(ANTHROPIC_FALLBACK_MODELS)
    model_default = model_current or DEFAULT_LLM_MODEL_ANTHROPIC
    default_idx = next((i for i, (mid, _) in enumerate(anthropic_models) if mid == model_default), 0)
    model_choice = st.selectbox(
        "Modelo Claude",
        options=[mid for mid, _ in anthropic_models],
        format_func=lambda mid: next((d for m, d in anthropic_models if m == mid), mid),
        index=min(default_idx, len(anthropic_models) - 1) if anthropic_models else 0,
        key="config_llm_model_anthropic",
    )
    if has_anthropic_key and not api_key_input.strip():
        st.info("Uma chave API já está configurada. Digite uma nova acima para substituir e clique em Salvar.")

elif provider_choice == "openai":
    has_openai_key = _has_key("openai")
    api_key_input = st.text_input(
        "Chave API OpenAI",
        value="",
        type="password",
        placeholder="Cole a chave OpenAI" if not has_openai_key else "••••••••••••",
        key="config_llm_api_key_openai",
        help="Obtenha em https://platform.openai.com/api-keys",
    )
    api_key_to_use = api_key_input.strip() or (api_keys_by_provider.get("openai") or "").strip()
    try:
        from src.config.llm_models import fetch_openai_models, OPENAI_FALLBACK_MODELS
        openai_models = fetch_openai_models(api_key_to_use) if api_key_to_use else list(OPENAI_FALLBACK_MODELS)
    except Exception:
        from src.config.llm_models import OPENAI_FALLBACK_MODELS
        openai_models = list(OPENAI_FALLBACK_MODELS)
    model_default = model_current or DEFAULT_LLM_MODEL_OPENAI
    default_idx = next((i for i, (mid, _) in enumerate(openai_models) if mid == model_default), 0)
    model_choice = st.selectbox(
        "Modelo OpenAI",
        options=[mid for mid, _ in openai_models],
        format_func=lambda mid: next((d for m, d in openai_models if m == mid), mid),
        index=min(default_idx, len(openai_models) - 1) if openai_models else 0,
        key="config_llm_model_openai",
    )
    if has_openai_key and not api_key_input.strip():
        st.info("Uma chave API já está configurada. Digite uma nova acima para substituir e clique em Salvar.")

elif provider_choice == "generic":
    generic_base_input = st.text_input(
        "URL base da API",
        value=generic_base_url,
        placeholder="https://api.openai.com/v1",
        key="config_generic_base",
        help="URL do endpoint (ex.: LM Studio, Together, Groq, proxy). Deve ser compatível com OpenAI Chat Completions.",
    )
    generic_model_input = st.text_input(
        "Nome do modelo",
        value=generic_model or "gpt-4o",
        placeholder="gpt-4o",
        key="config_generic_model",
    )
    has_generic_key = _has_key("generic")
    api_key_input = st.text_input(
        "Chave API (opcional)",
        value="",
        type="password",
        placeholder="••••••" if has_generic_key else "Deixe vazio se a API não exigir",
        key="config_llm_api_key_generic",
    )
    api_key_to_use = api_key_input.strip() or (api_keys_by_provider.get("generic") or "").strip()
    generic_base_to_save = (generic_base_input or "").strip()
    generic_model_to_save = (generic_model_input or "").strip()
    model_choice = generic_model_to_save or "gpt-4o"

else:
    ollama_base_input = st.text_input(
        "URL do Ollama",
        value=ollama_base_current or DEFAULT_LLM_OLLAMA_BASE_URL,
        placeholder="http://localhost:11434",
        key="config_ollama_base",
        help="Deixe padrão se o Ollama está rodando na mesma máquina.",
    )
    ollama_base_to_save = (ollama_base_input or "").strip() or DEFAULT_LLM_OLLAMA_BASE_URL
    try:
        ollama_models = fetch_ollama_models(ollama_base_to_save)
    except Exception:
        ollama_models = []
    if ollama_models:
        default_ollama = model_current or (ollama_models[0][0] if ollama_models else "")
        default_idx_o = next((i for i, (m, _) in enumerate(ollama_models) if m == default_ollama), 0)
        model_choice = st.selectbox(
            "Modelo Ollama",
            options=[m for m, _ in ollama_models],
            format_func=lambda m: next((d for x, d in ollama_models if x == m), m),
            index=min(default_idx_o, len(ollama_models) - 1),
            key="config_llm_model_ollama",
        )
    else:
        model_choice = st.text_input(
            "Modelo Ollama (nome exato)",
            value=model_current or "llama3.2",
            placeholder="llama3.2",
            key="config_llm_model_ollama_txt",
            help="Ex.: llama3.2, mistral. Liste com 'ollama list' no terminal.",
        )

if st.button("Salvar configuração do modelo", key="config_save_llm"):
    save_llm_config(
        provider=provider_choice,
        api_key=api_key_to_use if provider_choice in ("gemini", "anthropic", "openai", "generic") else (llm_config.get("api_key") or ""),
        model=(model_choice or "").strip() if model_choice else "",
        ollama_base_url=ollama_base_to_save if provider_choice == "ollama" else (llm_config.get("ollama_base_url") or ""),
        generic_base_url=generic_base_to_save,
        generic_model=generic_model_to_save,
    )
    st.success("Configuração do modelo salva. Use a aba Consultar com IA para fazer perguntas.")
