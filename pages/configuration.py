import streamlit as st
from datetime import datetime
from src.config.persistence import (
    load_config,
    save_config,
    load_llm_config,
    save_llm_config,
    DEFAULT_LLM_MODEL_GEMINI,
    DEFAULT_LLM_MODEL_GROQ,
    DEFAULT_LLM_OLLAMA_BASE_URL,
)
from src.config.llm_models import (
    fetch_gemini_models,
    fetch_ollama_models,
    GEMINI_FALLBACK_MODELS,
    GROQ_FALLBACK_MODELS,
)

st.title("⚙️ Configurações do Sistema")
st.markdown("---")

# Carrega da persistência (SQLite) ou usa padrões; inicializa session_state
if "filtro_anos" not in st.session_state or "filtro_ufs" not in st.session_state:
    persisted = load_config()
    st.session_state["filtro_anos"] = persisted["filtro_anos"]
    st.session_state["filtro_ufs"] = persisted["filtro_ufs"]

# Seção de Período Cronológico
st.subheader("Seleção de Período")
ano_atual = datetime.now().year
anos_disponiveis = list(range(1996, ano_atual + 1))

# Índices a partir dos parâmetros ativos
filtro_anos = st.session_state["filtro_anos"]
idx_inicio = anos_disponiveis.index(min(filtro_anos)) if filtro_anos and min(filtro_anos) in anos_disponiveis else len(anos_disponiveis) - 5
idx_fim = anos_disponiveis.index(max(filtro_anos)) if filtro_anos and max(filtro_anos) in anos_disponiveis else len(anos_disponiveis) - 1

col1, col2 = st.columns(2)

with col1:
    ano_inicio = st.selectbox(
        "Ano Inicial",
        anos_disponiveis,
        index=idx_inicio,
        key="config_ano_inicio",
    )

with col2:
    ano_fim = st.selectbox(
        "Ano Final",
        anos_disponiveis,
        index=idx_fim,
        key="config_ano_fim",
    )

# Validação simples de data
if ano_inicio > ano_fim:
    st.error("Erro: O ano inicial não pode ser maior que o ano final.")

# Seção de Localidade (Estados Brasileiros)
st.subheader("Abrangência Geográfica")

estados_br = [
    "Acre", "Alagoas", "Amapá", "Amazonas", "Bahia", "Ceará", "Distrito Federal",
    "Espírito Santo", "Goiás", "Maranhão", "Mato Grosso", "Mato Grosso do Sul",
    "Minas Gerais", "Pará", "Paraíba", "Paraná", "Pernambuco", "Piauí",
    "Rio de Janeiro", "Rio Grande do Norte", "Rio Grande do Sul", "Rondônia",
    "Roraima", "Santa Catarina", "São Paulo", "Sergipe", "Tocantins",
]

# Default do multiselect vem dos parâmetros ativos
ufs_default = [u for u in st.session_state["filtro_ufs"] if u in estados_br]
if not ufs_default:
    ufs_default = ["Paraná"]

ufs_selecionadas = st.multiselect(
    "Selecione as Unidades da Federação (UF):",
    options=estados_br,
    default=ufs_default,
    key="config_ufs",
)

st.markdown("---")

# Seção Modelo de linguagem (agente de dados)
st.subheader("Modelo de linguagem (agente de dados)")
st.caption("Configure para usar a aba **Consultar com IA**. Gemini (nuvem), Groq (Llama na nuvem) ou Ollama (local).")

llm_config = load_llm_config()
provider = (llm_config.get("provider") or "gemini").strip().lower()
api_key_current = (llm_config.get("api_key") or "").strip()
model_current = (llm_config.get("model") or "").strip()
ollama_base_current = (llm_config.get("ollama_base_url") or DEFAULT_LLM_OLLAMA_BASE_URL).strip()

provider_choice = st.radio(
    "Provedor",
    options=["gemini", "groq", "ollama"],
    format_func=lambda x: {"gemini": "Gemini (Google – nuvem)", "groq": "Groq (Llama – nuvem)", "ollama": "Ollama (local)"}.get(x, x),
    index=["gemini", "groq", "ollama"].index(provider) if provider in ("gemini", "groq", "ollama") else 0,
    key="config_llm_provider",
    horizontal=True,
)

api_key_to_use = ""
model_choice = model_current or DEFAULT_LLM_MODEL_GEMINI
ollama_base_to_save = llm_config.get("ollama_base_url") or ""

if provider_choice == "gemini":
    api_key_input = st.text_input(
        "Chave API Gemini",
        value="",
        type="password",
        placeholder="Cole aqui a chave do Google AI Studio" if not api_key_current else "••••••••••••",
        key="config_llm_api_key",
        help="Obtenha em https://aistudio.google.com/apikey",
    )
    # Só reutilizar chave salva se o provedor já era Gemini
    api_key_to_use = api_key_input.strip() or (api_key_current if provider == "gemini" else "")
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
        help="Gemini 3.1 Flash Lite oferece a melhor quota no free tier.",
    )
    if api_key_current and not api_key_input.strip():
        st.info("Uma chave API já está configurada. Digite uma nova acima para substituir e clique em Salvar.")
elif provider_choice == "groq":
    api_key_input = st.text_input(
        "Chave API Groq",
        value="",
        type="password",
        placeholder="Cole a chave do Groq (obrigatório ao usar Groq)" if provider != "groq" else ("••••••••" if api_key_current else "Cole a chave do Groq"),
        key="config_llm_api_key_groq",
        help="Obtenha em https://console.groq.com/keys — ao trocar de Gemini para Groq, cole a chave Groq aqui.",
    )
    # Só reutilizar chave salva se o provedor já era Groq (evita usar chave Gemini como Groq)
    api_key_to_use = api_key_input.strip() or (api_key_current if provider == "groq" else "")
    if provider_choice == "groq" and not api_key_to_use:
        st.warning("Cole a chave API do Groq acima e clique em Salvar. A chave do Gemini não funciona no Groq.")
    model_default = model_current or DEFAULT_LLM_MODEL_GROQ
    default_idx_g = next((i for i, (mid, _) in enumerate(GROQ_FALLBACK_MODELS) if mid == model_default), 0)
    model_choice = st.selectbox(
        "Modelo Groq",
        options=[mid for mid, _ in GROQ_FALLBACK_MODELS],
        format_func=lambda mid: next((d for m, d in GROQ_FALLBACK_MODELS if m == mid), mid),
        index=min(default_idx_g, len(GROQ_FALLBACK_MODELS) - 1) if GROQ_FALLBACK_MODELS else 0,
        key="config_llm_model_groq",
        help="Llama 3.3 70B Versatile é o modelo padrão recomendado.",
    )
    if api_key_current and not api_key_input.strip():
        st.info("Uma chave API já está configurada. Digite uma nova acima para substituir e clique em Salvar.")
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
            help="Ex.: llama3.2, mistral. Liste com 'ollama list' no terminal. Se o Ollama não estiver rodando, digite o nome do modelo.",
        )

if st.button("Salvar configuração do modelo", key="config_save_llm"):
    save_llm_config(
        api_key=api_key_to_use if provider_choice in ("gemini", "groq") else (llm_config.get("api_key") or ""),
        provider=provider_choice,
        model=(model_choice or "").strip() if model_choice else "",
        ollama_base_url=ollama_base_to_save if provider_choice == "ollama" else (llm_config.get("ollama_base_url") or ""),
    )
    st.success("Configuração do modelo salva. Use a aba Consultar com IA para fazer perguntas.")

st.markdown("---")

# Botão de ação para salvar ou disparar o pipeline
if st.button("Aplicar Configurações", type="primary"):
    filtro_anos = list(range(ano_inicio, ano_fim + 1))
    filtro_ufs = ufs_selecionadas

    st.session_state["filtro_anos"] = filtro_anos
    st.session_state["filtro_ufs"] = filtro_ufs
    save_config(filtro_anos, filtro_ufs)

    st.success(
        f"Configurações aplicadas e salvas para {len(filtro_ufs)} estado(s) "
        f"entre {ano_inicio}-{ano_fim}!"
    )

# Visualização dos parâmetros atuais (para debug ou conferência)
with st.expander("Visualizar Parâmetros Ativos"):
    st.json({
        "Anos": list(range(ano_inicio, ano_fim + 1)),
        "Estados": ufs_selecionadas
    })