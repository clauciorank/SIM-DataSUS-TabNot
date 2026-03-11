import streamlit as st
from datetime import datetime
from src.config.persistence import load_config, save_config

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