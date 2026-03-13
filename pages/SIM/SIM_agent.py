"""
Aba Consultar com IA: agente que responde perguntas com base nos dados oficiais (SIM).
Respostas auditáveis (query SQL exibida). Requer chave API Gemini em Configurações.
"""
import streamlit as st
from src.config.persistence import load_llm_config
from src.agent.graph import run_agent

st.set_page_config(page_title="Consultar com IA - SIM", layout="wide")
st.title("Consultar com IA")
st.caption("Perguntas sobre os dados de óbitos (SIM). As respostas são baseadas apenas nos dados oficiais e podem ser auditadas pela query exibida.")

llm_config = load_llm_config()
provider = (llm_config.get("provider") or "gemini").strip().lower()
api_key = (llm_config.get("api_key") or "").strip()

if provider == "gemini" and not api_key:
    st.warning(
        "Configure a chave de API do Gemini e o modelo em **Configurações** para usar o agente. "
        "A chave não é exibida após salvar."
    )
    st.stop()
if provider == "groq" and not api_key:
    st.warning(
        "Configure a chave de API do Groq e o modelo em **Configurações** para usar o agente. "
        "Obtenha em https://console.groq.com/keys"
    )
    st.stop()
if provider == "ollama":
    if not (llm_config.get("model") or "").strip():
        st.warning("Configure o modelo Ollama em **Configurações** (ex.: llama3.2, mistral).")
        st.stop()

# Histórico de mensagens (opcional: session_state)
if "agent_messages" not in st.session_state:
    st.session_state["agent_messages"] = []

# Input da pergunta
pergunta = st.chat_input("Digite sua pergunta sobre os dados de óbitos (ex.: Quantos óbitos em Curitiba em 2023?)")
if pergunta:
    st.session_state["agent_messages"].append({"role": "user", "content": pergunta})

# Exibir histórico
for msg in st.session_state["agent_messages"]:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg.get("sql_planejada"):
            with st.expander("Query utilizada para esta resposta"):
                st.code(msg["sql_planejada"], language="sql")
                st.caption("Você pode copiar e executar no Editor SQL para auditar.")

# Processar última pergunta do usuário (se acabou de enviar)
if pergunta:
    with st.chat_message("assistant"):
        with st.status("Consultando dados oficiais...", expanded=True):
            try:
                out = run_agent(pergunta, llm_config)
                resposta = out.get("resposta_final", "")
                sql_planejada = out.get("sql_planejada", "")
            except Exception as e:
                resposta = f"Erro ao executar o agente: {e!s}"
                sql_planejada = ""
        st.markdown(resposta)
        if sql_planejada:
            with st.expander("Query utilizada para esta resposta"):
                st.code(sql_planejada, language="sql")
                st.caption("Você pode copiar e executar no Editor SQL para auditar.")
    st.session_state["agent_messages"].append({
        "role": "assistant",
        "content": resposta,
        "sql_planejada": sql_planejada,
    })
    st.rerun()
