import logging
import sys
import streamlit as st

# Logs do agente (tokens, chamadas LLM) no terminal
_agent_logger = logging.getLogger("src.agent")
_agent_logger.setLevel(logging.INFO)
if not _agent_logger.handlers:
    _h = logging.StreamHandler(sys.stderr)
    _h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s", datefmt="%H:%M:%S"))
    _agent_logger.addHandler(_h)

# 1. Definição das páginas
# O primeiro argumento é o caminho do arquivo, o segundo é o título exibido
configuration = st.Page("pages/configuration.py", title="Configurações", icon="⚙️")

pg_sim_download = st.Page("pages/SIM/SIM_download.py", title="Download de Dados", icon="⬇️")
pg_sim_analise = st.Page("pages/SIM/SIM_analise.py", title="Análise Exploratória", icon="📊")
pg_sim_sql = st.Page("pages/SIM/SIM_sql.py", title="Editor SQL", icon="📝")
pg_sim_forecast = st.Page("pages/SIM/SIM_forecast.py", title="Previsão do número de mortes", icon="📈")
pg_sim_agent = st.Page("pages/SIM/SIM_agent.py", title="Consultar com IA", icon="💬")

# 2. Agrupamento em "Abas" (Seções) na barra lateral — restringe SIM durante download/construção
_in_progress = st.session_state.get("long_operation_in_progress")
_page = st.session_state.get("long_operation_page")
if _in_progress and _page == "download":
    sim_pages = [pg_sim_download]
else:
    sim_pages = [pg_sim_download, pg_sim_analise, pg_sim_agent, pg_sim_sql, pg_sim_forecast]
navegacao = st.navigation({
    "Configurações": [configuration],
    "SIM": sim_pages,
})

# 3. Execução
st.set_page_config(page_title="SIM DataSUS TabNot", layout="wide", page_icon = '⚕️')
navegacao.run()
