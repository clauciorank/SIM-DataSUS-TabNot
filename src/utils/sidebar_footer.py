"""
Rodapé da barra lateral: ícones oficiais do GitHub e LinkedIn, clicáveis, sem texto.
Deve ser chamado ao final de cada página para aparecer na parte inferior da sidebar.
"""
import streamlit as st

# TODO: Inserir URL do repositório GitHub (ex.: https://github.com/seu-usuario/DatasusBrasileiroApp)
GITHUB_URL = "https://github.com/seu-usuario/DatasusBrasileiroApp"
# TODO: Inserir URL do perfil LinkedIn (ex.: https://www.linkedin.com/in/seu-perfil)
LINKEDIN_URL = "https://www.linkedin.com/in/seu-perfil"

# Ícones oficiais (Simple Icons CDN — SVGs oficiais dos brands)
GITHUB_ICON = "https://cdn.simpleicons.org/github"
LINKEDIN_ICON = "https://cdn.simpleicons.org/linkedin"


def render_sidebar_footer():
    """Renderiza na barra lateral, na parte inferior, ícones clicáveis do GitHub e LinkedIn."""
    html = f"""
    <div style="margin-top: 2rem; padding-top: 1rem; border-top: 1px solid rgba(49, 51, 63, 0.2);">
        <a href="{GITHUB_URL}" target="_blank" rel="noopener noreferrer" title="Documentação no GitHub" style="margin-right: 12px;">
            <img src="{GITHUB_ICON}" alt="GitHub" width="28" height="28" style="vertical-align: middle; opacity: 0.9;">
        </a>
        <a href="{LINKEDIN_URL}" target="_blank" rel="noopener noreferrer" title="LinkedIn" style="margin-right: 12px;">
            <img src="{LINKEDIN_ICON}" alt="LinkedIn" width="28" height="28" style="vertical-align: middle; opacity: 0.9;">
        </a>
    </div>
    """
    st.sidebar.markdown(html, unsafe_allow_html=True)
