"""
Rodapé da barra lateral: ícone oficial do GitHub, clicável, sem texto.
"""
import streamlit as st

# TODO: Inserir URL do repositório GitHub (ex.: https://github.com/seu-usuario/DatasusBrasileiroApp)
GITHUB_URL = "https://github.com/seu-usuario/DatasusBrasileiroApp"
GITHUB_ICON = "https://cdn.simpleicons.org/github"


def render_sidebar_footer():
    """Renderiza na barra lateral, na parte inferior, ícone clicável do GitHub."""
    html = f"""
    <div style="margin-top: 2rem; padding-top: 1rem; border-top: 1px solid rgba(49, 51, 63, 0.2);">
        <a href="{GITHUB_URL}" target="_blank" rel="noopener noreferrer" title="Repositório no GitHub">
            <img src="{GITHUB_ICON}" alt="GitHub" width="28" height="28" style="vertical-align: middle; opacity: 0.9;">
        </a>
    </div>
    """
    st.sidebar.markdown(html, unsafe_allow_html=True)
