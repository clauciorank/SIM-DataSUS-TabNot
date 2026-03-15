"""
Aba Documentação: exibe os MDs de docs/ com navegação por seções,
suporte a imagens locais e renderização de blocos mermaid.
"""
import re
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = PROJECT_ROOT / "docs"

DOC_SECTIONS = {
    "Guia do Usuário": [
        ("Início Rápido", "guia-usuario/inicio-rapido"),
        ("Configurações", "guia-usuario/configuracoes"),
        ("Download de Dados", "guia-usuario/download-dados"),
        ("Análise Exploratória", "guia-usuario/analise-exploratoria"),
        ("Consultar com IA", "guia-usuario/consultar-ia"),
        ("Editor SQL", "guia-usuario/editor-sql"),
        ("Previsão de Óbitos", "guia-usuario/previsao-obitos"),
    ],
    "Documentação Técnica": [
        ("Arquitetura", "tecnico/arquitetura"),
        ("Pipeline de Dados", "tecnico/pipeline-dados"),
        ("Agente de IA", "tecnico/agente-ia"),
        ("Forecasting", "tecnico/forecasting"),
        ("Editor SQL (Dicionário)", "tecnico/editor-sql"),
    ],
}

ALL_SLUGS = {slug: title for entries in DOC_SECTIONS.values() for title, slug in entries}
IMAGE_RE = re.compile(r"!\[([^\]]*)\]\((\.\./images/[^)]+|images/[^)]+)\)")
MERMAID_RE = re.compile(r"```mermaid\s*\n(.*?)```", re.DOTALL)
MD_LINK_RE = re.compile(r"\]\(([a-z0-9/_-]+\.md)\)")

MERMAID_HTML_TEMPLATE = """
<div class="mermaid">{code}</div>
<script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
<script>mermaid.initialize({{startOnLoad:true, theme:'dark'}});</script>
"""


def _resolve_image_path(match_path: str, md_dir: Path) -> Path:
    """Resolve caminho relativo de imagem a partir do diretório do MD."""
    return (md_dir / match_path).resolve()


def _rewrite_md_links(content: str, current_slug: str) -> str:
    """Reescreve links .md internos para ?doc=slug, mantendo navegação dentro do app."""
    md_dir = current_slug.rsplit("/", 1)[0] if "/" in current_slug else ""

    def _replacer(m):
        href = m.group(1)
        # Resolve caminho relativo (ex.: ../tecnico/editor-sql.md ou configuracoes.md)
        if href.startswith("../"):
            resolved = href[3:].replace(".md", "")
        elif md_dir:
            resolved = f"{md_dir}/{href.replace('.md', '')}"
        else:
            resolved = href.replace(".md", "")
        if resolved in ALL_SLUGS:
            return f"](?doc={resolved})"
        return m.group(0)

    return MD_LINK_RE.sub(_replacer, content)


def _render_md_with_assets(content: str, md_path: Path, slug: str):
    """Renderiza MD alternando st.markdown, st.image e mermaid."""
    content = _rewrite_md_links(content, slug)
    md_dir = md_path.parent

    # Extrair mermaid blocks antes de processar imagens
    parts = MERMAID_RE.split(content)
    # parts = [text, mermaid_code, text, mermaid_code, ...]

    for i, part in enumerate(parts):
        if i % 2 == 1:
            # Mermaid code block
            html = MERMAID_HTML_TEMPLATE.format(code=part.strip())
            components.html(html, height=500, scrolling=True)
        else:
            # Regular MD with possible images
            _render_text_with_images(part, md_dir)


def _render_text_with_images(text: str, md_dir: Path):
    """Renderiza texto MD alternando st.markdown e st.image para imagens locais."""
    segments = []
    last_end = 0
    for m in IMAGE_RE.finditer(text):
        if m.start() > last_end:
            segments.append(("md", text[last_end:m.start()]))
        segments.append(("image", m.group(2).strip()))
        last_end = m.end()
    if last_end < len(text):
        segments.append(("md", text[last_end:]))

    for seg_type, value in segments:
        if seg_type == "md":
            stripped = value.strip()
            if stripped:
                st.markdown(stripped, unsafe_allow_html=False)
        else:
            img_path = _resolve_image_path(value, md_dir)
            if img_path.exists():
                st.image(str(img_path), use_container_width=True)
            else:
                st.caption(f"Imagem não encontrada: {value}")


# ── Layout ──

st.title("Documentação")

# Detectar deep link via query param
doc_param = None
try:
    doc_param = st.query_params.get("doc")
except Exception:
    pass

# Sidebar: seleção de seção e documento
section_names = list(DOC_SECTIONS.keys())
default_section_idx = 0
default_doc_idx = 0

if doc_param and doc_param in ALL_SLUGS:
    for si, (sec_name, entries) in enumerate(DOC_SECTIONS.items()):
        for di, (_, slug) in enumerate(entries):
            if slug == doc_param:
                default_section_idx = si
                default_doc_idx = di
                break

with st.sidebar:
    st.subheader("Navegação")
    section = st.radio(
        "Seção",
        section_names,
        index=default_section_idx,
        key="doc_section",
    )
    entries = DOC_SECTIONS[section]
    titles = [t for t, _ in entries]
    slugs = [s for _, s in entries]

    # Se deep link aponta para esta seção, usar o índice correto
    sel_idx = default_doc_idx if section == section_names[default_section_idx] and doc_param in slugs else 0

    selected_title = st.radio(
        "Documento",
        titles,
        index=sel_idx,
        key="doc_entry",
    )
    selected_slug = slugs[titles.index(selected_title)]

# Ler e renderizar
md_path = DOCS_DIR / f"{selected_slug}.md"

if not md_path.exists():
    st.warning(f"Arquivo não encontrado: `{selected_slug}.md`")
    st.stop()

try:
    raw = md_path.read_text(encoding="utf-8")
except Exception as e:
    st.error(f"Erro ao ler o arquivo: {e}")
    st.stop()

_render_md_with_assets(raw, md_path, selected_slug)
