import shutil
import streamlit as st
from pathlib import Path

import pandas as pd

# Mapeamento estado (nome completo) -> sigla UF para o FTP
UF_POR_NOME = {
    "Acre": "AC", "Alagoas": "AL", "Amapá": "AP", "Amazonas": "AM",
    "Bahia": "BA", "Ceará": "CE", "Distrito Federal": "DF",
    "Espírito Santo": "ES", "Goiás": "GO", "Maranhão": "MA",
    "Mato Grosso": "MT", "Mato Grosso do Sul": "MS",
    "Minas Gerais": "MG", "Pará": "PA", "Paraíba": "PB",
    "Paraná": "PR", "Pernambuco": "PE", "Piauí": "PI",
    "Rio de Janeiro": "RJ", "Rio Grande do Norte": "RN",
    "Rio Grande do Sul": "RS", "Rondônia": "RO", "Roraima": "RR",
    "Santa Catarina": "SC", "São Paulo": "SP", "Sergipe": "SE",
    "Tocantins": "TO",
}

# Diretórios de dados SIM
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_PATH = PROJECT_ROOT / "data" / "SIM" / "raw"
SILVER_PATH = PROJECT_ROOT / "data" / "SIM" / "silver"
GOLD_PATH = PROJECT_ROOT / "data" / "SIM" / "gold"


def _limpar_diretorio(path: Path) -> int:
    """Remove todos os arquivos e subpastas dentro de path. Retorna quantidade removida."""
    if not path.exists() or not path.is_dir():
        return 0
    count = 0
    for child in list(path.iterdir()):
        if child.is_file():
            child.unlink()
            count += 1
        elif child.is_dir():
            shutil.rmtree(child)
            count += 1
    return count


def limpar_todas_camadas() -> dict:
    """Remove todo o conteúdo de raw, silver e gold. Retorna dict com contagem por camada."""
    return {
        "raw": _limpar_diretorio(DATA_PATH),
        "silver": _limpar_diretorio(SILVER_PATH),
        "gold": _limpar_diretorio(GOLD_PATH),
    }


def _sync_config_from_persistence():
    """Garante que filtro_anos e filtro_ufs na sessão vêm da persistência quando ainda não foram definidos."""
    if "filtro_anos" not in st.session_state or "filtro_ufs" not in st.session_state:
        from src.config.persistence import load_config
        persisted = load_config()
        st.session_state["filtro_anos"] = persisted["filtro_anos"]
        st.session_state["filtro_ufs"] = persisted["filtro_ufs"]


def get_ftp_client():
    """Carrega cliente FTP do SIM e usa config da sessão (sincronizada com persistência)."""
    from pysus import SIM
    from src.data_extraction.FTPGeneral import DownloadFileGeneral

    anos = st.session_state["filtro_anos"]
    ufs_nomes = st.session_state["filtro_ufs"]
    ufs_siglas = [UF_POR_NOME.get(u, u) for u in ufs_nomes if u in UF_POR_NOME]
    if not ufs_siglas:
        ufs_siglas = ["PR"]

    start_year = min(anos) if anos else 1996
    end_year = max(anos) if anos else 2024

    sim = SIM().load()
    return DownloadFileGeneral(
        states=ufs_siglas,
        start_year=start_year,
        end_year=end_year,
        system=sim,
        file_path=str(DATA_PATH),
    )


st.title("⬇️ SIM - Download de Dados")
st.markdown(
    "Baixe e atualize os dados de mortalidade (SIM) do Datasus conforme os filtros "
    "configurados na aba **Configurações**."
)
st.markdown("---")

# Sincroniza sessão com configuração salva (evita exibir valores desatualizados)
_sync_config_from_persistence()

# Parâmetros atuais (sempre refletem o que está salvo ou aplicado na sessão)
col1, col2 = st.columns(2)
with col1:
    anos = st.session_state["filtro_anos"]
    st.info(f"**Período:** {min(anos) if anos else '-'} a {max(anos) if anos else '-'}")
with col2:
    ufs = st.session_state["filtro_ufs"]
    st.info(f"**Estados:** {', '.join(ufs) if ufs else '-'}")

if not anos or not ufs:
    st.warning(
        "Configure os anos e estados na aba **Configurações** e clique em "
        "\"Aplicar Configurações\" antes de usar esta página."
    )

st.markdown("---")

# Seção: Verificar atualizações
st.subheader("🔍 Verificar atualizações")

if st.button("Verificar se há arquivos desatualizados ou faltando", type="secondary"):
    with st.spinner("Consultando FTP do Datasus..."):
        try:
            ftp = get_ftp_client()
            to_download = ftp.verify_if_need_download()
            if not to_download:
                st.success("Todos os arquivos estão atualizados.")
            else:
                st.warning(f"Existem **{len(to_download)}** arquivo(s) para baixar ou atualizar.")
                desc_list = [ftp.system.describe(f) for f in to_download]
                df_dl = pd.DataFrame(desc_list)
                st.dataframe(df_dl, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"Erro ao verificar: {e}")

st.markdown("---")

# Seção: Download
st.subheader("📥 Baixar dados")

col_dl1, col_dl2 = st.columns(2)
with col_dl1:
    baixar_atualizados = st.button("Baixar apenas desatualizados/faltando", type="primary")
with col_dl2:
    forcar_todos = st.button("Forçar download de todos os arquivos")

if baixar_atualizados or forcar_todos:
    try:
        ftp = get_ftp_client()
        progress_bar = st.progress(0, text="Iniciando download...")
        status_text = st.empty()

        def update_progress(atual: int, total: int, nome: str):
            progress_bar.progress(atual / total, text=f"Baixando {nome} ({atual}/{total})")
            status_text.caption(f"Arquivo {atual} de {total}: {nome}")

        baixados = ftp.download_files(
            force_all=forcar_todos,
            progress_callback=update_progress,
        )

        if not baixados:
            progress_bar.empty()
            status_text.empty()
            st.info("Nenhum arquivo foi baixado. Todos já estão atualizados.")
        else:
            # Fase 2: Silver (barra 0.2 → 0.6)
            progress_bar.progress(0.2, text="Processando camada silver...")
            status_text.caption("Carregando dados raw...")

            from src.data_extraction.SIMProcessor import SIMProcessor
            silver_path = str(PROJECT_ROOT / "data" / "SIM" / "silver")
            processor = SIMProcessor(raw_path=str(DATA_PATH), silver_path=silver_path)

            def silver_status(msg: str):
                status_text.caption(msg)
                if "raw" in msg.lower():
                    progress_bar.progress(0.3, text="Processando camada silver...")
                elif "tratamento" in msg.lower():
                    progress_bar.progress(0.4, text="Processando camada silver...")
                elif "duckdb" in msg.lower() or "consumo" in msg.lower():
                    progress_bar.progress(0.6, text="Processando camada silver...")

            result = processor.process(progress_callback=silver_status)

            # Fase 3: Gold (barra 0.6 → 1.0)
            progress_bar.progress(0.7, text="Construindo camada gold...")
            status_text.caption("Gerando view única (v_obitos_completo)...")

            from src.data_extraction.gold_catalog import build_gold_catalog
            build_gold_catalog()

            progress_bar.progress(1.0, text="Concluído.")
            status_text.caption("Download, silver e gold atualizados.")
            progress_bar.empty()
            status_text.empty()

            st.success(
                f"**{len(baixados)}** arquivo(s) baixado(s). Silver: **{result['total_registros']:,}** registros. "
                "Gold atualizada (`v_obitos_completo`)."
            )
            for p in baixados:
                st.caption(f"`{p}`")
    except Exception as e:
        try:
            progress_bar.empty()
            status_text.empty()
        except NameError:
            pass
        st.error(f"Erro: {e}")

# Seção: Limpar dados
st.subheader("🗑️ Limpar dados")
st.caption("Remove todos os arquivos das camadas raw, silver e gold.")

@st.dialog("Confirmar exclusão")
def confirmar_limpeza():
    st.warning(
        "Isso vai apagar **permanentemente** todos os dados das camadas **raw**, **silver** e **gold**. "
        "Esta ação não pode ser desfeita."
    )
    st.caption("Deseja continuar?")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Cancelar"):
            st.session_state.pop("mostrar_dialog_limpeza", None)
            st.rerun()
    with col2:
        if st.button("Sim, limpar tudo", type="primary"):
            resultado = limpar_todas_camadas()
            st.session_state["limpeza_resultado"] = resultado
            st.session_state.pop("mostrar_dialog_limpeza", None)
            st.rerun()


if st.button("Limpar todos os arquivos", type="secondary"):
    st.session_state["mostrar_dialog_limpeza"] = True

if st.session_state.get("mostrar_dialog_limpeza"):
    confirmar_limpeza()

if st.session_state.get("limpeza_resultado"):
    r = st.session_state.pop("limpeza_resultado")
    st.success(
        f"Limpeza concluída: **raw** ({r['raw']} itens), **silver** ({r['silver']} itens), **gold** ({r['gold']} itens) removidos."
    )

st.markdown("---")

# Seção: Arquivos locais
st.subheader("📁 Arquivos no disco local")
st.caption(f"Diretório: `{DATA_PATH}`")

try:
    ftp = get_ftp_client()
    df_local = ftp.list_local_files()
    if df_local.empty:
        st.info("Nenhum arquivo local encontrado.")
    else:
        df_local["size"] = (df_local["size"] / 1024 / 1024).round(2).astype(str) + " MB"
        st.dataframe(
            df_local[["name", "size", "modified"]],
            use_container_width=True,
            hide_index=True,
        )
except Exception as e:
    st.error(f"Erro ao listar arquivos: {e}")
