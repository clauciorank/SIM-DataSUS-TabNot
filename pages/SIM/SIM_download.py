import shutil
import streamlit as st
from pathlib import Path
from datetime import datetime

import pandas as pd

# Lista de estados para o multiselect (mesmo que em configuration antes)
ESTADOS_BR = [
    "Acre", "Alagoas", "Amapá", "Amazonas", "Bahia", "Ceará", "Distrito Federal",
    "Espírito Santo", "Goiás", "Maranhão", "Mato Grosso", "Mato Grosso do Sul",
    "Minas Gerais", "Pará", "Paraíba", "Paraná", "Pernambuco", "Piauí",
    "Rio de Janeiro", "Rio Grande do Norte", "Rio Grande do Sul", "Rondônia",
    "Roraima", "Santa Catarina", "São Paulo", "Sergipe", "Tocantins",
]

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


st.title("SIM - Download de Dados")
st.caption("Escolha o período e os estados. Depois baixe e processe os dados.")
st.markdown("---")

# Sincroniza sessão com configuração salva
_sync_config_from_persistence()

# Step 1 – Escolha o que baixar
st.subheader("Step 1 – Escolha o que baixar")
ano_atual = datetime.now().year
anos_disponiveis = list(range(1996, ano_atual + 1))
anos = st.session_state.get("filtro_anos") or [ano_atual - 5, ano_atual]
ufs = st.session_state.get("filtro_ufs") or ["Paraná"]

idx_inicio = anos_disponiveis.index(min(anos)) if anos and min(anos) in anos_disponiveis else max(0, len(anos_disponiveis) - 5)
idx_fim = anos_disponiveis.index(max(anos)) if anos and max(anos) in anos_disponiveis else len(anos_disponiveis) - 1

c1, c2 = st.columns(2)
with c1:
    ano_inicio = st.selectbox("Ano inicial", anos_disponiveis, index=idx_inicio, key="dl_ano_inicio")
with c2:
    ano_fim = st.selectbox("Ano final", anos_disponiveis, index=idx_fim, key="dl_ano_fim")

ufs_default = [u for u in ufs if u in ESTADOS_BR]
if not ufs_default:
    ufs_default = ["Paraná"]
ufs_selecionadas = st.multiselect(
    "Unidades da Federação (UF)",
    options=ESTADOS_BR,
    default=ufs_default,
    key="dl_ufs",
)

if ano_inicio > ano_fim:
    st.error("O ano inicial não pode ser maior que o ano final.")
else:
    st.info(
        f"Você vai baixar dados de **{len(ufs_selecionadas)}** estado(s) entre **{ano_inicio}** e **{ano_fim}**."
    )

if st.button("Aplicar período e estados", type="primary", key="dl_apply"):
    from src.config.persistence import save_config
    filtro_anos = list(range(ano_inicio, ano_fim + 1))
    filtro_ufs = ufs_selecionadas or ["Paraná"]
    st.session_state["filtro_anos"] = filtro_anos
    st.session_state["filtro_ufs"] = filtro_ufs
    save_config(filtro_anos, filtro_ufs)
    st.success(f"Período e estados salvos: {len(filtro_ufs)} estado(s), {ano_inicio}–{ano_fim}.")
    st.rerun()

# Atualiza sessão para os blocos abaixo (caso tenha aplicado antes)
anos = st.session_state.get("filtro_anos") or list(range(ano_inicio, ano_fim + 1))
ufs = st.session_state.get("filtro_ufs") or ufs_selecionadas or ["Paraná"]

st.markdown("---")

# Step 2 – Verificar / Baixar

st.subheader("Step 2 – Verificar e baixar")

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

if st.session_state.get("download_result_message"):
    st.success(st.session_state.pop("download_result_message"))
if st.session_state.get("download_result_error"):
    st.error(st.session_state.pop("download_result_error"))

if st.session_state.get("pending_download"):
    st.session_state.pop("pending_download")
    forcar_todos = st.session_state.get("pending_download_force_all", False)
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
            st.session_state["download_result_message"] = "Nenhum arquivo foi baixado. Todos já estão atualizados."
        else:
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

            progress_bar.progress(0.7, text="Construindo camada gold...")
            status_text.caption("Gerando view única (v_obitos_completo)...")

            from src.data_extraction.gold_catalog import build_gold_catalog
            build_gold_catalog()

            progress_bar.progress(1.0, text="Concluído.")
            status_text.caption("Download, silver e gold atualizados.")
            progress_bar.empty()
            status_text.empty()

            st.session_state["download_result_message"] = (
                f"**{len(baixados)}** arquivo(s) baixado(s). Silver: **{result['total_registros']:,}** registros. "
                "Gold atualizada (`v_obitos_completo`)."
            )
        st.session_state.pop("long_operation_in_progress", None)
        st.session_state.pop("long_operation_page", None)
        st.session_state.pop("pending_download_force_all", None)
        st.rerun()
    except Exception as e:
        try:
            progress_bar.empty()
            status_text.empty()
        except NameError:
            pass
        st.session_state["download_result_error"] = str(e)
        st.session_state.pop("long_operation_in_progress", None)
        st.session_state.pop("long_operation_page", None)
        st.session_state.pop("pending_download_force_all", None)
        st.rerun()
else:
    col_dl1, col_dl2 = st.columns(2)
    with col_dl1:
        baixar_atualizados = st.button("Baixar apenas desatualizados/faltando", type="primary")
    with col_dl2:
        forcar_todos = st.button("Forçar download de todos os arquivos")

    if baixar_atualizados or forcar_todos:
        st.session_state["long_operation_in_progress"] = True
        st.session_state["long_operation_page"] = "download"
        st.session_state["pending_download"] = True
        st.session_state["pending_download_force_all"] = forcar_todos
        st.rerun()

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
