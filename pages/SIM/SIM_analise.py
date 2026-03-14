"""
Análise exploratória dos dados de óbitos (SIM).
Otimizado: view v_obitos_completo na gold (idade_anos, faixa_etaria); filtros e gráficos via SQL (sem carregar tudo em memória).
"""
import re
import sys
from pathlib import Path
_pages_sim = Path(__file__).resolve().parent
if str(_pages_sim) not in sys.path:
    sys.path.insert(0, str(_pages_sim))

from concurrent.futures import ThreadPoolExecutor
import streamlit as st
import pandas as pd
import duckdb
import plotly.graph_objects as go

from sim_filters import (
    GOLD_DB,
    VIEW_ANALISE,
    FAIXAS_ETARIAS,
    UFS_ORDEM,
    MAX_SELECT_MUNICIPIO,
    MAX_SELECT_CAUSA,
    _get_con,
    _to_date,
    _view_exists,
    get_bounds_dt_obito,
    _build_where_and_params,
    _effective_sel_for_where,
    _selection_for_where,
    _opts_sexo_silver,
    _opts_uf_silver,
    _opts_municipio_silver,
    _opts_capitulos_silver,
    _opts_causas_silver,
)

SK_APPLIED = "analise_sim_filtros_aplicados"
SK_COUNT = "analise_sim_count"
SK_TS = "analise_sim_ts"
SK_PYRAMID = "analise_sim_pyramid"
SK_SAMPLE = "analise_sim_sample"
SK_TS_GRANULARITY = "analise_sim_ts_granularidade"
SK_WHERE = "analise_sim_where"
SK_PARAMS = "analise_sim_params"
# Cache de opções (evitar chamadas à silver em todo run)
SK_CACHE_MIN_DATE = "analise_sim_cache_min_date"
SK_CACHE_MAX_DATE = "analise_sim_cache_max_date"
SK_CACHE_OPCOES_SEXO = "analise_sim_cache_opcoes_sexo"
SK_CACHE_UF_LIST = "analise_sim_cache_uf_list"
SK_CACHE_CAP_LIST = "analise_sim_cache_cap_list"
SK_CACHE_MUN_LIST = "analise_sim_cache_mun_list"
SK_CACHE_MUN_UF_KEY = "analise_sim_cache_mun_uf_key"
SK_CACHE_CAUSAS_LABELS = "analise_sim_cache_causas_labels"
SK_CACHE_CAUSAS_CAP_KEY = "analise_sim_cache_causas_cap_key"


def _run_count(where: str, params: list):
    """Executa count em conexão própria (para uso em thread)."""
    con = _get_con()
    try:
        return con.execute(f"SELECT count(*) FROM {VIEW_ANALISE} WHERE {where}", params).fetchone()[0]
    finally:
        con.close()


def _run_ts(where: str, params: list, ts_fmt: str):
    """Executa query da série temporal em conexão própria (para uso em thread)."""
    con = _get_con()
    try:
        if ts_fmt == "%Y-%m":
            sql = f"SELECT strftime(dt_obito_mes, '%Y-%m') AS periodo, count(*) AS obitos FROM {VIEW_ANALISE} WHERE {where} GROUP BY dt_obito_mes ORDER BY 1"
        else:
            sql = f"SELECT ano AS periodo, count(*) AS obitos FROM {VIEW_ANALISE} WHERE {where} GROUP BY ano ORDER BY 1"
        return con.execute(sql, params).fetchdf()
    finally:
        con.close()


def _run_pyr(where: str, params: list):
    """Executa query da pirâmide em conexão própria (para uso em thread)."""
    con = _get_con()
    try:
        return con.execute(
            f"SELECT faixa_etaria, sexo_desc, count(*) AS total FROM {VIEW_ANALISE} WHERE {where} GROUP BY 1, 2",
            params,
        ).fetchdf()
    finally:
        con.close()


def _run_sample(where: str, params: list):
    """Executa query da amostra em conexão própria (para uso em thread)."""
    con = _get_con()
    try:
        return con.execute(f"SELECT * FROM {VIEW_ANALISE} WHERE {where} LIMIT 200", params).fetchdf()
    finally:
        con.close()


def _normalize_todos_multiselect(key: str, all_options: list, sentinel: str):
    """
    Retorna (opcoes_para_widget, valor_para_default) para o multiselect.
    Não escreve em session_state (evita conflito com default= do widget).
    """
    val = st.session_state.get(key, [sentinel])
    if val == []:
        val = [sentinel]
    # Para opções: se já tem só itens concretos, mostrar lista sem sentinela
    opts = [o for o in all_options if o != sentinel] if (val and sentinel not in val) else all_options
    # Para default: se tem sentinela + outros, considerar só os outros (só para exibição do default)
    default_val = [x for x in val if x != sentinel] if (sentinel in val and len(val) > 1) else val
    return opts, default_val


st.set_page_config(page_title="Análise exploratória - SIM", layout="wide")
st.title("📊 Análise exploratória - Óbitos (SIM)")

if not GOLD_DB.exists():
    st.warning("A camada gold não foi encontrada. Faça o download e processe em Download de Dados.")
    st.stop()

# Abrir conexão só quando for preciso (caches, cascata ou aplicar)
con = None


def _ensure_static_caches(conn):
    """Preenche cache de min/max, sexo, UF e capítulos se ainda não existir."""
    if SK_CACHE_MIN_DATE not in st.session_state:
        min_date, max_date = get_bounds_dt_obito(conn)
        st.session_state[SK_CACHE_MIN_DATE] = min_date
        st.session_state[SK_CACHE_MAX_DATE] = max_date
    if SK_CACHE_OPCOES_SEXO not in st.session_state:
        st.session_state[SK_CACHE_OPCOES_SEXO] = ["Todos"] + _opts_sexo_silver(conn)
    if SK_CACHE_UF_LIST not in st.session_state:
        st.session_state[SK_CACHE_UF_LIST] = _opts_uf_silver(conn)
    if SK_CACHE_CAP_LIST not in st.session_state:
        st.session_state[SK_CACHE_CAP_LIST] = _opts_capitulos_silver(conn)


# Primeira vez: checar view e preencher caches estáticos
need_static = SK_CACHE_UF_LIST not in st.session_state
if need_static:
    con = _get_con()
    if not _view_exists(con):
        con.close()
        st.warning(
            "A view **v_obitos_completo** não existe. Reconstrua a camada gold (faça um novo download ou "
            "reprocesse os dados) para ativar a análise otimizada."
        )
        st.stop()
    _ensure_static_caches(con)

default_d1 = _to_date(st.session_state.get(SK_CACHE_MIN_DATE)) if SK_CACHE_MIN_DATE in st.session_state else None
default_d2 = _to_date(st.session_state.get(SK_CACHE_MAX_DATE)) if SK_CACHE_MAX_DATE in st.session_state else None
opcoes_sexo = st.session_state.get(SK_CACHE_OPCOES_SEXO, ["Todos"])
uf_list = st.session_state.get(SK_CACHE_UF_LIST, [])
cap_list = st.session_state.get(SK_CACHE_CAP_LIST, [])

# Cascata: chaves para cache de município e causas
uf_sel_raw = st.session_state.get("analise_uf", uf_list if uf_list else [])
uf_para_cascade = [u for u in uf_sel_raw if u != "Todas"] if uf_sel_raw else []
mun_uf_key = tuple(sorted(uf_para_cascade))

cap_sel_raw = st.session_state.get("analise_cap", cap_list if cap_list else [])
cap_para_cascade = [c for c in cap_sel_raw if c != "Todos"] if cap_sel_raw else []
causas_cap_key = tuple(sorted(cap_para_cascade))

need_mun = st.session_state.get(SK_CACHE_MUN_UF_KEY) != mun_uf_key
need_causas = st.session_state.get(SK_CACHE_CAUSAS_CAP_KEY) != causas_cap_key
if need_mun or need_causas:
    if con is None:
        con = _get_con()
    if need_mun:
        mun_list = _opts_municipio_silver(con, list(uf_para_cascade))
        st.session_state[SK_CACHE_MUN_LIST] = mun_list
        st.session_state[SK_CACHE_MUN_UF_KEY] = mun_uf_key
    else:
        mun_list = st.session_state.get(SK_CACHE_MUN_LIST, [])
    if need_causas:
        causas_labels = _opts_causas_silver(con, list(cap_para_cascade))
        st.session_state[SK_CACHE_CAUSAS_LABELS] = causas_labels
        st.session_state[SK_CACHE_CAUSAS_CAP_KEY] = causas_cap_key
        st.session_state.pop("analise_causa", None)  # deixa o widget definir o valor
    else:
        causas_labels = st.session_state.get(SK_CACHE_CAUSAS_LABELS, [])
else:
    mun_list = st.session_state.get(SK_CACHE_MUN_LIST, [])
    causas_labels = st.session_state.get(SK_CACHE_CAUSAS_LABELS, [])

if con is not None:
    con.close()
    con = None

default_uf = uf_sel_raw if uf_sel_raw else (uf_list or [])
default_cap = cap_sel_raw if cap_sel_raw else (cap_list or [])

# ---- Filtros na aba principal ----
st.subheader("🔽 Filtros")

c1, c2, c3 = st.columns(3)
with c1:
    d1 = default_d1
    d2 = default_d2
    if default_d1 and default_d2:
        d1 = st.date_input(
            "Data início",
            value=default_d1,
            min_value=default_d1,
            max_value=default_d2,
            key="analise_date_inicio",
        )
        d2 = st.date_input(
            "Data fim",
            value=default_d2,
            min_value=default_d1,
            max_value=default_d2,
            key="analise_date_fim",
        )
        if d1 and d2 and d1 > d2:
            d1, d2 = d2, d1
with c2:
    opts_sexo, default_sexo = _normalize_todos_multiselect("analise_sexo", opcoes_sexo, "Todos")
    sexo_sel = st.multiselect("Sexo", opts_sexo, default=default_sexo, key="analise_sexo")
with c3:
    all_faixas = ["Todas"] + FAIXAS_ETARIAS
    opts_faixa, default_faixa = _normalize_todos_multiselect("analise_faixa", all_faixas, "Todas")
    faixa_sel = st.multiselect("Faixa etária", opts_faixa, default=default_faixa, key="analise_faixa")

r2a, r2b = st.columns(2)
with r2a:
    uf_sel = st.multiselect("UF (residência)", uf_list, default=default_uf, key="analise_uf")
with r2b:
    all_muns = ["Todos"] + mun_list
    opts_mun, default_mun = _normalize_todos_multiselect("analise_mun", all_muns, "Todos")
    mun_sel = st.multiselect(
        "Município (residência)",
        opts_mun,
        default=default_mun,
        key="analise_mun",
        max_selections=MAX_SELECT_MUNICIPIO,
        help=f"Máximo {MAX_SELECT_MUNICIPIO} municípios para evitar travamento.",
    )

cap_sel = st.multiselect("Capítulo CID-10 (causa)", cap_list, default=default_cap, key="analise_cap")

causa_sel = st.multiselect(
    "Causa do óbito (código - descrição)",
    causas_labels,
    default=st.session_state.get("analise_causa") or [],
    key="analise_causa",
    max_selections=MAX_SELECT_CAUSA,
    help=f"Selecione até {MAX_SELECT_CAUSA} causas. Use o capítulo CID-10 acima para filtrar a lista. Máximo para evitar travamento.",
)

st.markdown("")
aplicar = st.button("Filtrar", type="primary", use_container_width=True)

st.markdown("---")

if aplicar:
    sexo_eff = _selection_for_where(sexo_sel, [o for o in opcoes_sexo if o != "Todos"], "Todos")
    faixa_eff = _selection_for_where(faixa_sel, FAIXAS_ETARIAS, "Todas")
    uf_eff = _effective_sel_for_where(uf_sel, uf_list, "Todas")
    mun_eff = _selection_for_where(mun_sel, mun_list, "Todos")
    cap_eff = _effective_sel_for_where(cap_sel, cap_list, "Todos")
    where, params = _build_where_and_params(
        d1, d2, sexo_eff, faixa_eff, uf_eff, mun_eff, cap_eff, causa_sel, ["Todas"], ["Todos"]
    )
    ts_fmt = "%Y" if st.session_state.get("analise_ts_granularity", "Anos") == "Anos" else "%Y-%m"
    st.session_state[SK_TS_GRANULARITY] = st.session_state.get("analise_ts_granularity", "Anos")
    with ThreadPoolExecutor(max_workers=4) as executor:
        f_count = executor.submit(_run_count, where, params)
        f_ts = executor.submit(_run_ts, where, params, ts_fmt)
        f_pyr = executor.submit(_run_pyr, where, params)
        f_sample = executor.submit(_run_sample, where, params)
        n = f_count.result()
        ts_df = f_ts.result()
        pyr_df = f_pyr.result()
        sample_df = f_sample.result()
    st.session_state[SK_APPLIED] = True
    st.session_state[SK_COUNT] = n
    st.session_state[SK_TS] = ts_df
    st.session_state[SK_PYRAMID] = pyr_df
    st.session_state[SK_SAMPLE] = sample_df
    st.session_state[SK_WHERE] = where
    st.session_state[SK_PARAMS] = params
    st.rerun()

if not st.session_state.get(SK_APPLIED, False):
    st.info("Ajuste os filtros e clique em **Filtrar** para ver os gráficos.")
    st.stop()

n = st.session_state.get(SK_COUNT, 0)
st.caption(f"**{n:,}** óbitos após filtros.")

if n == 0:
    st.info("Não houveram dados registrados com esses filtros.")
    st.stop()

ts_df = st.session_state.get(SK_TS)
pyr_df = st.session_state.get(SK_PYRAMID)

# Série temporal
st.subheader("📈 Série temporal – Número de óbitos")
ts_granularidade = st.radio(
    "Granularidade",
    options=["Anos", "Meses"],
    index=0 if st.session_state.get(SK_TS_GRANULARITY, "Anos") == "Anos" else 1,
    key="analise_ts_granularidade",
    horizontal=True,
)
if ts_granularidade != st.session_state.get(SK_TS_GRANULARITY, "Anos"):
    where_stored = st.session_state.get(SK_WHERE)
    params_stored = st.session_state.get(SK_PARAMS)
    if where_stored is not None and params_stored is not None:
        st.session_state[SK_TS_GRANULARITY] = ts_granularidade
        ts_fmt = "%Y" if ts_granularidade == "Anos" else "%Y-%m"
        con_ts = _get_con()
        try:
            if ts_fmt == "%Y-%m":
                sql_ts = f"SELECT strftime(dt_obito_mes, '%Y-%m') AS periodo, count(*) AS obitos FROM {VIEW_ANALISE} WHERE {where_stored} GROUP BY dt_obito_mes ORDER BY 1"
            else:
                sql_ts = f"SELECT ano AS periodo, count(*) AS obitos FROM {VIEW_ANALISE} WHERE {where_stored} GROUP BY ano ORDER BY 1"
            ts_df = con_ts.execute(sql_ts, params_stored).fetchdf()
            st.session_state[SK_TS] = ts_df
        finally:
            con_ts.close()
        st.rerun()
ts_df = st.session_state.get(SK_TS)
ts_x_label = "Ano" if st.session_state.get(SK_TS_GRANULARITY, "Anos") == "Anos" else "Mês/Ano"
ts_x_col = "periodo" if (ts_df is not None and "periodo" in ts_df.columns) else "mes_ano"
if ts_df is not None and not ts_df.empty:
    try:
        import plotly.express as px
        fig_ts = px.line(ts_df, x=ts_x_col, y="obitos", markers=True)
        fig_ts.update_layout(xaxis_title=ts_x_label, yaxis_title="Número de óbitos", margin=dict(t=30))
        st.plotly_chart(fig_ts, use_container_width=True)
    except Exception:
        st.line_chart(ts_df.set_index(ts_x_col)["obitos"])
else:
    st.caption("Sem dados para o período.")

# Total de óbitos por causa (top 15)
where_stored = st.session_state.get(SK_WHERE)
params_stored = st.session_state.get(SK_PARAMS)
if where_stored is not None and params_stored is not None:
    st.subheader("Total de óbitos por causa")
    por_capitulo = st.radio(
        "Agrupar por",
        options=["Capítulo da CID-10", "Causa base (código)"],
        index=0,
        key="analise_causa_agrupamento",
        horizontal=True,
    )
    con_cause = _get_con()
    try:
        if por_capitulo == "Capítulo da CID-10":
            sql_causa = f"""
                SELECT causa_cid10_capitulo_desc AS rotulo, count(*) AS total
                FROM {VIEW_ANALISE} WHERE {where_stored}
                GROUP BY 1 ORDER BY 2 DESC LIMIT 15
            """
        else:
            sql_causa = f"""
                SELECT causa_basica || ' - ' || COALESCE(ANY_VALUE(COALESCE(causa_cid10_desc, causa_cid10_capitulo_desc)), '') AS rotulo,
                       count(*) AS total
                FROM {VIEW_ANALISE} WHERE {where_stored}
                GROUP BY causa_basica ORDER BY 2 DESC LIMIT 15
            """
        causa_df = con_cause.execute(sql_causa, params_stored).fetchdf()
        if causa_df is not None and not causa_df.empty:
            import plotly.express as px
            causa_df["rotulo"] = causa_df["rotulo"].astype(str).str[:60]
            fig_causa = px.bar(causa_df, y="rotulo", x="total", orientation="h", text_auto=",.0f")
            fig_causa.update_layout(
                xaxis_title="Óbitos",
                yaxis_title="",
                margin=dict(t=20, b=20),
                yaxis={"categoryorder": "total ascending"},
                showlegend=False,
            )
            st.plotly_chart(fig_causa, use_container_width=True)
            total_geral_causa = con_cause.execute(f"SELECT count(*) FROM {VIEW_ANALISE} WHERE {where_stored}", params_stored).fetchone()[0]
            total_grafico_causa = int(causa_df["total"].sum())
            demais_causa = total_geral_causa - total_grafico_causa
            if demais_causa > 0:
                pct_causa = 100.0 * demais_causa / total_geral_causa if total_geral_causa else 0
                st.caption(f"Demais causas (fora do top 15): {demais_causa:,} óbitos ({pct_causa:.1f}% do total).")
            else:
                st.caption("Todos os óbitos estão nos itens do gráfico.")
        else:
            st.caption("Sem dados para o gráfico.")
    finally:
        con_cause.close()

# Total de óbitos por território (estados ou municípios)
if where_stored is not None and params_stored is not None:
    st.subheader("Total de óbitos por território")
    por_territorio = st.radio(
        "Agrupar por",
        options=["Por estado (UF)", "Por município"],
        index=0,
        key="analise_territorio_agrupamento",
        horizontal=True,
    )
    con_terr = _get_con()
    try:
        if por_territorio == "Por estado (UF)":
            sql_uf = f"""
                SELECT uf_residencia AS rotulo, count(*) AS total
                FROM {VIEW_ANALISE} WHERE {where_stored} AND uf_residencia IS NOT NULL
                GROUP BY uf_residencia
            """
            uf_df = con_terr.execute(sql_uf, params_stored).fetchdf()
            # Garantir 27 barras: merge com lista fixa de UFs (0 onde não houver dados)
            rotulos = []
            totais = []
            for uf in UFS_ORDEM:
                rotulos.append(uf)
                row = uf_df[uf_df["rotulo"].astype(str).str.upper() == uf] if uf_df is not None and not uf_df.empty else None
                totais.append(int(row["total"].iloc[0]) if row is not None and len(row) > 0 else 0)
            territorio_df = pd.DataFrame({"rotulo": rotulos, "total": totais})
            # Ordenar por total descendente para "total ascending" no eixo y
            territorio_df = territorio_df.sort_values("total", ascending=True).reset_index(drop=True)
        else:
            sql_mun = f"""
                SELECT municipio_residencia AS rotulo, count(*) AS total
                FROM {VIEW_ANALISE} WHERE {where_stored} AND municipio_residencia IS NOT NULL
                GROUP BY municipio_residencia ORDER BY 2 DESC LIMIT 27
            """
            territorio_df = con_terr.execute(sql_mun, params_stored).fetchdf()
            if territorio_df is not None and not territorio_df.empty:
                territorio_df["rotulo"] = territorio_df["rotulo"].astype(str).str[:50]
                territorio_df = territorio_df.sort_values("total", ascending=True).reset_index(drop=True)
            else:
                territorio_df = None
        if territorio_df is not None and not territorio_df.empty:
            import plotly.express as px
            n_barras = len(territorio_df)
            fig_terr = px.bar(territorio_df, y="rotulo", x="total", orientation="h", text_auto=",.0f")
            # Altura maior quando muitas barras (ex.: 27 estados) para todas as legendas caberem
            height = max(400, n_barras * 22)
            # Forçar exibição de todas as legendas do eixo y (evitar que Plotly omita pares/ímpares)
            tickvals = territorio_df["rotulo"].tolist()
            fig_terr.update_layout(
                xaxis_title="Óbitos",
                yaxis_title="",
                margin=dict(t=20, b=20),
                yaxis={
                    "categoryorder": "total ascending",
                    "tickmode": "array",
                    "tickvals": tickvals,
                    "ticktext": tickvals,
                },
                showlegend=False,
                height=height,
            )
            st.plotly_chart(fig_terr, use_container_width=True)
            # Compilado abaixo do gráfico de território
            total_geral_terr = con_terr.execute(f"SELECT count(*) FROM {VIEW_ANALISE} WHERE {where_stored}", params_stored).fetchone()[0]
            total_grafico_terr = int(territorio_df["total"].sum())
            if por_territorio == "Por estado (UF)":
                st.caption(f"Total: {total_grafico_terr:,} óbitos nos 27 estados.")
            else:
                demais_terr = total_geral_terr - total_grafico_terr
                if demais_terr > 0:
                    pct = 100.0 * demais_terr / total_geral_terr if total_geral_terr else 0
                    st.caption(f"Demais municípios: {demais_terr:,} óbitos ({pct:.1f}% do total).")
                else:
                    st.caption("Todos os óbitos estão nos municípios do gráfico.")
        else:
            st.caption("Sem dados para o gráfico.")
    finally:
        con_terr.close()

# Pirâmide etária
st.subheader("👥 Pirâmide etária")
if pyr_df is not None and not pyr_df.empty:
    try:
        import plotly.graph_objects as go
        # Excluir Ignorado e sexo nulo/indefinido (evitar que None vá para feminino)
        s_desc = pyr_df["sexo_desc"].astype(str).str.upper().str.strip()
        mask_ignorado = s_desc.str.contains("IGNORADO", na=False)
        mask_none_ou_vazio = pyr_df["sexo_desc"].isna() | (s_desc == "") | s_desc.isin(["NONE", "NAN"])
        pyr_pyr = pyr_df[~mask_ignorado & ~mask_none_ou_vazio].copy()
        if pyr_pyr.empty:
            st.caption("Sem dados para a pirâmide (apenas sexo ignorado).")
        else:
            # Pivotar: uma linha por faixa_etaria com totais masculino e feminino
            is_masculino = pyr_pyr["sexo_desc"].astype(str).str.upper().str.contains("MASCULINO", na=False)
            pyr_pyr["sexo_tipo"] = is_masculino.map({True: "masculino", False: "feminino"})
            pivot = pyr_pyr.pivot_table(index="faixa_etaria", columns="sexo_tipo", values="total", aggfunc="sum", fill_value=0)
            if "masculino" not in pivot.columns:
                pivot["masculino"] = 0
            if "feminino" not in pivot.columns:
                pivot["feminino"] = 0
            # Ordenar pelas faixas na ordem padrão
            ordem_faixas = [f for f in reversed(FAIXAS_ETARIAS) if f in pivot.index]
            pivot = pivot.reindex(ordem_faixas).fillna(0)
            fig_pyr = go.Figure()
            fig_pyr.add_trace(go.Bar(name="Masculino", y=pivot.index, x=-(pivot["masculino"].astype(int)), orientation="h", marker_color="steelblue"))
            fig_pyr.add_trace(go.Bar(name="Feminino", y=pivot.index, x=pivot["feminino"].astype(int), orientation="h", marker_color="coral"))
            fig_pyr.update_layout(
                barmode="overlay", xaxis_title="Óbitos",
                yaxis={"categoryorder": "array", "categoryarray": list(pivot.index)},
                margin=dict(t=30), legend=dict(orientation="h", yanchor="bottom", y=1.02), showlegend=True,
            )
            st.plotly_chart(fig_pyr, use_container_width=True)
    except Exception:
        st.bar_chart(pyr_df.set_index("faixa_etaria")[["total"]])
else:
    st.caption("Sem dados para a pirâmide.")

st.markdown("---")
with st.expander("Ver amostra dos dados filtrados"):
    sample = st.session_state.get(SK_SAMPLE)
    if sample is not None and not sample.empty:
        st.dataframe(sample, use_container_width=True, hide_index=True)
    else:
        st.caption("Nenhuma amostra.")
