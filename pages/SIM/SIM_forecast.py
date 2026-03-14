"""
Previsão de óbitos (forecasting) – SIM.
Reutiliza filtros da análise exploratória, consulta v_obitos_completo e executa
a pipeline de previsão (MortalityForecaster) sem alterá-la.
"""
import sys
from pathlib import Path
_pages_sim = Path(__file__).resolve().parent
if str(_pages_sim) not in sys.path:
    sys.path.insert(0, str(_pages_sim))

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from dateutil.relativedelta import relativedelta

from sim_filters import (
    GOLD_DB,
    VIEW_ANALISE,
    FAIXAS_ETARIAS,
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

# Chaves de cache para a página de forecast (evitar conflito com Analise)
FC_CACHE_MIN_DATE = "forecast_sim_cache_min_date"
FC_CACHE_MAX_DATE = "forecast_sim_cache_max_date"
FC_CACHE_OPCOES_SEXO = "forecast_sim_cache_opcoes_sexo"
FC_CACHE_UF_LIST = "forecast_sim_cache_uf_list"
FC_CACHE_CAP_LIST = "forecast_sim_cache_cap_list"
FC_CACHE_MUN_LIST = "forecast_sim_cache_mun_list"
FC_CACHE_MUN_UF_KEY = "forecast_sim_cache_mun_uf_key"
FC_CACHE_CAUSAS_LABELS = "forecast_sim_cache_causas_labels"
FC_CACHE_CAUSAS_CAP_KEY = "forecast_sim_cache_causas_cap_key"


def _normalize_todos_multiselect(key: str, all_options: list, sentinel: str):
    val = st.session_state.get(key, [sentinel])
    if val == []:
        val = [sentinel]
    opts = [o for o in all_options if o != sentinel] if (val and sentinel not in val) else all_options
    default_val = [x for x in val if x != sentinel] if (sentinel in val and len(val) > 1) else val
    return opts, default_val


def _ensure_forecast_caches(conn):
    if FC_CACHE_MIN_DATE not in st.session_state:
        min_date, max_date = get_bounds_dt_obito(conn)
        st.session_state[FC_CACHE_MIN_DATE] = min_date
        st.session_state[FC_CACHE_MAX_DATE] = max_date
    if FC_CACHE_OPCOES_SEXO not in st.session_state:
        st.session_state[FC_CACHE_OPCOES_SEXO] = ["Todos"] + _opts_sexo_silver(conn)
    if FC_CACHE_UF_LIST not in st.session_state:
        st.session_state[FC_CACHE_UF_LIST] = _opts_uf_silver(conn)
    if FC_CACHE_CAP_LIST not in st.session_state:
        st.session_state[FC_CACHE_CAP_LIST] = _opts_capitulos_silver(conn)


st.set_page_config(page_title="Previsão de óbitos - SIM", layout="wide")
st.title("Previsão de óbitos (forecasting)")

if not GOLD_DB.exists():
    st.warning("A camada **gold** não foi encontrada. Faça o download e processe em **Download de Dados**.")
    st.stop()

con = None
need_static = FC_CACHE_UF_LIST not in st.session_state
if need_static:
    con = _get_con()
    if not _view_exists(con):
        con.close()
        st.warning(
            "A view **v_obitos_completo** não existe. Reconstrua a camada gold para ativar a previsão."
        )
        st.stop()
    _ensure_forecast_caches(con)

default_d1 = _to_date(st.session_state.get(FC_CACHE_MIN_DATE)) if FC_CACHE_MIN_DATE in st.session_state else None
default_d2 = _to_date(st.session_state.get(FC_CACHE_MAX_DATE)) if FC_CACHE_MAX_DATE in st.session_state else None
opcoes_sexo = st.session_state.get(FC_CACHE_OPCOES_SEXO, ["Todos"])
uf_list = st.session_state.get(FC_CACHE_UF_LIST, [])
cap_list = st.session_state.get(FC_CACHE_CAP_LIST, [])

uf_sel_raw = st.session_state.get("forecast_uf", uf_list if uf_list else [])
uf_para_cascade = [u for u in uf_sel_raw if u != "Todas"] if uf_sel_raw else []
mun_uf_key = tuple(sorted(uf_para_cascade))
cap_sel_raw = st.session_state.get("forecast_cap", cap_list if cap_list else [])
cap_para_cascade = [c for c in cap_sel_raw if c != "Todos"] if cap_sel_raw else []
causas_cap_key = tuple(sorted(cap_para_cascade))

need_mun = st.session_state.get(FC_CACHE_MUN_UF_KEY) != mun_uf_key
need_causas = st.session_state.get(FC_CACHE_CAUSAS_CAP_KEY) != causas_cap_key
if need_mun or need_causas:
    if con is None:
        con = _get_con()
    if need_mun:
        mun_list = _opts_municipio_silver(con, list(uf_para_cascade))
        st.session_state[FC_CACHE_MUN_LIST] = mun_list
        st.session_state[FC_CACHE_MUN_UF_KEY] = mun_uf_key
    else:
        mun_list = st.session_state.get(FC_CACHE_MUN_LIST, [])
    if need_causas:
        causas_labels = _opts_causas_silver(con, list(cap_para_cascade))
        st.session_state[FC_CACHE_CAUSAS_LABELS] = causas_labels
        st.session_state[FC_CACHE_CAUSAS_CAP_KEY] = causas_cap_key
        st.session_state["forecast_causa"] = []
    else:
        causas_labels = st.session_state.get(FC_CACHE_CAUSAS_LABELS, [])
else:
    mun_list = st.session_state.get(FC_CACHE_MUN_LIST, [])
    causas_labels = st.session_state.get(FC_CACHE_CAUSAS_LABELS, [])

if con is not None:
    con.close()
    con = None

default_uf = uf_sel_raw if uf_sel_raw else (uf_list or [])
default_cap = cap_sel_raw if cap_sel_raw else (cap_list or [])

# ---- Filtros ----
st.subheader("Filtros")
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
            key="forecast_date_inicio",
        )
        d2 = st.date_input(
            "Data fim",
            value=default_d2,
            min_value=default_d1,
            max_value=default_d2,
            key="forecast_date_fim",
        )
        if d1 and d2 and d1 > d2:
            d1, d2 = d2, d1
with c2:
    opts_sexo, default_sexo = _normalize_todos_multiselect("forecast_sexo", opcoes_sexo, "Todos")
    sexo_sel = st.multiselect("Sexo", opts_sexo, default=default_sexo, key="forecast_sexo")
with c3:
    all_faixas = ["Todas"] + FAIXAS_ETARIAS
    opts_faixa, default_faixa = _normalize_todos_multiselect("forecast_faixa", all_faixas, "Todas")
    faixa_sel = st.multiselect("Faixa etária", opts_faixa, default=default_faixa, key="forecast_faixa")

r2a, r2b = st.columns(2)
with r2a:
    uf_sel = st.multiselect("UF (residência)", uf_list, default=default_uf, key="forecast_uf")
with r2b:
    all_muns = ["Todos"] + mun_list
    opts_mun, default_mun = _normalize_todos_multiselect("forecast_mun", all_muns, "Todos")
    mun_sel = st.multiselect(
        "Município (residência)",
        opts_mun,
        default=default_mun,
        key="forecast_mun",
        max_selections=MAX_SELECT_MUNICIPIO,
        help=f"Máximo {MAX_SELECT_MUNICIPIO} municípios para evitar travamento.",
    )

cap_sel = st.multiselect("Capítulo CID-10 (causa)", cap_list, default=default_cap, key="forecast_cap")
causa_sel = st.multiselect(
    "Causa do óbito (código - descrição)",
    causas_labels,
    default=st.session_state.get("forecast_causa") or [],
    key="forecast_causa",
    max_selections=MAX_SELECT_CAUSA,
    help=f"Selecione até {MAX_SELECT_CAUSA} causas. Use o capítulo CID-10 acima para filtrar. Máximo para evitar travamento.",
)

st.markdown("")
# Frequência e horizonte
st.subheader("Configuração da previsão")


def _on_freq_change():
    f = st.session_state.get("forecast_freq", "Anual")
    st.session_state["forecast_horizon"] = 16 if f == "Mensal" else 2
    st.rerun()


freq = st.radio(
    "Frequência da série",
    options=["Anual", "Mensal"],
    index=0,
    key="forecast_freq",
    horizontal=True,
    help="Série anual: um valor por ano. Série mensal: um valor por mês.",
    on_change=_on_freq_change,
)
is_monthly = freq == "Mensal"
max_horizon = 30 if is_monthly else 10
default_horizon = 16 if is_monthly else 2
horizon = st.number_input(
    "Passos a prever (horizonte)",
    min_value=1,
    max_value=max_horizon,
    value=st.session_state.get("forecast_horizon", default_horizon),
    step=1,
    key="forecast_horizon",
    help="Número de periodos à frente que o modelo prevê (ex.: 2 anos, 16 meses). Quanto maior, maior a incerteza.",
)
st.caption("Quanto maior o número de passos, maior a incerteza da previsão.")

gerar = st.button("Gerar previsão", type="primary", use_container_width=True)

if not gerar:
    st.info("Ajuste os filtros e o horizonte e clique em **Gerar previsão** para executar a pipeline.")
    st.stop()

# Construir where/params
sexo_eff = _selection_for_where(sexo_sel, [o for o in opcoes_sexo if o != "Todos"], "Todos")
faixa_eff = _selection_for_where(faixa_sel, FAIXAS_ETARIAS, "Todas")
uf_eff = _effective_sel_for_where(uf_sel, uf_list, "Todas")
mun_eff = _selection_for_where(mun_sel, mun_list, "Todos")
cap_eff = _effective_sel_for_where(cap_sel, cap_list, "Todos")
where, params = _build_where_and_params(
    d1, d2, sexo_eff, faixa_eff, uf_eff, mun_eff, cap_eff, causa_sel, [], []
)

conn = _get_con()
try:
    if is_monthly:
        sql = f"""
        SELECT strftime(dt_obito_mes, '%Y-%m') AS periodo, count(*) AS total
        FROM {VIEW_ANALISE} WHERE {where} GROUP BY dt_obito_mes ORDER BY 1
        """
        df_raw = conn.execute(sql, params).fetchdf()
        if df_raw is None or df_raw.empty:
            st.warning("Nenhum dado encontrado com os filtros selecionados.")
            st.stop()
        df_raw["periodo"] = pd.to_datetime(df_raw["periodo"].astype(str) + "-01")
        min_per = df_raw["periodo"].min()
        max_per = df_raw["periodo"].max()
        full_range = pd.date_range(start=min_per, end=max_per, freq="MS")
        full_df = pd.DataFrame({"periodo": full_range})
        full_df = full_df.merge(df_raw, on="periodo", how="left")
        full_df["total"] = full_df["total"].fillna(0).astype(int)
        full_df = full_df.sort_values("periodo").reset_index(drop=True)
        series_arr = full_df["total"].values
        periodos = full_df["periodo"]
    else:
        sql = f"SELECT ano, count(*) AS total FROM {VIEW_ANALISE} WHERE {where} GROUP BY ano ORDER BY ano"
        df_raw = conn.execute(sql, params).fetchdf()
        if df_raw is None or df_raw.empty:
            st.warning("Nenhum dado encontrado com os filtros selecionados.")
            st.stop()
        min_ano = int(df_raw["ano"].min())
        max_ano = int(df_raw["ano"].max())
        full_anos = pd.DataFrame({"ano": range(min_ano, max_ano + 1)})
        full_df = full_anos.merge(df_raw, on="ano", how="left")
        full_df["total"] = full_df["total"].fillna(0).astype(int)
        full_df = full_df.sort_values("ano").reset_index(drop=True)
        series_arr = full_df["total"].values
        periodos = full_df["ano"].astype(str)
finally:
    conn.close()

# Regra 20 óbitos/ano
if is_monthly:
    por_ano = full_df.groupby(full_df["periodo"].dt.to_period("Y"))["total"].sum()
    if np.any(por_ano.values < 20):
        st.warning("O número de mortes em um ou mais anos é muito baixo para predição confiável. A previsão pode ter maior incerteza.")
    st.caption("O número de mortes por mês pode ser baixo; as predições podem ter maior incerteza.")
else:
    if np.any(series_arr < 20):
        st.warning("O número de mortes em um ou mais anos é muito baixo para predição confiável.")

# Data final da predição
if is_monthly:
    ultima_data = full_df["periodo"].max()
    if hasattr(ultima_data, "to_pydatetime"):
        ultima_py = ultima_data.to_pydatetime()
    else:
        ultima_py = pd.Timestamp(ultima_data)
    fim_pred = ultima_py + relativedelta(months=horizon)
    st.caption(f"Última data na série: {ultima_py.strftime('%d/%m/%Y')}. Com **{horizon}** passos à frente (mensal), a predição se encerra em **{fim_pred.strftime('%d/%m/%Y')}**.")
else:
    ultimo_ano = int(full_df["ano"].max())
    fim_ano = ultimo_ano + horizon
    st.caption(f"A predição se encerra em **{fim_ano}**.")

# Executar pipeline
frequency = "monthly" if is_monthly else "yearly"
try:
    from src.forecasting.MortalityForecaster import MortalityForecaster, ForecastResult
except Exception:
    st.error("Módulo de previsão não encontrado (src.forecasting.MortalityForecaster).")
    st.stop()

with st.status("Executando pipeline de previsão...", expanded=True) as status:
    st.write("Pode levar alguns minutos. Os cálculos estão sendo realizados.")
    progress = st.progress(0.3, text="Ajustando modelos...")
    try:
        mf = MortalityForecaster(series_arr, frequency=frequency, horizon=horizon)
        result = mf.fit()
    finally:
        progress.progress(1.0, text="Concluído.")
    status.update(label="Pipeline concluído", state="complete")

if result is None:
    st.error("A previsão não retornou resultado.")
    st.stop()

# ---- Gráfico da previsão ----
st.subheader("Gráfico da previsão")
n_hist = len(series_arr)
x_hist = list(range(n_hist))
x_fc = list(range(n_hist, n_hist + result.horizon))
if is_monthly and "periodo" in full_df.columns:
    labels_hist = full_df["periodo"].dt.strftime("%Y-%m").tolist()
    last_per = full_df["periodo"].iloc[-1]
    labels_fc = [(pd.Timestamp(last_per) + relativedelta(months=i+1)).strftime("%Y-%m") for i in range(result.horizon)]
else:
    labels_hist = periodos.astype(str).tolist()
    last_ano = int(full_df["ano"].iloc[-1])
    labels_fc = [str(last_ano + i + 1) for i in range(result.horizon)]

# Previsão em inteiros para exibição; trace de previsão começa no último ponto histórico para linha contínua
forecast_int = np.round(result.forecast).astype(int)
x_previsao_conectada = [n_hist - 1] + x_fc
y_previsao_conectada = [int(series_arr[-1])] + forecast_int.tolist()
# Rótulo do hover: mês (mensal) ou ano (anual)
hover_label = "Mês" if is_monthly else "Ano"

fig = go.Figure()
# IC no fundo para não bloquear o hover dos pontos (ordem: primeiro desenhado fica atrás)
fig.add_trace(go.Scatter(x=x_fc + x_fc[::-1], y=np.r_[result.upper_95, result.lower_95[::-1]], fill="toself", fillcolor="rgba(0,100,80,0.2)", line=dict(color="rgba(255,255,255,0)"), name="IC 95%"))
fig.add_trace(go.Scatter(x=x_fc + x_fc[::-1], y=np.r_[result.upper_80, result.lower_80[::-1]], fill="toself", fillcolor="rgba(0,100,80,0.3)", line=dict(color="rgba(255,255,255,0)"), name="IC 80%"))
fig.add_trace(go.Scatter(
    x=x_hist, y=series_arr, name="Histórico", mode="lines+markers", line=dict(color="blue"),
    customdata=labels_hist,
    hovertemplate=f"{hover_label}: %{{customdata}}<br>Óbitos: %{{y:.0f}}<extra></extra>",
))
fig.add_trace(go.Scatter(
    x=x_previsao_conectada, y=y_previsao_conectada, name="Previsão", mode="lines+markers", line=dict(color="green", dash="dash"),
    customdata=[labels_hist[-1]] + labels_fc,
    hovertemplate=f"{hover_label}: %{{customdata}}<br>Óbitos: %{{y:.0f}}<extra></extra>",
))

# Eixo X: no mensal limitar rótulos para evitar sobreposição
x_all = x_hist + x_fc
labels_all = labels_hist + labels_fc
N_MAX_TICKS = 24
if is_monthly and len(labels_all) > N_MAX_TICKS:
    step = max(1, len(x_all) // N_MAX_TICKS)
    tickvals = list(x_all[::step])
    ticktext = [labels_all[i] for i in range(0, len(labels_all), step)]
    if x_all[-1] not in tickvals:
        tickvals.append(x_all[-1])
        ticktext.append(labels_all[-1])
else:
    tickvals = x_all
    ticktext = labels_all

fig.update_layout(
    xaxis=dict(tickvals=tickvals, ticktext=ticktext, tickangle=-45),
    xaxis_title="Período",
    yaxis_title="Número de óbitos",
    margin=dict(t=40),
    showlegend=True,
)
st.plotly_chart(fig, use_container_width=True)
with st.expander("O que são os intervalos de confiança?"):
    st.markdown("**Intervalo de confiança (80% / 95%):** faixa em que a previsão tende a cair com essa probabilidade.")

# ---- Modelos e métricas ----
st.subheader("Modelos testados e métricas")
with st.expander("O que é AICc / RMSE de validação?"):
    st.markdown(
        "**AICc:** critério de qualidade do modelo (menor é melhor). "
        "**RMSE de validação:** erro em dados reservados para validação; usado para escolher o modelo quando há série longa (mensal)."
    )
d = result.diagnostics
metric = d.get("selection_metric", "AICc")
prefix = "aicc_" if metric == "AICc" else "rmse_"
score_keys = {k: v for k, v in d.items() if k.startswith(prefix)}
rows = []
for k, v in score_keys.items():
    name = k.replace(prefix, "").replace("_", " ").title()
    selected = "✓" if name.lower() in result.model_name.lower() else ""
    rows.append({"Modelo": name, metric: round(v, 2) if isinstance(v, (int, float)) else v, "Selecionado": selected})
if rows:
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
col_rmse, col_mape = st.columns(2)
with col_rmse:
    rmse_val = f"{result.rmse:.2f}" if result.rmse is not None else "—"
    st.metric(
        "RMSE",
        rmse_val,
        help="Erro típico da previsão na mesma unidade (óbitos): quanto maior, maior a incerteza. Valores menores indicam previsões mais precisas.",
    )
with col_mape:
    mape_val = f"{result.mape:.2f} %" if result.mape is not None else "—"
    st.metric(
        "MAPE",
        mape_val,
        help="Erro percentual médio da previsão: indica, em média, quanto a previsão se desvia dos valores que seriam observados. Ex.: 5% significa desvio médio de 5% em relação ao valor real.",
    )

# ---- Decomposição (apenas série mensal) ----
if is_monthly:
    st.subheader("Decomposição da série")
    if len(series_arr) >= 24:
        try:
            from statsmodels.tsa.seasonal import seasonal_decompose
            idx = full_df["periodo"]
            s = pd.Series(series_arr, index=idx)
            decomp = seasonal_decompose(s, model="additive", period=12)
            # Usar datas no eixo X e no hover para mostrar meses em vez do índice
            hovertemplate_mes = "Mês: %{x|%Y-%m}<br>Valor: %{y:.2f}<extra></extra>"
            fig_trend = go.Figure()
            fig_trend.add_trace(go.Scatter(x=decomp.trend.dropna().index, y=decomp.trend.dropna().values, name="Tendência", line=dict(color="blue"), hovertemplate=hovertemplate_mes))
            fig_trend.update_layout(title="Tendência", height=280, xaxis_title="Mês", yaxis_title="Valor", margin=dict(t=40), xaxis=dict(type="date", tickformat="%Y-%m"))
            st.plotly_chart(fig_trend, use_container_width=True)
            fig_season = go.Figure()
            fig_season.add_trace(go.Scatter(x=decomp.seasonal.dropna().index, y=decomp.seasonal.dropna().values, name="Sazonalidade", line=dict(color="green"), hovertemplate=hovertemplate_mes))
            fig_season.update_layout(title="Sazonalidade", height=280, xaxis_title="Mês", yaxis_title="Valor", margin=dict(t=40), xaxis=dict(type="date", tickformat="%Y-%m"))
            st.plotly_chart(fig_season, use_container_width=True)
            fig_resid = go.Figure()
            fig_resid.add_trace(go.Scatter(x=decomp.resid.dropna().index, y=decomp.resid.dropna().values, name="Resíduo", line=dict(color="gray"), hovertemplate=hovertemplate_mes))
            fig_resid.update_layout(title="Resíduo (ruído)", height=280, xaxis_title="Mês", yaxis_title="Valor", margin=dict(t=40), xaxis=dict(type="date", tickformat="%Y-%m"))
            st.plotly_chart(fig_resid, use_container_width=True)
        except Exception as e:
            st.caption(f"Decomposição não disponível: {e}")
    else:
        st.caption("Série mensal com menos de 24 pontos: decomposição sazonal não aplicada.")

if result.warnings:
    st.subheader("Avisos do modelo")
    for w in result.warnings:
        st.warning(w)
