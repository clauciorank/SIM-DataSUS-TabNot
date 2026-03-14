"""
Editor SQL para consultar os dados da camada silver ou gold (DuckDB).
"""
import streamlit as st
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SILVER_DB = PROJECT_ROOT / "data" / "SIM" / "silver" / "obitos.duckdb"
GOLD_DB = PROJECT_ROOT / "data" / "SIM" / "gold" / "obitos.duckdb"

# Dicionário de dados: descrição por (tabela, coluna). Padrão snake_case na gold.
DESCRICOES_GOLD = {
    "v_obitos_completo": {
        "origem": "Código da origem da declaração",
        "tipo_obito": "Código tipo óbito (1 Fetal, 2 Não fetal)",
        "tipo_obito_desc": "Descrição do tipo de óbito",
        "dt_obito": "Data do óbito",
        "dt_obito_mes": "Mês do óbito (1º dia do mês)",
        "ano": "Ano do óbito (extraído da data)",
        "hora_obito": "Hora do óbito",
        "natural": "Indicador de óbito natural",
        "cod_mun_nascimento": "Código IBGE do município de nascimento",
        "dt_nascimento": "Data de nascimento",
        "idade": "Código idade (SIM)",
        "sexo": "Código sexo (1 M, 2 F)",
        "sexo_desc": "Descrição do sexo",
        "racacor": "Código raça/cor",
        "racacor_desc": "Descrição raça/cor",
        "estciv": "Código estado civil",
        "estciv_desc": "Descrição estado civil",
        "esc": "Código escolaridade",
        "esc_2010": "Código escolaridade (2010)",
        "ocup": "Código ocupação (CBO)",
        "cod_mun_residencia": "Código IBGE município de residência",
        "municipio_residencia": "Nome do município de residência",
        "uf_residencia": "UF de residência",
        "loc_ocorrencia": "Código local de ocorrência do óbito",
        "local_ocorrencia_desc": "Descrição local de ocorrência",
        "cod_mun_ocorrencia": "Código IBGE município de ocorrência",
        "municipio_ocorrencia": "Nome do município de ocorrência",
        "uf_ocorrencia": "UF de ocorrência",
        "causa_basica": "Código CID-10 da causa básica do óbito",
        "causa_cid10_capitulo_desc": "Descrição do capítulo CID-10 da causa",
        "circ_obito": "Código circunstância do óbito (acidente, suicídio, etc.)",
        "circunstancia_desc": "Descrição da circunstância",
        "peso": "Peso (gramas)",
        "sem_gestacao": "Semanas de gestação",
        "gestacao": "Código duração gestação",
        "parto": "Código tipo de parto",
        "contador": "Contador (uso interno)",
    },
}
DESCRICOES_SILVER = {
    "obitos": {
        "ORIGEM": "Código origem da declaração",
        "TIPOBITO": "Tipo óbito (1 Fetal, 2 Não fetal)",
        "dt_obito": "Data do óbito",
        "HORAOBITO": "Hora do óbito",
        "NATURAL": "Óbito natural",
        "CODMUNNATU": "Código município nascimento",
        "dt_nascimento": "Data nascimento",
        "IDADE": "Código idade",
        "SEXO": "Código sexo",
        "RACACOR": "Código raça/cor",
        "ESTCIV": "Código estado civil",
        "ESC": "Escolaridade",
        "ESC2010": "Escolaridade 2010",
        "OCUP": "Código ocupação",
        "CODMUNRES": "Código município residência",
        "LOCOCOR": "Local ocorrência",
        "CODMUNOCOR": "Código município ocorrência",
        "CAUSABAS": "Causa básica CID-10",
        "CIRCOBITO": "Circunstância óbito",
        "PESO": "Peso (gramas)",
        "SEMAGESTAC": "Semanas gestação",
        "GESTACAO": "Gestação",
        "PARTO": "Tipo parto",
        "CONTADOR": "Contador",
    },
    "municipios": {
        "codigo": "Código IBGE do município (6 dígitos)",
        "municipio": "Nome do município",
        "uf": "Sigla da UF",
    },
}

# Consultas rápidas (gold: v_obitos_completo; silver: obitos, municipios)
QUERIES_GOLD = {
    "Óbitos por cidade": """
SELECT municipio_residencia, uf_residencia, count(*) AS total
FROM v_obitos_completo
GROUP BY 1, 2 ORDER BY total DESC LIMIT 50
""",
    "Óbitos por cidade e ano": """
SELECT municipio_residencia, uf_residencia, ano, count(*) AS total
FROM v_obitos_completo
WHERE ano >= 2020
GROUP BY 1, 2, 3 ORDER BY ano DESC, total DESC LIMIT 100
""",
    "Óbitos por sexo": """
SELECT sexo_desc, count(*) AS total FROM v_obitos_completo GROUP BY 1
""",
    "Óbitos por ano": """
SELECT ano, count(*) AS total FROM v_obitos_completo GROUP BY 1 ORDER BY 1
""",
    "Óbitos por circunstância": """
SELECT circunstancia_desc, count(*) AS total
FROM v_obitos_completo GROUP BY 1 ORDER BY total DESC LIMIT 15
""",
    "Óbitos por causa (CID-10 capítulo)": """
SELECT causa_cid10_capitulo_desc, count(*) AS total
FROM v_obitos_completo
WHERE causa_cid10_capitulo_desc IS NOT NULL
GROUP BY 1 ORDER BY total DESC LIMIT 25
""",
    "Amostra": """
SELECT ano, sexo_desc, municipio_residencia, causa_basica, causa_cid10_capitulo_desc, circunstancia_desc
FROM v_obitos_completo LIMIT 50
""",
}
QUERIES_SILVER = {
    "Óbitos por cidade": """
SELECT m.municipio AS nome_municipio, m.uf, count(*) AS total_obitos
FROM obitos o
LEFT JOIN municipios m ON m.codigo = TRIM(CAST(o.CODMUNRES AS VARCHAR))
WHERE o.CODMUNRES IS NOT NULL
GROUP BY 1, 2 ORDER BY total_obitos DESC LIMIT 50
""",
    "Óbitos por ano": """
SELECT year(dt_obito) AS ano, count(*) AS total FROM obitos GROUP BY 1 ORDER BY 1
""",
    "Amostra (silver)": """
SELECT * FROM obitos LIMIT 100
""",
}


def get_schema(con) -> list[tuple]:
    """Lista tabelas/views e colunas do DuckDB."""
    try:
        return con.execute("""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'main'
            ORDER BY table_name, ordinal_position
        """).fetchall()
    except Exception:
        return []


def build_data_dictionary(schema: list[tuple], descricoes: dict) -> pd.DataFrame:
    """Monta tabela do dicionário de dados: coluna, tipo, descrição (sem nome da tabela)."""
    rows = []
    for table_name, column_name, data_type in schema:
        desc = ""
        if table_name in descricoes and column_name in descricoes[table_name]:
            desc = descricoes[table_name][column_name]
        rows.append({"Coluna": column_name, "Tipo": data_type, "Descrição": desc})
    return pd.DataFrame(rows)


def _is_read_only_query(sql: str) -> bool:
    """Retorna True se o SQL for apenas consulta (SELECT ou WITH). Bloqueia qualquer edição."""

    def _first_keyword(stmt: str) -> str:
        stmt = stmt.strip()
        for line in stmt.split("\n"):
            line = line.strip()
            if not line or line.startswith("--") or line.startswith("#"):
                continue
            if "--" in line:
                line = line[: line.index("--")].strip()
            if line:
                return (line.split()[0].upper() if line.split() else "")
        return ""

    for part in sql.split(";"):
        part = part.strip()
        if not part:
            continue
        if _first_keyword(part) not in ("SELECT", "WITH"):
            return False
    return True


st.title("📝 Editor SQL")

# Escolhe gold se existir, senão silver
USE_GOLD = GOLD_DB.exists()
DUCKDB_PATH = GOLD_DB if USE_GOLD else SILVER_DB
camada = "gold" if USE_GOLD else "silver"

st.markdown(
    f"Execute consultas SQL sobre os dados de óbitos (camada **{camada}**). "
    + ("View principal: **`v_obitos_completo`** (todas as legendas)." if USE_GOLD else "Construa a camada gold para usar `v_obitos_completo`.")
)
st.markdown("---")

if not SILVER_DB.exists() and not GOLD_DB.exists():
    st.warning(
        "Nenhum dado encontrado. Faça o download e processe em **Download de Dados** "
        "(silver); opcionalmente construa a **view única (gold)**."
    )
    st.stop()

# Dicionário de dados: todas as colunas em formato tabela
with st.expander("📋 Dicionário de dados (colunas disponíveis)", expanded=True):
    try:
        import duckdb

        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        schema = get_schema(con)
        if USE_GOLD:
            schema = [(t, c, d) for t, c, d in schema if t == "v_obitos_completo"]
        descricoes = DESCRICOES_GOLD if USE_GOLD else DESCRICOES_SILVER
        df_dict = build_data_dictionary(schema, descricoes)
        con.close()

        if not df_dict.empty:
            st.dataframe(
                df_dict,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Coluna": st.column_config.TextColumn("Coluna", width="medium"),
                    "Tipo": st.column_config.TextColumn("Tipo", width="small"),
                    "Descrição": st.column_config.TextColumn("Descrição", width="large"),
                },
            )
            st.caption(f"Total: **{len(df_dict)}** colunas.")
        else:
            st.caption("Nenhuma tabela ou view encontrada.")
    except Exception as e:
        st.caption(f"Não foi possível carregar o dicionário: {e}")

# Consultas rápidas
st.subheader("Consultas rápidas")
queries = QUERIES_GOLD if USE_GOLD else QUERIES_SILVER
cols = st.columns(2)
for i, (label, sql) in enumerate(queries.items()):
    with cols[i % 2]:
        if st.button(label, key=f"q_{i}"):
            st.session_state["sql_editor"] = sql.strip()
            st.rerun()

# Editor SQL
default_sql = "SELECT * FROM v_obitos_completo LIMIT 20" if USE_GOLD else "SELECT * FROM obitos LIMIT 20"
if "sql_editor" not in st.session_state:
    st.session_state["sql_editor"] = default_sql
sql = st.text_area(
    "SQL",
    height=140,
    placeholder="Digite sua consulta SQL...",
    key="sql_editor",
)

col1, col2 = st.columns([1, 4])
with col1:
    run = st.button("Executar", type="primary")

if run and sql.strip():
    if not _is_read_only_query(sql):
        st.error("Somente consultas **SELECT** (ou **WITH** para CTEs) são permitidas. Edição de dados não é permitida.")
    else:
        try:
            import duckdb

            con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
            df = con.execute(sql).fetchdf()
            con.close()

            if df.empty:
                st.info("Consulta retornou nenhuma linha.")
            else:
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.caption(f"**{len(df):,}** linha(s)")
        except Exception as e:
            st.error(f"Erro ao executar: {e}")
