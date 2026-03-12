"""
Catálogo da camada gold: apenas a view pronta para consumo.
Dados intermediários (óbitos, municípios, legendas) ficam na silver.
"""
from pathlib import Path
from typing import Optional

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SILVER_PATH = PROJECT_ROOT / "data" / "SIM" / "silver"
GOLD_PATH = PROJECT_ROOT / "data" / "SIM" / "gold"
GOLD_DB = GOLD_PATH / "obitos.duckdb"


def ensure_silver_legendas(silver_path: Optional[Path] = None) -> None:
    """
    Garante que todos os parquets de legenda existam na silver.
    Chamado antes de construir a view gold; intermediários ficam na silver.
    """
    silver_path = Path(silver_path or SILVER_PATH)
    silver_path.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(":memory:")

    def write_parquet(name: str, sql: str) -> None:
        con.execute(sql)
        out = str((silver_path / f"{name}.parquet").resolve()).replace("\\", "/")
        con.execute(f"COPY {name} TO '{out}' (FORMAT PARQUET)")

    write_parquet("legenda_sexo", """
        CREATE OR REPLACE TABLE legenda_sexo AS SELECT * FROM (VALUES
            ('1', 'Masculino'), ('2', 'Feminino'), ('0', 'Ignorado')
        ) AS t(codigo, descricao)
    """)
    write_parquet("legenda_racacor", """
        CREATE OR REPLACE TABLE legenda_racacor AS SELECT * FROM (VALUES
            ('1', 'Branca'), ('2', 'Preta'), ('3', 'Amarela'), ('4', 'Parda'),
            ('5', 'Indígena'), ('9', 'Ignorado')
        ) AS t(codigo, descricao)
    """)
    write_parquet("legenda_circunstancia", """
        CREATE OR REPLACE TABLE legenda_circunstancia AS SELECT * FROM (VALUES
            ('1', 'Acidente'), ('2', 'Suicídio'), ('3', 'Homicídio'), ('4', 'Outro'),
            ('5', 'Ignorado'), ('0', 'Ignorado')
        ) AS t(codigo, descricao)
    """)
    write_parquet("legenda_tipo_obito", """
        CREATE OR REPLACE TABLE legenda_tipo_obito AS SELECT * FROM (VALUES
            ('1', 'Fetal'), ('2', 'Não fetal'), ('0', 'Ignorado')
        ) AS t(codigo, descricao)
    """)
    write_parquet("legenda_local_ocorrencia", """
        CREATE OR REPLACE TABLE legenda_local_ocorrencia AS SELECT * FROM (VALUES
            ('1', 'Hospital'), ('2', 'Outro estabelecimento de saúde'), ('3', 'Domicílio'),
            ('4', 'Via pública'), ('5', 'Outros'), ('9', 'Ignorado'), ('0', 'Ignorado')
        ) AS t(codigo, descricao)
    """)
    write_parquet("legenda_estado_civil", """
        CREATE OR REPLACE TABLE legenda_estado_civil AS SELECT * FROM (VALUES
            ('1', 'Solteiro'), ('2', 'Casado'), ('3', 'Viúvo'), ('4', 'Separado'),
            ('5', 'União estável'), ('9', 'Ignorado'), ('0', 'Ignorado')
        ) AS t(codigo, descricao)
    """)
    # CID-10 oficial (OMS/Datasus): uma linha por letra; descrição do capítulo pode repetir (ex.: A e B = mesmo capítulo).
    write_parquet("legenda_cid10_capitulo", """
        CREATE OR REPLACE TABLE legenda_cid10_capitulo AS SELECT * FROM (VALUES
            ('A', 'Doenças infecciosas e parasitárias (A00-B99)'),
            ('B', 'Doenças infecciosas e parasitárias (A00-B99)'),
            ('C', 'Neoplasias (C00-D48)'),
            ('D', 'Doenças do sangue e dos órgãos hematopoéticos e alguns transtornos imunitários (D50-D89)'),
            ('E', 'Doenças endócrinas, nutricionais e metabólicas (E00-E90)'),
            ('F', 'Transtornos mentais e comportamentais (F00-F99)'),
            ('G', 'Doenças do sistema nervoso (G00-G99)'),
            ('H', 'Doenças do olho, ouvido e anexos (H00-H95)'),
            ('I', 'Doenças do aparelho circulatório (I00-I99)'),
            ('J', 'Doenças do aparelho respiratório (J00-J99)'),
            ('K', 'Doenças do aparelho digestivo (K00-K93)'),
            ('L', 'Doenças da pele e tecido subcutâneo (L00-L99)'),
            ('M', 'Doenças osteomuscular e tecido conjuntivo (M00-M99)'),
            ('N', 'Doenças do aparelho geniturinário (N00-N99)'),
            ('O', 'Gravidez, parto e puerpério (O00-O99)'),
            ('P', 'Afecções do período perinatal (P00-P96)'),
            ('Q', 'Malformações congênitas e anomalias cromossômicas (Q00-Q99)'),
            ('R', 'Sintomas e achados anormais (R00-R99)'),
            ('S', 'Lesões e envenenamentos (S00-T98)'),
            ('T', 'Lesões e envenenamentos (S00-T98)'),
            ('U', 'Códigos para propósitos especiais (U00-U99)'),
            ('V', 'Causas externas de morbidade e mortalidade (V01-Y98)'),
            ('W', 'Causas externas de morbidade e mortalidade (V01-Y98)'),
            ('X', 'Causas externas de morbidade e mortalidade (V01-Y98)'),
            ('Y', 'Causas externas de morbidade e mortalidade (V01-Y98)'),
            ('Z', 'Fatores que influenciam estado de saúde (Z00-Z99)')
        ) AS t(letra, descricao)
    """)
    con.close()


def ensure_cid10_causa_legenda(silver_path: Optional[Path] = None) -> None:
    """
    Garante que o parquet legenda_cid10_causa (código CID-10 -> descrição) exista na silver.
    Usa a tabela CID10 do Datasus via pysus (download sob demanda).
    Se o arquivo existir mas estiver vazio, recria (re-download). Colunas normalizadas: codigo, descricao.
    """
    silver_path = Path(silver_path or SILVER_PATH)
    silver_path.mkdir(parents=True, exist_ok=True)
    out_path = silver_path / "legenda_cid10_causa.parquet"
    re_download = False
    if out_path.exists():
        try:
            import pandas as pd
            existing = pd.read_parquet(out_path)
            if existing is None or len(existing) == 0:
                re_download = True
        except Exception:
            re_download = True
        if re_download:
            out_path.unlink(missing_ok=True)
    if out_path.exists():
        return
    try:
        from pysus.online_data.SIM import get_CID10_table
        import pandas as pd
        df = get_CID10_table(cache=True)
        if df is None or df.empty:
            pd.DataFrame(columns=["codigo", "descricao"]).to_parquet(out_path, index=False)
            return
        cod_col = "CID10" if "CID10" in df.columns else ("codigo" if "codigo" in df.columns else None)
        desc_col = "DESCR" if "DESCR" in df.columns else ("descricao" if "descricao" in df.columns else None)
        if not cod_col or not desc_col:
            pd.DataFrame(columns=["codigo", "descricao"]).to_parquet(out_path, index=False)
            return
        legenda = df[[cod_col, desc_col]].copy()
        legenda.columns = ["codigo", "descricao"]
        legenda["codigo"] = legenda["codigo"].astype(str).str.strip()
        legenda["descricao"] = legenda["descricao"].astype(str).str.strip()
        legenda = legenda.drop_duplicates(subset=["codigo"], keep="first")
        legenda.to_parquet(out_path, index=False)
    except Exception:
        import pandas as pd
        pd.DataFrame(columns=["codigo", "descricao"]).to_parquet(out_path, index=False)


def build_gold_catalog(
    silver_path: Optional[Path] = None,
    gold_path: Optional[Path] = None,
) -> dict:
    """
    Garante legendas na silver e cria na gold apenas a view v_obitos_completo
    (pronta para consumo). Gold não armazena tabelas, só a definição da view.
    """
    silver_path = Path(silver_path or SILVER_PATH)
    gold_path = Path(gold_path or GOLD_PATH)
    gold_path.mkdir(parents=True, exist_ok=True)

    obitos_parquet = silver_path / "obitos.parquet"
    municipios_parquet = silver_path / "municipios.parquet"
    if not obitos_parquet.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {obitos_parquet}. "
            "Execute o processamento silver antes."
        )
    if not municipios_parquet.exists():
        from src.data_extraction.municipios import build_municipios_table
        build_municipios_table(municipios_parquet)

    ensure_silver_legendas(silver_path)
    ensure_cid10_causa_legenda(silver_path)

    from src.data_extraction.cid10_depara import ensure_cid10_depara
    use_depara = ensure_cid10_depara(silver_path)

    o = str(obitos_parquet.resolve()).replace("\\", "/")
    m = str(municipios_parquet.resolve()).replace("\\", "/")
    ls_path = str((silver_path / "legenda_sexo.parquet").resolve()).replace("\\", "/")
    lr_path = str((silver_path / "legenda_racacor.parquet").resolve()).replace("\\", "/")
    lc_path = str((silver_path / "legenda_circunstancia.parquet").resolve()).replace("\\", "/")
    lt_path = str((silver_path / "legenda_tipo_obito.parquet").resolve()).replace("\\", "/")
    loc_path = str((silver_path / "legenda_local_ocorrencia.parquet").resolve()).replace("\\", "/")
    le_path = str((silver_path / "legenda_estado_civil.parquet").resolve()).replace("\\", "/")
    lcid_path = str((silver_path / "legenda_cid10_capitulo.parquet").resolve()).replace("\\", "/")
    lcod_path = str((silver_path / "legenda_cid10_causa.parquet").resolve()).replace("\\", "/")
    d_path = str((silver_path / "cid10_depara.parquet").resolve()).replace("\\", "/").replace("'", "''")

    # Normalização do código CID-10 para join: com ponto quando 4 caracteres (ex.: A900 -> A90.0)
    causa_norm = (
        "CASE WHEN INSTR(TRIM(COALESCE(CAST(o.CAUSABAS AS VARCHAR), '')), '.') > 0 "
        "THEN TRIM(COALESCE(CAST(o.CAUSABAS AS VARCHAR), '')) "
        "WHEN LENGTH(TRIM(COALESCE(CAST(o.CAUSABAS AS VARCHAR), ''))) = 4 "
        "THEN SUBSTR(TRIM(CAST(o.CAUSABAS AS VARCHAR)), 1, 3) || '.' || SUBSTR(TRIM(CAST(o.CAUSABAS AS VARCHAR)), 4, 1) "
        "ELSE TRIM(COALESCE(CAST(o.CAUSABAS AS VARCHAR), '')) END"
    )
    if use_depara:
        cid_join = f"""
        LEFT JOIN read_parquet('{d_path}') d_cid ON d_cid.codigo = ({causa_norm})
        """
        cid_select = "COALESCE(d_cid.capitulo_descricao, lcid.descricao) AS causa_cid10_capitulo_desc,\n            COALESCE(d_cid.descricao, lcod.descricao) AS causa_cid10_desc,"
    else:
        cid_join = ""
        cid_select = "lcid.descricao AS causa_cid10_capitulo_desc,\n            lcod.descricao AS causa_cid10_desc,"

    db_path = gold_path / "obitos.duckdb"
    con = duckdb.connect(str(db_path))

    con.execute(f"""
        CREATE OR REPLACE VIEW v_obitos_completo AS
        SELECT
            o.ORIGEM AS origem,
            o.TIPOBITO AS tipo_obito,
            lt.descricao AS tipo_obito_desc,
            o.dt_obito,
            year(o.dt_obito) AS ano,
            o.HORAOBITO AS hora_obito,
            o."NATURAL" AS natural,
            o.CODMUNNATU AS cod_mun_nascimento,
            o.dt_nascimento,
            o.IDADE AS idade,
            o.SEXO AS sexo,
            ls.descricao AS sexo_desc,
            o.RACACOR AS racacor,
            lr.descricao AS racacor_desc,
            o.ESTCIV AS estciv,
            le.descricao AS estciv_desc,
            o.ESC AS esc,
            o.ESC2010 AS esc_2010,
            o.OCUP AS ocup,
            o.CODMUNRES AS cod_mun_residencia,
            m_res.geocodigo AS geocodigo_residencia,
            m_res.municipio AS municipio_residencia,
            m_res.uf AS uf_residencia,
            o.LOCOCOR AS loc_ocorrencia,
            loc.descricao AS local_ocorrencia_desc,
            o.CODMUNOCOR AS cod_mun_ocorrencia,
            m_ocor.municipio AS municipio_ocorrencia,
            m_ocor.uf AS uf_ocorrencia,
            TRIM(COALESCE(CAST(o.CAUSABAS AS VARCHAR), '')) AS causa_basica,
            {cid_select}
            o.CIRCOBITO AS circ_obito,
            lc.descricao AS circunstancia_desc,
            o.PESO AS peso,
            o.SEMAGESTAC AS sem_gestacao,
            o.GESTACAO AS gestacao,
            o.PARTO AS parto,
            o.CONTADOR AS contador
        FROM read_parquet('{o}') o
        LEFT JOIN read_parquet('{m}') m_res
            ON m_res.codigo = TRIM(CAST(o.CODMUNRES AS VARCHAR))
        LEFT JOIN read_parquet('{m}') m_ocor
            ON m_ocor.codigo = TRIM(CAST(o.CODMUNOCOR AS VARCHAR))
        LEFT JOIN read_parquet('{ls_path}') ls
            ON ls.codigo = CAST(COALESCE(TRY_CAST(o.SEXO AS INT), 0) AS VARCHAR)
        LEFT JOIN read_parquet('{lr_path}') lr
            ON lr.codigo = COALESCE(CAST(o.RACACOR AS VARCHAR), '9')
        LEFT JOIN read_parquet('{lc_path}') lc
            ON lc.codigo = COALESCE(CAST(o.CIRCOBITO AS VARCHAR), '0')
        LEFT JOIN read_parquet('{lt_path}') lt
            ON lt.codigo = COALESCE(CAST(o.TIPOBITO AS VARCHAR), '0')
        LEFT JOIN read_parquet('{loc_path}') loc
            ON loc.codigo = COALESCE(CAST(o.LOCOCOR AS VARCHAR), '0')
        LEFT JOIN read_parquet('{le_path}') le
            ON le.codigo = COALESCE(CAST(o.ESTCIV AS VARCHAR), '9')
        LEFT JOIN read_parquet('{lcid_path}') lcid
            ON lcid.letra = UPPER(SUBSTR(TRIM(COALESCE(CAST(o.CAUSABAS AS VARCHAR), '')), 1, 1))
        LEFT JOIN read_parquet('{lcod_path}') lcod
            ON lcod.codigo = TRIM(COALESCE(CAST(o.CAUSABAS AS VARCHAR), ''))
        {cid_join}
    """)

    # View para análise: mesma base + faixa_etaria calculada em SQL (evita carregar tudo em memória)
    con.execute("""
        CREATE OR REPLACE VIEW v_obitos_analise AS
        SELECT
            v.origem, v.tipo_obito, v.tipo_obito_desc, v.dt_obito, v.ano, v.hora_obito, v.natural,
            v.cod_mun_nascimento, v.dt_nascimento, v.idade, v.sexo, v.sexo_desc, v.racacor, v.racacor_desc,
            v.estciv, v.estciv_desc, v.esc, v.esc_2010, v.ocup,             v.cod_mun_residencia, v.geocodigo_residencia, v.municipio_residencia,
            v.uf_residencia, v.loc_ocorrencia, v.local_ocorrencia_desc, v.cod_mun_ocorrencia, v.municipio_ocorrencia,
            v.uf_ocorrencia, v.causa_basica, v.causa_cid10_capitulo_desc, v.causa_cid10_desc, v.circ_obito, v.circunstancia_desc,
            v.peso, v.sem_gestacao, v.gestacao, v.parto, v.contador,
            CASE
                WHEN SUBSTR(v.ida, 1, 1) IN ('0','9') OR LENGTH(v.ida) < 3 THEN 'Ignorado'
                WHEN SUBSTR(v.ida, 1, 1) IN ('1','2','3') THEN '< 1 ano'
                WHEN SUBSTR(v.ida, 1, 1) = '4' THEN
                    CASE
                        WHEN COALESCE(TRY_CAST(SUBSTR(v.ida, 2, 2) AS INT), 0) < 1 THEN '< 1 ano'
                        WHEN COALESCE(TRY_CAST(SUBSTR(v.ida, 2, 2) AS INT), 0) < 5 THEN '1-4 anos'
                        WHEN COALESCE(TRY_CAST(SUBSTR(v.ida, 2, 2) AS INT), 0) < 10 THEN '5-9 anos'
                        WHEN COALESCE(TRY_CAST(SUBSTR(v.ida, 2, 2) AS INT), 0) < 15 THEN '10-14 anos'
                        WHEN COALESCE(TRY_CAST(SUBSTR(v.ida, 2, 2) AS INT), 0) < 20 THEN '15-19 anos'
                        WHEN COALESCE(TRY_CAST(SUBSTR(v.ida, 2, 2) AS INT), 0) < 30 THEN '20-29 anos'
                        WHEN COALESCE(TRY_CAST(SUBSTR(v.ida, 2, 2) AS INT), 0) < 40 THEN '30-39 anos'
                        WHEN COALESCE(TRY_CAST(SUBSTR(v.ida, 2, 2) AS INT), 0) < 50 THEN '40-49 anos'
                        WHEN COALESCE(TRY_CAST(SUBSTR(v.ida, 2, 2) AS INT), 0) < 60 THEN '50-59 anos'
                        WHEN COALESCE(TRY_CAST(SUBSTR(v.ida, 2, 2) AS INT), 0) < 70 THEN '60-69 anos'
                        WHEN COALESCE(TRY_CAST(SUBSTR(v.ida, 2, 2) AS INT), 0) < 80 THEN '70-79 anos'
                        ELSE '80+ anos'
                    END
                WHEN SUBSTR(v.ida, 1, 1) = '5' THEN
                    CASE
                        WHEN (100 + COALESCE(TRY_CAST(SUBSTR(v.ida, 2, 2) AS INT), 0)) < 5 THEN '1-4 anos'
                        WHEN (100 + COALESCE(TRY_CAST(SUBSTR(v.ida, 2, 2) AS INT), 0)) < 10 THEN '5-9 anos'
                        WHEN (100 + COALESCE(TRY_CAST(SUBSTR(v.ida, 2, 2) AS INT), 0)) < 15 THEN '10-14 anos'
                        WHEN (100 + COALESCE(TRY_CAST(SUBSTR(v.ida, 2, 2) AS INT), 0)) < 20 THEN '15-19 anos'
                        WHEN (100 + COALESCE(TRY_CAST(SUBSTR(v.ida, 2, 2) AS INT), 0)) < 30 THEN '20-29 anos'
                        WHEN (100 + COALESCE(TRY_CAST(SUBSTR(v.ida, 2, 2) AS INT), 0)) < 40 THEN '30-39 anos'
                        WHEN (100 + COALESCE(TRY_CAST(SUBSTR(v.ida, 2, 2) AS INT), 0)) < 50 THEN '40-49 anos'
                        WHEN (100 + COALESCE(TRY_CAST(SUBSTR(v.ida, 2, 2) AS INT), 0)) < 60 THEN '50-59 anos'
                        WHEN (100 + COALESCE(TRY_CAST(SUBSTR(v.ida, 2, 2) AS INT), 0)) < 70 THEN '60-69 anos'
                        WHEN (100 + COALESCE(TRY_CAST(SUBSTR(v.ida, 2, 2) AS INT), 0)) < 80 THEN '70-79 anos'
                        ELSE '80+ anos'
                    END
                ELSE 'Ignorado'
            END AS faixa_etaria
        FROM (
            SELECT *, LPAD(TRIM(COALESCE(CAST(idade AS VARCHAR), '')), 3, '0') AS ida
            FROM v_obitos_completo
        ) v
    """)

    con.close()
    return {"duckdb": str(db_path), "view": "v_obitos_completo"}
