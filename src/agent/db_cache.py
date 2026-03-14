"""
Uma única conexão DuckDB para aquecer todos os caches de metadata do agente.
Evita consultas repetitivas e múltiplas conexões (schema, rich_schema, municípios).
"""
import logging
import threading
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
GOLD_DB = PROJECT_ROOT / "data" / "SIM" / "gold" / "obitos.duckdb"

_warm_lock = threading.Lock()
_warm_done = False

logger = logging.getLogger(__name__)


def warm_all_caches() -> None:
    """
    Abre uma única conexão, preenche os caches de schema, schema_enricher e municipality,
    e fecha a conexão. Só executa uma vez por processo; chamadas seguintes são no-op.
    """
    global _warm_done
    with _warm_lock:
        if _warm_done:
            return
        if not GOLD_DB.is_file():
            logger.debug("db_cache: gold DB não encontrado, skip warm")
            _warm_done = True
            return
        try:
            import duckdb
            con = duckdb.connect(str(GOLD_DB), read_only=True)
            try:
                _warm_schema(con)
                _warm_rich_schema(con)
                _warm_municipalities(con)
                _warm_done = True
                logger.debug("db_cache: caches aquecidos com uma conexão")
            finally:
                con.close()
        except Exception as e:
            logger.warning("db_cache: falha ao aquecer caches: %s", e)
            _warm_done = True


def _warm_schema(con) -> None:
    """Preenche cache do schema (DESCRIBE v_obitos_completo)."""
    from src.agent import schema as schema_mod
    if schema_mod._SCHEMA_FROM_VIEW_CACHE is not None:
        return
    try:
        desc = con.execute("DESCRIBE v_obitos_completo").fetchall()
        if not desc:
            schema_mod._SCHEMA_FROM_VIEW_CACHE = schema_mod._fallback_schema_views()
            return
        col_lines = []
        for r in desc:
            name = str(r[0]) if r else ""
            dtype = str(r[1]) if len(r) > 1 else ""
            if name:
                col_lines.append(f"   {name} ({dtype})")
        cols_text = "\n".join(col_lines)
        schema_mod._SCHEMA_FROM_VIEW_CACHE = (
            "Você gera apenas uma query SQL DuckDB. Tabela disponível (read-only):\n\n"
            "v_obitos_completo - view principal (use esta). Colunas exatamente como na base:\n"
            f"{cols_text}\n\n"
            "Regras: dt_obito é a data do óbito; dt_obito_mes é o primeiro dia do mês (use para agregações mensais: GROUP BY dt_obito_mes ou strftime(dt_obito_mes, '%Y-%m')); ano já existe. "
            "NUNCA use '%w' (dia da semana). Para filtro por município use EXATAMENTE os valores do contexto. "
            "Para causa use EXATAMENTE os códigos ou causa_cid10_capitulo_desc do contexto; não use ILIKE em texto livre nem invente códigos. "
            "Apenas SELECT (ou WITH ... SELECT). Retorne JSON com chave \"sql\"."
        )
    except Exception:
        schema_mod._SCHEMA_FROM_VIEW_CACHE = schema_mod._fallback_schema_views()


def _warm_rich_schema(con) -> None:
    """Preenche cache do schema enriquecido (uma conexão, várias consultas)."""
    from src.agent import schema_enricher as enricher_mod
    if enricher_mod._rich_schema_cache is not None:
        return
    partes = []
    try:
        # Colunas
        try:
            desc = con.execute("DESCRIBE v_obitos_completo").fetchall()
            if desc:
                col_names = [str(r[0]) for r in desc]
                partes.append("Tabela principal para consultas: v_obitos_completo")
                partes.append(f"Colunas: {', '.join(col_names)}")
        except Exception:
            partes.append(
                "Colunas: dt_obito, ano, sexo_desc, uf_residencia, municipio_residencia, "
                "causa_basica, causa_cid10_capitulo_desc, causa_cid10_desc, circunstancia_desc, "
                "local_ocorrencia_desc, municipio_ocorrencia, uf_ocorrencia, tipo_obito_desc, racacor_desc, estciv_desc"
            )

        # Uma query para min_ano, max_ano, count
        row = con.execute(
            "SELECT MIN(ano) AS a, MAX(ano) AS b, COUNT(*) AS c FROM v_obitos_completo"
        ).fetchone()
        if row and row[0] is not None:
            partes.append(f"\nAnos disponíveis na base: {row[0]} a {row[1]}")
        if row and row[2] is not None:
            partes.append(f"Total de óbitos na base: {row[2]:,}")

        # Amostra 5 linhas
        try:
            amostra = con.execute(
                "SELECT ano, sexo_desc, uf_residencia, municipio_residencia, causa_basica, causa_cid10_capitulo_desc, causa_cid10_desc "
                "FROM v_obitos_completo LIMIT 5"
            ).fetchall()
            col_amostra = ["ano", "sexo_desc", "uf_residencia", "municipio_residencia", "causa_basica", "causa_cid10_capitulo_desc", "causa_cid10_desc"]
            if amostra:
                partes.append("\nAmostra (5 linhas) — use para ver formato dos valores:")
                partes.append("  " + " | ".join(col_amostra))
                for row in amostra:
                    partes.append("  " + " | ".join(str(v) if v is not None else "" for v in row))
        except Exception:
            pass

        # causa_basica (top 8)
        causas = con.execute(
            "SELECT causa_basica, COUNT(*) AS n FROM v_obitos_completo "
            "WHERE causa_basica IS NOT NULL AND TRIM(CAST(causa_basica AS VARCHAR)) != '' "
            "GROUP BY causa_basica ORDER BY n DESC LIMIT 8"
        ).fetchall()
        if causas:
            ex = ", ".join(f"'{str(r[0]).strip()}'" for r in causas)
            partes.append(
                f"Coluna causa_basica — formato dos valores (exemplos reais): {ex}. "
                "Use sempre este formato exato nos filtros (ex.: WHERE causa_basica = 'I219' ou LIKE 'I21%')."
            )

        # municipio_residencia (top 6)
        munis = con.execute(
            "SELECT municipio_residencia FROM v_obitos_completo "
            "WHERE municipio_residencia IS NOT NULL AND TRIM(CAST(municipio_residencia AS VARCHAR)) != '' "
            "GROUP BY municipio_residencia ORDER BY COUNT(*) DESC LIMIT 6"
        ).fetchall()
        if munis:
            ex = ", ".join(f"'{str(r[0]).strip()}'" for r in munis)
            partes.append(
                f"Coluna municipio_residencia — exemplos de valores: {ex}. "
                "Use EXATAMENTE estes nomes (com acentos e maiúsculas corretas) nos filtros."
            )

        # uf_residencia
        ufs = con.execute(
            "SELECT DISTINCT uf_residencia FROM v_obitos_completo "
            "WHERE uf_residencia IS NOT NULL AND TRIM(CAST(uf_residencia AS VARCHAR)) != '' "
            "ORDER BY uf_residencia LIMIT 30"
        ).fetchall()
        if ufs:
            ex = ", ".join(f"'{str(r[0]).strip()}'" for r in ufs)
            partes.append(f"Coluna uf_residencia — valores possíveis (siglas): {ex}.")

        # sexo_desc
        sexos = con.execute(
            "SELECT DISTINCT sexo_desc FROM v_obitos_completo WHERE sexo_desc IS NOT NULL"
        ).fetchall()
        if sexos:
            ex = ", ".join(f"'{str(r[0]).strip()}'" for r in sexos)
            partes.append(f"Coluna sexo_desc — valores possíveis: {ex}.")

        # causa_cid10_capitulo_desc
        capitulos = con.execute(
            "SELECT DISTINCT causa_cid10_capitulo_desc FROM v_obitos_completo "
            "WHERE causa_cid10_capitulo_desc IS NOT NULL AND TRIM(COALESCE(CAST(causa_cid10_capitulo_desc AS VARCHAR), '')) != '' "
            "ORDER BY 1"
        ).fetchall()
        if capitulos:
            ex = ", ".join(f"'{str(r[0]).strip().replace(chr(39), chr(39)+chr(39))}'" for r in capitulos)
            partes.append(
                f"Coluna causa_cid10_capitulo_desc — valores possíveis na base (use EXATAMENTE em IN (...)): {ex}."
            )

        enricher_mod._rich_schema_cache = "\n".join(partes)
    except Exception as e:
        enricher_mod._rich_schema_cache = f"(schema enriquecido indisponível: {e})"


def _warm_municipalities(con) -> None:
    """Preenche caches de municípios (lista e lista com UF)."""
    from src.agent import municipality as mun_mod
    if mun_mod._municipios_cache is not None and mun_mod._municipios_uf_cache is not None:
        return
    try:
        if mun_mod._municipios_cache is None:
            rows = con.execute(
                "SELECT DISTINCT municipio_residencia FROM v_obitos_completo "
                "WHERE municipio_residencia IS NOT NULL AND TRIM(CAST(municipio_residencia AS VARCHAR)) != ''"
            ).fetchall()
            mun_mod._municipios_cache = [str(r[0]).strip() for r in rows if r[0]]
        if mun_mod._municipios_uf_cache is None:
            rows = con.execute(
                "SELECT DISTINCT municipio_residencia, uf_residencia FROM v_obitos_completo "
                "WHERE municipio_residencia IS NOT NULL AND TRIM(CAST(municipio_residencia AS VARCHAR)) != '' "
                "AND uf_residencia IS NOT NULL"
            ).fetchall()
            mun_mod._municipios_uf_cache = [(str(r[0]).strip(), str(r[1]).strip()) for r in rows if r[0] and r[1]]
    except Exception:
        if mun_mod._municipios_cache is None:
            mun_mod._municipios_cache = []
        if mun_mod._municipios_uf_cache is None:
            mun_mod._municipios_uf_cache = []
