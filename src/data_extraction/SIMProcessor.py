"""
Processamento dos dados SIM (Declaração de Óbito) para camada silver.
Lê parquets da pasta raw, aplica tratamento SQL e grava em silver, deixando
dados prontos para consumo via DuckDB.
"""
from pathlib import Path
from typing import Callable, Optional

import duckdb

# Caminho do script SQL de tratamento (relativo a este arquivo)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
TRATAMENTO_SQL = Path(__file__).parent / "tratamento.sql"
DEFAULT_RAW = PROJECT_ROOT / "data" / "SIM" / "raw"
DEFAULT_SILVER = PROJECT_ROOT / "data" / "SIM" / "silver"


class SIMProcessor:
    """
    Processa dados SIM de raw para silver usando DuckDB.
    Gera parquet tratado e banco DuckDB para consumo direto.
    """

    def __init__(
        self,
        raw_path: Optional[str | Path] = None,
        silver_path: Optional[str | Path] = None,
    ):
        self.raw_path = Path(raw_path or DEFAULT_RAW)
        self.silver_path = Path(silver_path or DEFAULT_SILVER)
        self.silver_path.mkdir(parents=True, exist_ok=True)
        self._output_parquet = self.silver_path / "obitos.parquet"
        self._output_db = self.silver_path / "obitos.duckdb"

    def _get_parquet_pattern(self) -> str:
        """Retorna padrão glob para ler todos os parquets (arquivos e diretórios)."""
        path_str = str(self.raw_path.resolve()).replace("\\", "/")
        return f"{path_str}/**/*.parquet"

    @staticmethod
    def _raw_columns(con) -> set:
        """Retorna o conjunto de nomes de colunas da view 'obitos' (schema do raw)."""
        try:
            rows = con.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'main' AND table_name = 'obitos'
                ORDER BY ordinal_position
            """).fetchall()
            return {r[0] for r in rows}
        except Exception:
            return set()

    def _build_treatment_select(self, raw_columns: set) -> str:
        """
        Monta o SELECT do tratamento para a silver.
        Usa apenas colunas que existem no raw; colunas ausentes (layout antigo/outro) viram NULL.
        """
        def col(name: str, alias: str | None = None) -> str:
            out = alias or name
            ref = f'"{name}"' if name == "NATURAL" else name  # palavra reservada
            if name in raw_columns:
                if alias:
                    return f"{ref} AS {out}"
                return ref
            return f'NULL AS "{out}"' if name == "NATURAL" else f"NULL AS {out}"

        # Datas: conversão ddmmaaaa -> DATE
        if "DTOBITO" in raw_columns:
            dt_obito = "TRY_STRPTIME(DTOBITO, '%d%m%Y')::DATE AS dt_obito"
        else:
            dt_obito = "NULL::DATE AS dt_obito"
        if "DTNASC" in raw_columns:
            dt_nasc = "TRY_STRPTIME(DTNASC, '%d%m%Y')::DATE AS dt_nascimento"
        else:
            dt_nasc = "NULL::DATE AS dt_nascimento"

        select_parts = [
            col("ORIGEM"),
            col("TIPOBITO"),
            dt_obito,
            col("HORAOBITO"),
            col("NATURAL"),
            col("CODMUNNATU"),
            dt_nasc,
            col("IDADE"),
            col("SEXO"),
            col("RACACOR"),
            col("ESTCIV"),
            col("ESC"),
            col("ESC2010"),
            col("OCUP"),
            col("CODMUNRES"),
            col("LOCOCOR"),
            col("CODMUNOCOR"),
            col("CAUSABAS"),
            col("CIRCOBITO"),
            col("PESO"),
            col("SEMAGESTAC"),
            col("GESTACAO"),
            col("PARTO"),
            col("CONTADOR"),
        ]
        return ",\n        ".join(select_parts)

    def _build_treatment_where(self, raw_columns: set) -> str:
        """Cláusula WHERE: filtra data inválida quando DTOBITO existe."""
        if "DTOBITO" in raw_columns:
            return "WHERE TRY_STRPTIME(DTOBITO, '%d%m%Y') IS NOT NULL"
        return "WHERE 1=1"

    def process(
        self,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> dict:
        """
        Executa o processamento: raw -> tratamento SQL -> silver (parquet + duckdb).
        Retorna dict com 'parquet', 'duckdb', 'total_registros'.
        """
        parquet_pattern = self._get_parquet_pattern()

        def _log(msg: str):
            if progress_callback:
                progress_callback(msg)

        con = duckdb.connect()

        # Cria view com dados raw (pode misturar layouts antigos/novos)
        _log("Carregando dados raw...")
        con.execute(f"""
            CREATE OR REPLACE VIEW obitos AS
            SELECT * FROM read_parquet('{parquet_pattern}')
        """)

        # Monta tratamento conforme colunas existentes (compatível com layout antigo e novo)
        raw_cols = self._raw_columns(con)
        _log("Aplicando tratamento...")
        select_clause = self._build_treatment_select(raw_cols)
        where_clause = self._build_treatment_where(raw_cols)
        output_quoted = str(self._output_parquet).replace("'", "''")
        copy_sql = f"""
            COPY (
                SELECT {select_clause}
                FROM obitos
                {where_clause}
            ) TO '{output_quoted}' (FORMAT PARQUET, COMPRESSION 'zstd')
        """
        con.execute(copy_sql)

        # Contagem
        total = con.execute(
            f"SELECT count(*) FROM read_parquet('{self._output_parquet}')"
        ).fetchone()[0]

        con.close()
        _log("Gerando banco DuckDB para consumo...")

        # Cria banco DuckDB com view apontando para o parquet (pronto para consumo)
        self._create_duckdb_catalog()

        return {
            "parquet": str(self._output_parquet),
            "duckdb": str(self._output_db),
            "total_registros": total,
        }

    def _create_duckdb_catalog(self) -> None:
        """
        Cria/atualiza arquivo DuckDB com views para consumo.
        - obitos: dados tratados (parquet)
        - municipios: código IBGE → nome/UF (para JOIN)
        """
        from src.data_extraction.municipios import build_municipios_table

        build_municipios_table(self.silver_path / "municipios.parquet")

        db_con = duckdb.connect(str(self._output_db))
        parquet_abs = str(self._output_parquet.resolve()).replace("\\", "/")
        mun_abs = str((self.silver_path / "municipios.parquet").resolve()).replace("\\", "/")

        db_con.execute(f"""
            CREATE OR REPLACE VIEW obitos AS
            SELECT * FROM read_parquet('{parquet_abs}');

            CREATE OR REPLACE VIEW municipios AS
            SELECT * FROM read_parquet('{mun_abs}');
        """)
        db_con.close()

    def update_catalog(self) -> None:
        """
        Atualiza apenas o catálogo DuckDB (views, municipios).
        Use quando obitos.parquet já existe e você quer recriar as views.
        """
        if not self._output_parquet.exists():
            raise FileNotFoundError(
                f"Arquivo não encontrado: {self._output_parquet}. "
                "Execute process() antes."
            )
        self._create_duckdb_catalog()

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """
        Retorna conexão DuckDB apontando para o catálogo em silver.
        Uso: con = processor.get_connection()
             df = con.execute("SELECT * FROM obitos LIMIT 10").fetchdf()
        """
        if not self._output_db.exists():
            raise FileNotFoundError(
                f"Banco DuckDB não encontrado em {self._output_db}. "
                "Execute process() antes."
            )
        return duckdb.connect(str(self._output_db), read_only=True)
