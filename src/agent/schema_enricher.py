"""
Schema enriquecido: consulta apenas a view v_obitos_completo para obter colunas e valores reais.
Todo dado injetado no agente vem da própria view (DESCRIBE, DISTINCT, amostras) para garantir alinhamento.
"""
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
GOLD_DB = PROJECT_ROOT / "data" / "SIM" / "gold" / "obitos.duckdb"

_rich_schema_cache: Optional[str] = None


def build_rich_schema() -> str:
    """
    Retorna exemplos reais de valores por coluna (cache). Se o cache estiver vazio,
    usa warm_all_caches() para preencher com uma única conexão (evita consultas repetitivas).
    """
    global _rich_schema_cache
    if _rich_schema_cache is not None:
        return _rich_schema_cache
    if GOLD_DB.is_file():
        from src.agent.db_cache import warm_all_caches
        warm_all_caches()
    if _rich_schema_cache is not None:
        return _rich_schema_cache
    return "(schema enriquecido indisponível)"
