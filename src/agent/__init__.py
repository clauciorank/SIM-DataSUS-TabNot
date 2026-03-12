"""
Agente de IA para consultas sobre dados oficiais (SIM).
Fluxo planejar-executar-avaliar com LangGraph; resolução fuzzy de municípios.
"""
from src.agent.graph import run_agent
from src.agent.municipality import resolve_municipality, get_municipalities_for_context

__all__ = ["run_agent", "resolve_municipality", "get_municipalities_for_context"]
