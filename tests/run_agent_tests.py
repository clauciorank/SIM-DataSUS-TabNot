#!/usr/bin/env python3
"""
Executa os casos de teste do agente (tests/agent_cases.yaml), chama run_agent,
aplica expectativas (expect_sql_contains, expect_resposta_contains, must_not_sql, expect_sql_empty)
e gera relatório no console e opcionalmente em tests/reports/.
Requer PyYAML e LLM configurado (Configurações do app).
"""
from pathlib import Path
import sys
from datetime import datetime
from typing import List, Tuple

# Raiz do projeto (pai de tests/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

CASES_PATH = Path(__file__).resolve().parent / "agent_cases.yaml"
CORRECTIONS_PATH = Path(__file__).resolve().parent / "agent_corrections.yaml"
REPORTS_DIR = Path(__file__).resolve().parent / "reports"


def load_yaml(path: Path) -> dict:
    try:
        import yaml
        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except ImportError:
        raise SystemExit("Instale PyYAML: pip install pyyaml")
    except Exception as e:
        print(f"Erro ao carregar {path}: {e}")
        return {}


def load_cases() -> list:
    data = load_yaml(CASES_PATH)
    cases = data.get("cases") or data.get("case") or []
    if not isinstance(cases, list):
        cases = [cases] if cases else []
    corrections = {}
    if CORRECTIONS_PATH.exists():
        corrections = load_yaml(CORRECTIONS_PATH) or {}
    # Merge correções por id (correção sobrescreve)
    for c in cases:
        cid = c.get("id")
        if cid and cid in corrections:
            for k, v in corrections[cid].items():
                c[k] = v
    return cases


def run_one(pergunta: str, llm_config: dict) -> dict:
    from src.agent.graph import run_agent
    return run_agent(pergunta, llm_config or {})


def check_expectations(case: dict, out: dict) -> Tuple[bool, List[str]]:
    """Retorna (passou, lista de motivos de falha)."""
    failures = []
    sql = (out.get("sql_planejada") or "").strip()
    resposta = (out.get("resposta_final") or "").strip()

    if case.get("expect_sql_empty"):
        if sql:
            failures.append("esperado sql vazio, mas há SQL")
    else:
        for sub in case.get("expect_sql_contains") or []:
            if sub not in sql:
                failures.append(f"SQL não contém: {sub!r}")

    for sub in case.get("expect_resposta_contains") or []:
        if sub not in resposta:
            failures.append(f"resposta não contém: {sub!r}")

    for forbidden in case.get("must_not_sql") or []:
        if forbidden in sql:
            failures.append(f"SQL não deve conter: {forbidden!r}")

    return (len(failures) == 0, failures)


def main() -> None:
    import argparse
    p = argparse.ArgumentParser(description="Roda testes do agente (agent_cases.yaml)")
    p.add_argument("--categoria", "-c", type=str, default="", help="Filtrar só esta categoria (ex: guardrail)")
    args = p.parse_args()

    cases = load_cases()
    if args.categoria:
        cases = [c for c in cases if (c.get("categoria") or "").lower() == args.categoria.lower()]
        print(f"Filtrado por categoria: {args.categoria!r} ({len(cases)} casos)")
    if not cases:
        print("Nenhum caso em", CASES_PATH)
        return

    try:
        from src.config.persistence import load_llm_config
        llm_config = load_llm_config()
    except Exception as e:
        print("Aviso: não foi possível carregar llm_config:", e)
        llm_config = {}

    passed = 0
    failed = 0
    results = []

    for i, case in enumerate(cases):
        cid = case.get("id") or f"case_{i}"
        pergunta = (case.get("pergunta") or "").strip()
        categoria = case.get("categoria", "")
        if not pergunta:
            continue
        print(f"[{i+1}/{len(cases)}] {cid} ...", end=" ", flush=True)
        try:
            out = run_one(pergunta, llm_config)
        except Exception as e:
            print("ERRO", str(e)[:60])
            failed += 1
            results.append({
                "id": cid, "categoria": categoria, "pergunta": pergunta,
                "passou": False, "erro": str(e), "sql": "", "resposta": "",
            })
            continue
        resp = out.get("resposta_final", "")
        sql = out.get("sql_planejada", "")
        ok, failure_reasons = check_expectations(case, out)
        if ok:
            print("OK")
            passed += 1
        else:
            print("FALHOU:", "; ".join(failure_reasons)[:80])
            failed += 1
        results.append({
            "id": cid, "categoria": categoria, "pergunta": pergunta,
            "passou": ok, "falhas": failure_reasons, "sql": sql[:500], "resposta": resp[:300],
        })

    print("\n--- Resumo ---")
    print(f"Passaram: {passed} | Falharam: {failed} | Total: {len(cases)}")

    # Relatório em arquivo
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = REPORTS_DIR / f"agent_report_{ts}.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("# Relatório de testes do agente\n\n")
        f.write(f"Data: {datetime.now().isoformat()}\n\n")
        f.write(f"Passaram: {passed} | Falharam: {failed} | Total: {len(cases)}\n\n")
        f.write("---\n\n")
        for r in results:
            status = "OK" if r["passou"] else "FALHOU"
            f.write(f"## [{status}] {r['id']} ({r['categoria']})\n\n")
            f.write(f"**Pergunta:** {r['pergunta']}\n\n")
            if not r["passou"] and r.get("falhas"):
                f.write(f"**Falhas:** {', '.join(r['falhas'])}\n\n")
            f.write(f"**Resposta (trecho):** {r.get('resposta', '')[:400]}...\n\n")
            if r.get("sql"):
                f.write(f"**SQL (trecho):**\n```sql\n{r['sql'][:600]}\n```\n\n")
            f.write("---\n\n")
    print("Relatório salvo em", report_path)


if __name__ == "__main__":
    main()
