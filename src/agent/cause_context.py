"""
Contexto de causas/doenças para o agente: mapeia termos da pergunta para filtros SQL.
Facilita que perguntas como "óbitos por dengue" ou "mortes por COVID" gerem SQL correta.
"""
import re
from typing import List, Tuple

# (termo ou palavra-chave na pergunta, prefixo CID-10 ou código, descrição para ILIKE)
# Ordenar por especificidade (código mais específico primeiro quando vários)
DOENCAS_CONHECIDAS: List[Tuple[str, str, str]] = [
    ("covid", "U07.1", "COVID-19"),
    ("covid-19", "U07.1", "COVID-19"),
    ("sars-cov", "U07.1", "COVID-19"),
    ("dengue", "A90", "Dengue"),
    ("dengue hemorrágica", "A91", "Dengue hemorrágico"),
    ("infarto", "I21", "Infarto agudo do miocárdio"),
    ("iam", "I21", "Infarto"),
    ("avc", "I64", "Acidente vascular cerebral"),
    ("avc isquêmico", "I63", "Oclusão"),
    ("acidente vascular", "I64", "Acidente vascular"),
    ("pneumonia", "J18", "Pneumonia"),
    ("gripe", "J11", "Influenza"),
    ("influenza", "J11", "Influenza"),
    ("diabetes", "E14", "Diabetes"),
    ("hipertensão", "I10", "Hipertensão"),
    ("câncer", "C80", "Neoplasia"),
    ("cancer", "C80", "Neoplasia"),
    ("neoplasia", "C80", "Neoplasia"),
    ("acidente de trânsito", "V89", "Acidente de transporte"),
    ("acidente trânsito", "V89", "Acidente de transporte"),
    ("transito", "V89", "Acidente de transporte"),
    ("trânsito", "V89", "Acidente de transporte"),
    ("homicídio", "X85", "Agressão"),
    ("homicidio", "X85", "Agressão"),
    ("suicídio", "X60", "Autolesão"),
    ("suicidio", "X60", "Autolesão"),
    ("insuficiência renal", "N18", "Insuficiência renal"),
    ("irc", "N18", "Insuficiência renal"),
    ("doença renal", "N18", "Insuficiência renal"),
    ("doença de chagas", "B57", "Chagas"),
    ("chagas", "B57", "Chagas"),
    ("tuberculose", "A15", "Tuberculose"),
    ("tbc", "A15", "Tuberculose"),
    ("alzheimer", "G30", "Alzheimer"),
    ("demência", "F03", "Demência"),
    ("demencia", "F03", "Demência"),
    ("doença respiratória", "J96", "Insuficiência respiratória"),
    ("respiratória crônica", "J44", "DPOC"),
    ("dpoc", "J44", "DPOC"),
    ("asma", "J45", "Asma"),
    ("septicemia", "A41", "Septicemia"),
    ("sepse", "A41", "Septicemia"),
]


def get_cause_context_for_plan(pergunta: str) -> str:
    """
    A partir da pergunta, detecta menções a doenças/causas conhecidas e retorna
    um texto para injetar no contexto do planejador (ex.: "Para filtro por causa use ...").
    Assim a SQL usa causa_basica ou causa_cid10_desc de forma consistente.
    """
    if not pergunta or len(pergunta.strip()) < 3:
        return ""
    low = pergunta.lower().strip()
    # Remover acentos para match (simplificado)
    low_norm = low
    for a, b in [("á", "a"), ("é", "e"), ("í", "i"), ("ó", "o"), ("ú", "u"), ("â", "a"), ("ê", "e"), ("ô", "o"), ("ã", "a"), ("õ", "o")]:
        low_norm = low_norm.replace(a, b)
    hints = []
    seen_codes = set()
    # Ordenar por tamanho do termo (mais longo primeiro) para "dengue hemorrágica" antes de "dengue"
    for termo, codigo, desc in sorted(DOENCAS_CONHECIDAS, key=lambda x: -len(x[0])):
        termo_norm = termo
        for a, b in [("á", "a"), ("é", "e"), ("í", "i"), ("ó", "o"), ("ú", "u"), ("â", "a"), ("ê", "e"), ("ô", "o"), ("ã", "a"), ("õ", "o")]:
            termo_norm = termo_norm.replace(a, b)
        if termo_norm in low_norm or termo in low:
            if codigo not in seen_codes:
                seen_codes.add(codigo)
                # Sugestão: usar causa_basica LIKE 'codigo%' OU causa_cid10_desc ILIKE '%desc%'
                hints.append(f"causa_basica LIKE '{codigo}%' OU causa_cid10_desc ILIKE '%{desc}%'")
    if not hints:
        return ""
    return (
        "Filtro por causa/doença (use na cláusula WHERE): "
        + " OU ".join(hints)
        + ". Prefira causa_basica quando o código for exato (ex.: LIKE 'A90%')."
    )
