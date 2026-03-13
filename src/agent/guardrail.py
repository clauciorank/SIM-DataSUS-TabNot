"""
Guardrail: rejeita perguntas fora do tema (dados de óbitos/mortalidade SIM).
Evita consumo de tokens e API com perguntas absurdas ou off-topic.
"""
import re
from typing import Optional

# Termos que indicam que a pergunta é sobre óbitos/mortalidade/dados SIM
TOPIC_TERMS = [
    # Óbito / morte / falecimento
    "óbito", "óbitos", "obito", "obitos", "morte", "mortes", "mortais",
    "causa de morte", "causa básica", "causa basica", "mortalidade",
    "falecimento", "falecimentos", "falecidos", "óbitos por", "mortes por",
    "total de óbitos", "total de mortes", "número de óbitos", "numero de obitos",
    "quantidade de óbitos", "quantidade de mortes", "contagem de óbitos",
    "óbito fetal", "obito fetal", "óbitos fetais", "natimorto", "natimortos",
    "declaração de óbito", "declaracao de obito", "atestado de óbito",
    # SIM / sistema / Datasus / saúde
    "dados sim", "sim (sistema)", "sistema de informação", "sistema de informações sobre mortalidade",
    "datasus", "datasus sim", "ministerio da saúde", "ministério da saúde",
    "saúde pública", "saude publica", "vigilância", "vigilancia",
    "epidemiologia", "dados de mortalidade", "registro de óbitos",
    "informações de mortalidade", "informacoes de mortalidade",
    # Doença / causa / CID
    "doença", "doenças", "doenca", "doencas", "causa", "causas",
    "cid", "cid-10", "cid10", "capítulo cid", "capitulo cid", "capítulos cid",
    "categoria cid", "código cid", "codigo cid", "causa básica de morte",
    "grupo de causas", "capítulo de causas", "lista de causas",
    "câncer", "cancer", "neoplasia", "infarto", "AVC", "acidente vascular",
    "covid", "covid-19", "sars-cov", "doença respiratória", "doenca respiratoria",
    "diabetes", "homicídio", "homicidio", "suicídio", "suicidio",
    "acidente", "acidentes", "trânsito", "transito", "violência", "violencia",
    "doença infecciosa", "doenca infecciosa", "tuberculose", "AIDS", "HIV",
    # Local / geografia
    "município", "municipio", "municípios", "municipios", "municipal",
    "estado", "estados", "UF", "unidade federativa", "região", "regiao", "regiões",
    "residência", "residencia", "local de residência", "cidade", "cidades",
    "brasil", "brasileiro", "capital", "macro região", "macrorregião",
    "micro região", "microrregião", "mesorregião", "mesorregiao",
    # Demografia / atributos
    "faixa etária", "faixa etaria", "idade", "idades", "grupo etário", "grupo etario",
    "sexo", "feminino", "masculino", "raça", "raca", "cor", "escolaridade",
    "anos de estudo", "estado civil", "ocupação", "ocupacao",
    # Tempo / período
    "ano", "anos", "período", "periodo", "série histórica", "serie historica",
    "entre 20", "de 201", "em 202", "últimos anos", "ultimos anos",
    "por ano", "por mês", "por mes", "mensal", "anual",
    # Números / estatística
    "total", "totais", "quantos", "quantas", "quantidade", "número", "numero",
    "quantos óbitos", "quantas mortes", "taxa de mortalidade", "taxa de óbito",
    "proporção", "proporcao", "percentual", "porcentagem", "distribuição",
    "distribuicao", "evolução", "evolucao", "tendência", "tendencia",
    "comparar", "comparação", "ranking", "maior", "menor", "top ",
    # Consultas típicas
    "listar", "listagem", "quais", "quais municípios", "quais estados",
    "onde", "em que", "por que", "por causa", "por doença",
    "por município", "por estado", "por causa básica", "por sexo", "por idade",
]

# Padrões ou palavras que indicam assunto claramente fora do tema (rejeitar mesmo com termo em comum)
OFF_TOPIC_PATTERNS = [
    # Culinária / receitas
    r"\breceita\b", r"\bbolo\b", r"\bcomo fazer\b", r"\bcomo cozinhar\b",
    r"\breceita de\b", r"\bingredientes\b", r"\bmassa\b.*\bbolo\b",
    r"\bpasso a passo\b.*\bcomida\b", r"\bcomo preparar\b.*\bcomida\b",
    # Esporte / futebol
    r"\bgol do\b", r"\bfutebol\b", r"\btime\b.*\bcampeonato\b", r"\bjogo\b.*\bfutebol\b",
    r"\bmelhor time\b", r"\bplacar\b", r"\bcopa do mundo\b", r"\bcampeonato brasileiro\b",
    r"\bquem ganhou\b", r"\bresultado do jogo\b", r"\bpartida\b.*\bfutebol\b",
    # Piada / entretenimento
    r"\bpiada\b", r"\bconto uma\b", r"\bme conta\b.*\bpiada\b",
    r"\bpoema\b", r"\bme escreva um poema\b", r"\bescreva um poema\b",
    r"\bconte uma história\b", r"\bconte uma historia\b", r"\bpiada de\b",
    # Tradução
    r"\btraduza\b", r"\btraduzir\b", r"\btradução\b", r"\btraduzir para\b",
    # Programação
    r"\bcódigo em python\b", r"\bcodigo em python\b", r"\bscript\b.*\bpython\b",
    r"\bcomo programar\b", r"\bexemplo de código\b", r"\bexemplo de codigo\b",
    r"\bfunção em\b.*\bpython\b", r"\bclasse em\b.*\bjava\b",
    # Clima / tempo
    r"\bclima\b", r"\bprevisão do tempo\b", r"\bprevisao do tempo\b",
    r"\btemperatura\b.*\bhoje\b", r"\bchover\b", r"\bprevisão\b.*\bclima\b",
    # Conhecimento geral / curiosidade fora de saúde
    r"\bqual a capital\b", r"\bhistória do brasil\b", r"\bhistoria do brasil\b",
    r"\bquem descobriu\b", r"\bem que ano\b.*\bindependência\b",
    r"\bpopulação mundial\b", r"\bquantos habitantes\b.*\bmundo\b",
    # Compras / preços
    r"\bpreço do\b", r"\bpreco do\b", r"\bquanto custa\b", r"\bonde comprar\b",
    r"\bmelhor celular\b", r"\bcomparar preços\b",
]

MIN_LENGTH = 3  # Perguntas com menos caracteres (após strip) podem ser rejeitadas se não tiverem termo de tema


def _normalize(text: str) -> str:
    """Minúscula e remove acentos para comparação."""
    if not text:
        return ""
    text = text.lower().strip()
    for a, b in [
        ("á", "a"), ("é", "e"), ("í", "i"), ("ó", "o"), ("ú", "u"),
        ("â", "a"), ("ê", "e"), ("ô", "o"), ("ã", "a"), ("õ", "o"), ("ç", "c"),
    ]:
        text = text.replace(a, b)
    return text


def is_on_topic(pergunta: str) -> bool:
    """
    Retorna True se a pergunta for sobre dados de óbitos/mortalidade (SIM).
    False caso contrário (rejeitar e não chamar o agente).
    """
    if not pergunta or not isinstance(pergunta, str):
        return False
    raw = pergunta.strip()
    if len(raw) < MIN_LENGTH:
        return False
    low = _normalize(raw)

    # Regra 2: blacklist primeiro (intenção absurda)
    for pattern in OFF_TOPIC_PATTERNS:
        if re.search(pattern, low):
            return False

    # Regra 1: pelo menos um termo de tema
    for term in TOPIC_TERMS:
        term_norm = _normalize(term)
        if term_norm in low:
            return True

    return False


def get_reject_message() -> str:
    """Mensagem fixa retornada quando o guardrail rejeita a pergunta."""
    return (
        "Essa pergunta não parece ser sobre dados de óbitos ou mortalidade (SIM). "
        "Posso ajudar apenas com consultas sobre esses dados oficiais."
    )
