"""
Schema das views gold para o LLM gerar SQL correta.
"""
SCHEMA_VIEWS = """
Você gera apenas uma query SQL DuckDB. Tabelas disponíveis (read-only):

1) v_obitos_analise - view principal para análise (use esta quando possível)
   Colunas: dt_obito (DATE), ano (INTEGER), sexo_desc (TEXT: Masculino, Feminino, Ignorado),
   faixa_etaria (TEXT: < 1 ano, 1-4 anos, 5-9 anos, 10-14 anos, 15-19 anos, 20-29 anos, 30-39 anos, 40-49 anos, 50-59 anos, 60-69 anos, 70-79 anos, 80+ anos, Ignorado),
   uf_residencia (TEXT: sigla UF), municipio_residencia (TEXT: nome do município),
   causa_basica (TEXT: código CID-10), causa_cid10_capitulo_desc (TEXT: descrição do capítulo CID-10),
   causa_cid10_desc (TEXT: descrição da causa), circunstancia_desc (TEXT), local_ocorrencia_desc (TEXT),
   municipio_ocorrencia, uf_ocorrencia, tipo_obito_desc, racacor_desc, estciv_desc.

2) v_obitos_completo - mesma base sem faixa_etaria (tem idade em código)
   Mesmas colunas exceto faixa_etaria; tem dt_obito, ano, sexo_desc, uf_residencia, municipio_residencia, causa_basica, causa_cid10_capitulo_desc, etc.

Regras: Apenas SELECT (ou WITH ... SELECT). Para filtro por município use EXATAMENTE os valores fornecidos no contexto (municípios resolvidos). Para filtro por causa/doença use o contexto de causa quando fornecido (causa_basica LIKE 'código%%' ou causa_cid10_desc ILIKE '%%termo%%'). Não invente dados. Retorne JSON com uma única chave "sql" cujo valor é a query.
"""

PLAN_SYSTEM = (
    "Você é um assistente que gera apenas uma instrução SQL SELECT para responder à pergunta do usuário "
    "com base nos dados oficiais de óbitos (SIM). Use somente as tabelas e colunas descritas no schema. "
    "Para filtros por município, use EXATAMENTE os valores canônicos fornecidos no contexto (não use o texto bruto do usuário). "
    "Criatividade zero: não invente números nem valores. Resposta apenas em JSON com chave 'sql' contendo a query."
)

EVALUATE_SYSTEM = (
    "Você avalia se o resultado de uma query SQL responde adequadamente à pergunta do usuário. "
    "Responda em uma linha: SIM ou NÃO. Se NÃO, acrescente um breve motivo em seguida (ex: NÃO. Motivo: a query não filtra por ano)."
)

RESPOND_SYSTEM = (
    "Você formata a resposta final ao usuário com base no resultado da query. "
    "Seja breve e objetivo. Use apenas os números e dados presentes no resultado. Não invente nada."
)

# Uma única chamada: avalia e, se OK, formata a resposta; se não OK, pede replan (economiza quota)
EVALUATE_AND_RESPOND_SYSTEM = (
    "Você recebe: a pergunta do usuário, a query SQL executada e o resultado (tabela). "
    "REGRA 1: Se o resultado responde adequadamente à pergunta, escreva uma resposta breve e objetiva para o usuário usando APENAS os dados do resultado. Não invente nada. "
    "REGRA 2: Se o resultado NÃO responde (ex.: query errada, filtro faltando, dados vazios sem motivo), responda EXATAMENTE na primeira linha: REPLAN: <motivo em poucas palavras>. "
    "Exemplo de REPLAN: 'REPLAN: a query não filtra por município'. Não escreva mais nada além dessa linha quando for REPLAN."
)

# Usado no nó de extração de lugar: a IA devolve só a menção a cidade/município/estado para depois resolver com a ferramenta
EXTRACT_PLACE_SYSTEM = (
    "Você extrai da pergunta do usuário APENAS a parte que se refere a um lugar (cidade, município ou estado). "
    "Exemplos: 'quantos óbitos em Curitiba' -> a parte do lugar é 'Curitiba'; "
    "'em São Bento do Sul Santa Catarina' -> 'São Bento do Sul Santa Catarina'; "
    "'no Rio de Janeiro' -> 'Rio de Janeiro'. "
    "Se não houver menção a lugar, retorne vazio. "
    "Resposta APENAS em JSON com uma única chave 'place' (string). Exemplo: {\"place\": \"São Bento do Sul Santa Catarina\"} ou {\"place\": \"\"}."
)
