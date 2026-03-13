"""
Mensagens centralizadas para quando o sistema não encontra contexto ou não consegue responder.
Usadas no grafo e no fallback para garantir resposta clara ao usuário.
"""

MSG_LUGAR_NAO_RESOLVIDO = (
    "Não consegui identificar o município ou estado que você mencionou. "
    "Tente informar o nome completo do município ou a sigla da UF (ex.: SC, SP)."
)

MSG_CAUSA_NAO_ENCONTRADA = (
    "Não encontrei essa causa ou doença na base de códigos. "
    "Você pode tentar termos como 'dengue', 'COVID-19', 'capítulo de neoplasias' ou 'doenças do aparelho circulatório'."
)

MSG_NAO_CONSEGUIU_CONSULTA = (
    "Não consegui montar uma consulta adequada para sua pergunta após várias tentativas. "
    "Tente reformular indicando, por exemplo, ano, município ou causa de interesse."
)

MSG_SEM_DADOS_OU_ERRO = (
    "A consulta não retornou dados para os critérios informados, ou ocorreu um erro. "
    "Verifique se ano, município e causa estão corretos e dentro da base disponível."
)
