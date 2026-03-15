# Consultar com IA

A aba **Consultar com IA** permite fazer perguntas em português sobre os dados de óbitos. O agente interpreta a pergunta, gera uma consulta SQL, executa e devolve a resposta em texto.

---

## Requisitos

- Camada **gold** construída (veja [Download de Dados](download-dados.md)).
- Provedor e chave de API configurados (veja [Configurações](configuracoes.md)).

---

## Como usar

1. Acesse **SIM → Consultar com IA**.
2. Digite sua pergunta no campo de chat. Exemplos:
   - *"Quantos óbitos por dengue ocorreram em 2023?"*
   - *"Quais as 5 principais causas de morte no Paraná?"*
   - *"Em qual dos últimos 10 anos ocorreu o maior número de mortes por dengue? Considere todos os estados."*
3. Aguarde o processamento — o agente mostrará a resposta e, em um expander, a **query SQL** utilizada.

<!-- TODO: GIF demonstração de pergunta e resposta -->

---

## Auditoria

Cada resposta é **auditável**: a consulta SQL gerada é exibida e pode ser copiada para o **Editor SQL** para verificação manual. Isso garante transparência total sobre como a resposta foi obtida.

---

## Dicas

- Seja específico sobre lugar e período para respostas mais precisas.
- Use "considere todos os estados" ou "todo o Brasil" quando quiser escopo nacional.
- Perguntas fora do tema (que não sejam sobre óbitos/mortalidade) são rejeitadas automaticamente pelo guardrail.

---

Próximo passo: [Editor SQL](editor-sql.md)
