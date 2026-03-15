# Documentação

Bem-vindo à documentação do **SIM DataSUS**. Aqui você encontra guias de uso e documentação técnica sobre cada parte do sistema.

---

## Guia do Usuário

Passo a passo de como usar cada funcionalidade do aplicativo.

| Documento | Descrição |
|-----------|-----------|
| [Início Rápido](guia-usuario/inicio-rapido.md) | Instalação, primeiro acesso e primeiros passos |
| [Configurações](guia-usuario/configuracoes.md) | Período, UFs e chaves de API |
| [Download de Dados](guia-usuario/download-dados.md) | Baixar e processar dados do SIM |
| [Análise Exploratória](guia-usuario/analise-exploratoria.md) | Filtros, gráficos e visualizações |
| [Consultar com IA](guia-usuario/consultar-ia.md) | Perguntas em linguagem natural |
| [Editor SQL](guia-usuario/editor-sql.md) | Consultas diretas ao DuckDB |
| [Previsão de Óbitos](guia-usuario/previsao-obitos.md) | Forecasting e interpretação dos resultados |

---

## Documentação Técnica

Detalhes de arquitetura, pipelines e algoritmos.

| Documento | Descrição |
|-----------|-----------|
| [Arquitetura](tecnico/arquitetura.md) | Visão geral, stack tecnológico e estrutura de pastas |
| [Pipeline de Dados](tecnico/pipeline-dados.md) | Camadas raw, silver e gold; view analítica |
| [Agente de IA](tecnico/agente-ia.md) | Grafo LangGraph, guardrail e resolução de contexto |
| [Forecasting](tecnico/forecasting.md) | Pipeline do MortalityForecaster (5 fases) |
| [Editor SQL](tecnico/editor-sql.md) | Dicionário de dados completo da view `v_obitos_completo` |
