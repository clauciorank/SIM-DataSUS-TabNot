# Análise Exploratória

A aba **Análise Exploratória** permite filtrar e visualizar os dados de óbitos com gráficos interativos.

---

## Filtros disponíveis

| Filtro | Descrição |
|--------|-----------|
| **Período** | Data mínima e máxima do óbito |
| **Sexo** | Masculino, Feminino ou Todos |
| **Faixa etária** | Faixas padrão (0–4, 5–9, …, 80+) ou Todas |
| **UF** | Uma ou mais UFs de residência |
| **Município** | Dependente das UFs selecionadas |
| **Capítulo CID-10** | Capítulo da causa básica |
| **Causa do óbito** | Códigos e descrições específicas |

Aplique os filtros e os gráficos serão atualizados automaticamente.

<!-- TODO: Screenshot dos filtros da Análise Exploratória -->

---

## Visualizações

- **Série temporal** — Total de óbitos por período (anual ou mensal).

<!-- TODO: Screenshot do gráfico de série temporal -->

- **Óbitos por causa** — Ranking das principais causas de morte no período filtrado.

<!-- TODO: Screenshot do gráfico de causas -->

- **Óbitos por território** — Totais por estado (UF) ou por município, em gráfico de barras.

<!-- TODO: Screenshot do gráfico por território -->

- **Pirâmide etária** — Distribuição por sexo e faixa etária.

<!-- TODO: Screenshot da pirâmide etária -->

Todos os gráficos são interativos (Plotly) — passe o mouse para ver detalhes, clique na legenda para filtrar séries.

---

Próximo passo: [Consultar com IA](consultar-ia.md)
