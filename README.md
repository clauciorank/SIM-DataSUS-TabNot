<p align="center">
  <h1 align="center">SIM DataSUS</h1>
  <p align="center">
    Análise de mortalidade do Brasil com IA, forecasting e gráficos interativos
  </p>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/version-0.0.0-blue" alt="v0.0.0">
  <img src="https://img.shields.io/badge/Python-3.10%2B-blue?logo=python&logoColor=white" alt="Python 3.10+">
  <img src="https://img.shields.io/badge/Streamlit-1.28%2B-FF4B4B?logo=streamlit&logoColor=white" alt="Streamlit">
  <img src="https://img.shields.io/badge/DuckDB-Anal%C3%ADtico-FFC107?logo=duckdb&logoColor=black" alt="DuckDB">
  <img src="https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white" alt="Docker">
</p>

---

<!-- TODO: Banner/GIF principal mostrando o app em ação -->

## Sobre o projeto

O **SIM DataSUS** é uma aplicação completa para download, processamento e análise dos dados de mortalidade do Brasil (SIM — Sistema de Informações sobre Mortalidade). Com ela você pode explorar óbitos por período, causa, local e perfil demográfico, fazer perguntas em linguagem natural usando IA, executar consultas SQL diretamente e gerar previsões de mortalidade com um pipeline estatístico automatizado.

---

## Funcionalidades

- **Download e processamento de dados** — Obtém dados diretamente do FTP do Datasus e processa em camadas (raw → silver → gold).

<!-- TODO: Screenshot da tela Download de Dados -->

- **Análise exploratória** — Filtros por período, sexo, faixa etária, UF, município e causa; gráficos de série temporal, causas, território e pirâmide etária.

<!-- TODO: Screenshot da Análise Exploratória -->

- **Consulta com IA** — Perguntas em português sobre os dados; o agente gera SQL automaticamente e devolve respostas auditáveis.

<!-- TODO: GIF demonstração da Consulta com IA -->

- **Editor SQL** — Consultas diretas à view analítica `v_obitos_completo` (DuckDB).

<!-- TODO: Screenshot do Editor SQL -->

- **Previsão de óbitos** — Forecasting com pipeline automático que testa ARIMA, ETS, XGBoost e média móvel, selecionando o melhor modelo.

<!-- TODO: Screenshot da Previsão de Óbitos -->

---

## Início rápido

### Com Docker (recomendado)

```bash
git clone https://github.com/seu-usuario/DatasusBrasileiroApp.git
cd DatasusBrasileiroApp
docker compose up
```

Acesse **http://localhost:8501**. Os dados ficam persistidos na pasta `data/`.

### Instalação local

```bash
git clone https://github.com/seu-usuario/DatasusBrasileiroApp.git
cd DatasusBrasileiroApp

python3 -m venv .venv
source .venv/bin/activate   # Linux/macOS
# .venv\Scripts\activate    # Windows

pip install -r requirements.txt
streamlit run app.py
```

---

## Guia de uso

### 1. Configurações

Na barra lateral, abra **Configurações** para definir:
- **Período e UFs** para download dos dados.
- **Provedor de IA** (Gemini, Anthropic, OpenAI, Ollama) e chave de API para a consulta com IA.

<!-- TODO: Screenshot da tela Configurações -->

> Para detalhes, veja o [guia completo de Configurações](docs/guia-usuario/configuracoes.md).

### 2. Download e processamento

Em **SIM → Download de Dados**:
1. Clique em **Baixar** para obter os dados do FTP.
2. Rode o **processamento silver** e depois **construa a gold**.
3. Pronto — as demais abas ficam habilitadas.

<!-- TODO: Screenshot da tela Download -->

> Para detalhes, veja o [guia de Download de Dados](docs/guia-usuario/download-dados.md).

### 3. Análise exploratória

Em **SIM → Análise Exploratória**, aplique filtros e explore os gráficos interativos:
- Série temporal (anual ou mensal)
- Ranking de causas de morte
- Óbitos por estado ou município
- Pirâmide etária

<!-- TODO: Screenshot da Análise Exploratória -->

> Para detalhes, veja o [guia de Análise Exploratória](docs/guia-usuario/analise-exploratoria.md).

### 4. Consultar com IA

Em **SIM → Consultar com IA**, digite perguntas como:
- *"Quantos óbitos por dengue em 2023?"*
- *"Quais as 5 principais causas de morte no Paraná?"*

O agente gera a SQL, executa e mostra a resposta com a query para auditoria.

<!-- TODO: GIF demonstração -->

> Para detalhes, veja o [guia de Consulta com IA](docs/guia-usuario/consultar-ia.md).

### 5. Editor SQL

Em **SIM → Editor SQL**, execute consultas diretamente na view `v_obitos_completo`. Use as consultas prontas ou escreva a sua.

<!-- TODO: Screenshot do Editor SQL -->

> Para detalhes, veja o [guia do Editor SQL](docs/guia-usuario/editor-sql.md).

### 6. Previsão de óbitos

Em **SIM → Previsão do número de mortes**, configure filtros, frequência (anual/mensal) e horizonte. O pipeline testa múltiplos modelos e seleciona o melhor automaticamente.

<!-- TODO: Screenshot da Previsão -->

> Para detalhes, veja o [guia de Previsão de Óbitos](docs/guia-usuario/previsao-obitos.md).

---

## Documentação completa

A documentação técnica e os guias detalhados estão na pasta `docs/`:

- **[Índice da documentação](docs/README.md)** — Guia do usuário e documentação técnica.

---

## Estrutura do projeto

```
DatasusBrasileiroApp/
├── app.py                    # Ponto de entrada Streamlit
├── pages/                    # Páginas do app (uma por aba)
├── src/
│   ├── agent/                # Agente Text-to-SQL (LangGraph)
│   ├── config/               # Persistência e chaves
│   ├── data_extraction/      # FTP, processamento e gold
│   └── forecasting/          # MortalityForecaster
├── data/                     # Dados (não versionado)
├── docs/                     # Documentação completa
├── Dockerfile                # Container Docker
├── docker-compose.yml        # Compose com persistência
└── requirements.txt          # Dependências Python
```

---

## Licença

Uso dos dados conforme termos do [Datasus](https://datasus.saude.gov.br/). Código deste repositório sob licença de sua escolha.
