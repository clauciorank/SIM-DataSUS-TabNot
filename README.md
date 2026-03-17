

# SIM DataSUS

![Streamlit](https://img.shields.io/badge/Streamlit-FF1B4B?style=for-the-badge&logo=streamlit&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-003B57?style=for-the-badge&logo=duckdb&logoColor=white)
![LangGraph](https://img.shields.io/badge/LangGraph-0A84FF?style=for-the-badge&logo=langgraph&logoColor=white)
![Plotly](https://img.shields.io/badge/Plotly-3F4D66?style=for-the-badge&logo=plotly&logoColor=white)

![OpenAI](https://img.shields.io/badge/OpenAI-412991?style=for-the-badge&logo=openai&logoColor=white)
![Anthropic](https://img.shields.io/badge/Anthropic-000000?style=for-the-badge&logo=anthropic&logoColor=white)
![Gemini](https://img.shields.io/badge/Gemini-4285F4?style=for-the-badge&logo=google&logoColor=white)
![Ollama](https://img.shields.io/badge/Ollama-000000?style=for-the-badge&logo=ollama&logoColor=white)

![XGBoost](https://img.shields.io/badge/XGBoost-000000?style=for-the-badge&logo=xgboost&logoColor=white)

Análise de mortalidade do Brasil com IA, forecasting, SQL e gráficos interativos





---



## Sobre o projeto

O **SIM DataSUS** é uma aplicação completa para download, processamento e análise dos dados de mortalidade do Brasil (SIM — Sistema de Informações sobre Mortalidade). Com ela você pode explorar óbitos por período, causa, local e perfil demográfico, fazer perguntas em linguagem natural usando IA, executar consultas SQL diretamente e gerar previsões de mortalidade com um pipeline forecasting automatizado com modelos estatísticos e machine learning.



## Inspiração

Só quem já trabalhou com dados do DataSUS sabe da dificuldade de usar o tabnet para análises rápidas o sistema não traz nada muito interativo e intuitivo, todas as análises geradas pelo TabNet (daí vem o nome TabNot) são tabelas formatadas no estilo web do início dos anos 2000 onde o SO mais comum ainda era o saudoso Windows XP. Apesar de ser possível baixar os microdados ainda é algo trabalhoso apesar de existirem ótimas bibliotecas para R ([microdatasus](https://github.com/rfsaldanha/microdatasus)) e Python ([PySUS](https://pysus.readthedocs.io/pt/latest/)) o arquivo é um .dbc (que ninguém usa) necessitando de conversões e de conhecimento técnico específico mesmo para análises simples. Além disso os dados demoram para serem atualizados (no lançamento desse projeto -- Março de 2026, os últimos dados disponíveis são de 2024) deixando os dados mais recentes vazios e os gestores de saúde cegos com a tendência para os tempos mais recentes.

A idéia do DataSUS SIM TabNot surge para modernizar a análise trazendo facilidade para o consumo dos microdados, de maneira gráfica rápida (graças ao duckdb), moderna e intuitiva, por meio inclusive de linguagem natural interagindo com IA generativa e trazendo métodos de forecasting para pelo menos atenuar a cegueira sobre os dados mais recentes. 

Por enquanto o sistema deve ser executado localmente (pq máquina em nuvem e token de IA são cobrados em dólares), mas com pequenas alterações pode ser servido na WEB!

---

## Funcionalidades

- **Download e processamento de dados** — Obtém dados diretamente do FTP do Datasus e processa em camadas (raw → silver → gold) para consulta com DuckDB.



- **Análise exploratória** — Filtros por período, sexo, faixa etária, UF, município e causa; gráficos de série temporal, causas, território e pirâmide etária.

https://github.com/user-attachments/assets/27111254-b630-4581-af7d-4d0facc75aa8



- **Consulta com IA** — Perguntas em português sobre os dados; o agente gera SQL automaticamente e devolve respostas auditáveis.

https://github.com/user-attachments/assets/0ad36030-cd48-44c3-9b7b-05186aa26879


- **Editor SQL** — Consultas diretas à view analítica `v_obitos_completo` (DuckDB).


https://github.com/user-attachments/assets/09a321f6-25db-45ff-a703-f0cb9ba726d2


- **Previsão de óbitos** — Forecasting com pipeline automático que testa ARIMA, ETS, XGBoost e média móvel, selecionando o melhor modelo.

https://github.com/user-attachments/assets/ddebcf9d-0e50-4351-b611-42c97e8ce259


---

## Início rápido

### Com Docker (recomendado)

```bash
git clone https://github.com/clauciorank/SIM-DataSUS-TabNot
cd SIM-DataSUS-TabNot
docker compose up
```

Acesse **[http://localhost:8501](http://localhost:8501)**. Os dados ficam persistidos na pasta `data/`.

### Instalação local

```bash
git clone https://github.com/clauciorank/SIM-DataSUS-TabNot
cd SIM-DataSUS-TabNot

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

- **Provedor de IA** (Gemini, Anthropic, OpenAI, Ollama) e chave de API para a consulta com IA.



> Para detalhes, veja o [guia completo de Configurações](docs/guia-usuario/configuracoes.md).

### 2. Download e processamento

Em **SIM → Download de Dados**:

1. Selecione **Período e UFs** para download dos dados.
2. Clique em **Baixar** para obter os dados do FTP.
3. Rode o **processamento silver** e depois **construa a gold**.
4. Pronto — as demais abas ficam habilitadas.



> Para detalhes, veja o [guia de Download de Dados](docs/guia-usuario/download-dados.md).

### 3. Análise exploratória

Em **SIM → Análise Exploratória**, aplique filtros e explore os gráficos interativos:

- Série temporal (anual ou mensal)
- Ranking de causas de morte
- Óbitos por estado ou município
- Pirâmide etária



> Para detalhes, veja o [guia de Análise Exploratória](docs/guia-usuario/analise-exploratoria.md).

### 4. Consultar com IA

Em **SIM → Consultar com IA**, digite perguntas como:

- *"Quantos óbitos por dengue em 2023?"*
- *"Quais as 5 principais causas de morte no Paraná?"*

O agente gera a SQL, executa e mostra a resposta com a query para auditoria.



> Para detalhes, veja o [guia de Consulta com IA](docs/guia-usuario/consultar-ia.md).

### 5. Editor SQL

Em **SIM → Editor SQL**, execute consultas diretamente na view `v_obitos_completo`. Use as consultas prontas ou escreva a sua.



> Para detalhes, veja o [guia do Editor SQL](docs/guia-usuario/editor-sql.md).

### 6. Previsão de óbitos

Em **SIM → Previsão do número de mortes**, configure filtros, frequência (anual/mensal) e horizonte. O pipeline testa múltiplos modelos e seleciona o melhor automaticamente.



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

Uso dos dados conforme termos do [Datasus](https://datasus.saude.gov.br/). 

Reposítório sob [licença MIT](https://opensource.org/license/MIT).
