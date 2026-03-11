# Sistema de Dados Saúde (Datasus)

Aplicação em Streamlit para download, processamento e análise de dados do **SIM** (Sistema de Informações sobre Mortalidade) do Datasus.

## O que o sistema faz

- **Download de dados**: obtém arquivos SIM (óbitos) do FTP do Datasus por UF e ano, em formato Parquet.
- **Processamento**: pipeline em camadas (raw → silver → gold) com DuckDB; view analítica com faixa etária, legendas (sexo, raça/cor, CID-10, etc.) e join com municípios.
- **Análise exploratória**: filtros por período, sexo, faixa etária, UF, município e causa; série temporal, óbitos por causa, por território (estados/municípios), pirâmide etária.
- **Editor SQL**: consultas diretas à view de óbitos.
- **Dashboard**: visão de mortalidade (forecast).
- **Configurações**: período e UFs padrão para download, persistidos em SQLite (`data/config.db`).

## Requisitos

- Python 3.10+
- Dependências listadas em `requirements.txt`

## Instalação

```bash
# Clone o repositório (ou use o diretório do projeto)
cd DatasusBrasileiroApp

# Crie e ative um ambiente virtual
python3 -m venv .venv
source .venv/bin/activate   # Linux/macOS
# ou: .venv\Scripts\activate   # Windows

# Instale as dependências
pip install -r requirements.txt
```

## Como executar

```bash
streamlit run app.py
```

O navegador abrirá em `http://localhost:8501`. Use a barra lateral para acessar:

- **Configurações** – anos e UFs padrão para download.
- **SIM**
  - **Download de Dados** – baixar arquivos do FTP, processar (raw → silver) e construir a camada gold.
  - **Análise Exploratória** – gráficos e filtros sobre óbitos (requer gold construída).
  - **Editor SQL** – consultas à view de óbitos.
  - **Dashboard Mortalidade** – indicadores de mortalidade.

## Estrutura de dados

As pastas são criadas automaticamente quando necessário:

| Camada   | Caminho           | Conteúdo |
|----------|-------------------|----------|
| Raw      | `data/SIM/raw/`   | Parquets baixados do FTP (por UF/ano). |
| Silver   | `data/SIM/silver/`| Parquets tratados (óbitos, legendas, municípios). |
| Gold     | `data/SIM/gold/`  | DuckDB com views `v_obitos_completo` e `v_obitos_analise`. |

- **Config**: `data/config.db` (SQLite) – preferências de anos e UFs; criado ao usar Configurações.

Nenhuma dessas pastas nem os arquivos de dados (`.parquet`, `.duckdb`, `config.db`) precisam ser versionados; o `.gitignore` já os exclui.

## Fluxo recomendado

1. Abra **Configurações** e defina anos e UFs desejados.
2. Em **SIM → Download de Dados**, baixe os arquivos e rode o processamento (silver e gold).
3. Use **Análise Exploratória** para filtrar e visualizar óbitos (série temporal, causas, território, pirâmide etária).

## Licença

Uso dos dados conforme termos do [Datasus](https://datasus.saude.gov.br/). Código deste repositório sob licença de sua escolha.
