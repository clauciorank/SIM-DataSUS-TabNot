# Início Rápido

Este guia mostra como instalar e rodar o **SIM DataSUS** em poucos minutos.

---

## Opção 1 — Docker (recomendado)

Se você tem o Docker instalado, basta dois comandos:

```bash
git clone https://github.com/seu-usuario/DatasusBrasileiroApp.git
cd DatasusBrasileiroApp
docker compose up
```

O aplicativo estará disponível em **http://localhost:8501**. Os dados ficam persistidos na pasta `data/` mesmo após parar o container.

---

## Opção 2 — Instalação local

```bash
git clone https://github.com/seu-usuario/DatasusBrasileiroApp.git
cd DatasusBrasileiroApp

python3 -m venv .venv
source .venv/bin/activate   # Linux/macOS
# .venv\Scripts\activate    # Windows

pip install -r requirements.txt
streamlit run app.py
```

O navegador abrirá automaticamente em **http://localhost:8501**.

---

## Primeiros passos

Após abrir o aplicativo, siga esta sequência:

1. **Configurações** — Defina o período (anos) e as UFs desejadas. Se quiser usar a consulta com IA, configure também a chave de API.

<!-- TODO: Screenshot da tela Configurações -->

2. **Download de Dados** — Baixe os arquivos do FTP do Datasus e rode o processamento (silver e gold).

<!-- TODO: Screenshot da tela Download de Dados -->

3. **Explore** — Use a Análise Exploratória para gráficos, a Consulta com IA para perguntas em texto, ou o Editor SQL para consultas diretas.

<!-- TODO: Screenshot da tela inicial com dados carregados -->

> **Dica**: a camada gold precisa ser construída antes de usar as abas de análise, IA, SQL e previsão.

---

Próximo passo: [Configurações](configuracoes.md)
