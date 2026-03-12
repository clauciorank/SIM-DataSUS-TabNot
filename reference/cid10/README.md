# Dados estáticos CID-10 (Datasus)

Esta pasta é **versionável** (não está no `.gitignore`). Coloque aqui os arquivos oficiais do Datasus para que o app **não precise baixar** nada ao gerar o depara de causas.

## Opção 1: ZIP (recomendado)

Baixe o arquivo e coloque nesta pasta com o nome `CID10CSV.zip`:

- **URL:** [CID10CSV.zip](http://www2.datasus.gov.br/cid10/V2008/downloads/CID10CSV.zip) (≈297 KB)
- **Documentação:** [Arquivos CSV CID-10](http://www2.datasus.gov.br/cid10/V2008/descrcsv.htm)

## Opção 2: CSVs soltos

Extraia do ZIP e coloque nesta pasta os dois arquivos:

- `CID-10-CAPITULOS.CSV`
- `CID-10-SUBCATEGORIAS.CSV`

Encoding: ISO-8859-1. Separador: `;`.

---

Se esta pasta estiver vazia (ou sem esses arquivos), o app tentará baixar o ZIP do Datasus na primeira vez que construir o depara.
