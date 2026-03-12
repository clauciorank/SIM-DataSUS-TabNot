# Dados estáticos: municípios e estados (IBGE)

Esta pasta é **versionável** (não está no `.gitignore`). Coloque aqui os arquivos para que o app **não precise** usar o pysus (ou rede) ao construir a tabela de municípios.

## Arquivos

### `estados.csv`
Lista de UFs e nomes completos (para resolução de lugar no agente).  
Formato: `uf,nome` (uma linha por estado). Já incluso no repositório.

### `municipios.csv`
Lista de municípios com código IBGE (para JOIN nos óbitos).  
Formato esperado (separador vírgula, UTF-8):

```text
codigo,geocodigo,municipio,uf
110001,1100015,Alta Floresta D'Oeste,RO
110002,1100023,Ariquemes,RO
...
```

- **codigo**: 6 dígitos (como no SIM: CODMUNRES/CODMUNOCOR)
- **geocodigo**: 7 dígitos (código IBGE completo)
- **municipio**: nome do município
- **uf**: sigla (RO, AC, …, DF)

## Como obter `municipios.csv`

1. **Exportar a partir do pysus** (uma vez, com rede):
   ```bash
   python -c "
   from src.data_extraction.municipios import export_municipios_to_reference
   export_municipios_to_reference()
   "
   ```
   Isso gera `reference/municipios/municipios.csv` a partir do dicionário do pysus. Depois é só commitar o arquivo.

2. **Fonte externa**: use qualquer CSV no formato acima (ex.: [estados-e-municipios-ibge](https://github.com/leogermani/estados-e-municipios-ibge)); adapte os nomes das colunas para `codigo`, `geocodigo`, `municipio`, `uf` se necessário.

Se `municipios.csv` **não** existir nesta pasta, o app tentará usar o pysus na primeira vez que construir a tabela de municípios (e poderá gravar o CSV aqui para uso futuro).
