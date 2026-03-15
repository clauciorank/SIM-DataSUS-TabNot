# Editor SQL — Dicionário de Dados

Referência técnica completa da view `v_obitos_completo` e do Editor SQL.

---

## View `v_obitos_completo`

A view reside no DuckDB (`data/SIM/gold/obitos.duckdb`) e reúne óbitos com legendas aplicadas via JOIN.

### Dicionário completo de colunas

| Coluna | Descrição |
|--------|-----------|
| `origem` | Código da origem da declaração |
| `tipo_obito` | Código tipo óbito (1 = Fetal, 2 = Não fetal) |
| `tipo_obito_desc` | Descrição do tipo de óbito |
| `dt_obito` | Data do óbito |
| `dt_obito_mes` | Mês do óbito (1o dia do mês) |
| `ano` | Ano do óbito (extraído da data) |
| `hora_obito` | Hora do óbito |
| `natural` | Indicador de óbito natural |
| `cod_mun_nascimento` | Código IBGE do município de nascimento |
| `dt_nascimento` | Data de nascimento |
| `idade` | Código idade (formato SIM) |
| `idade_anos` | Idade em anos (calculada) |
| `faixa_etaria` | Faixa etária (ex.: "0-4", "60-69", "80+") |
| `sexo` | Código sexo (1 = M, 2 = F) |
| `sexo_desc` | Descrição do sexo |
| `racacor` | Código raça/cor |
| `racacor_desc` | Descrição raça/cor |
| `estciv` | Código estado civil |
| `estciv_desc` | Descrição estado civil |
| `esc` | Código escolaridade |
| `esc_2010` | Código escolaridade (classificação 2010) |
| `ocup` | Código ocupação (CBO) |
| `cod_mun_residencia` | Código IBGE município de residência |
| `municipio_residencia` | Nome do município de residência |
| `uf_residencia` | UF de residência (sigla) |
| `loc_ocorrencia` | Código local de ocorrência do óbito |
| `local_ocorrencia_desc` | Descrição local de ocorrência |
| `cod_mun_ocorrencia` | Código IBGE município de ocorrência |
| `municipio_ocorrencia` | Nome do município de ocorrência |
| `uf_ocorrencia` | UF de ocorrência (sigla) |
| `causa_basica` | Código CID-10 da causa básica do óbito |
| `causa_cid10_capitulo_desc` | Descrição do capítulo CID-10 da causa |
| `causa_cid10_desc` | Descrição da causa (subcategoria CID-10) |
| `circ_obito` | Código circunstância do óbito |
| `circunstancia_desc` | Descrição da circunstância (acidente, suicídio, etc.) |
| `peso` | Peso (gramas) |
| `sem_gestacao` | Semanas de gestação |
| `gestacao` | Código duração gestação |
| `parto` | Código tipo de parto |
| `contador` | Contador (uso interno) |

---

## Limites

- **MAX_ROWS**: o agente de IA limita resultados a 500 linhas para evitar respostas excessivamente grandes. No Editor SQL o usuário pode executar qualquer query, mas recomenda-se usar `LIMIT`.
- **Somente leitura**: o Editor SQL aceita apenas `SELECT` e `WITH`. Operações de escrita são bloqueadas.

---

## Exemplos de consultas

### Óbitos por município e UF

```sql
SELECT municipio_residencia, uf_residencia, COUNT(*) AS total
FROM v_obitos_completo
GROUP BY 1, 2
ORDER BY total DESC
LIMIT 50
```

### Série temporal anual

```sql
SELECT ano, COUNT(*) AS total
FROM v_obitos_completo
GROUP BY ano
ORDER BY ano
```

### Top 10 causas de morte em um estado

```sql
SELECT causa_cid10_capitulo_desc, COUNT(*) AS total
FROM v_obitos_completo
WHERE uf_residencia = 'PR'
GROUP BY 1
ORDER BY total DESC
LIMIT 10
```

### Série mensal por causa específica

```sql
SELECT strftime(dt_obito_mes, '%Y-%m') AS periodo, COUNT(*) AS total
FROM v_obitos_completo
WHERE causa_basica LIKE 'I21%'
GROUP BY dt_obito_mes
ORDER BY 1
```

---

## Base silver (referência)

Se acessar a base silver, as tabelas principais são `obitos` e `municipios` com colunas em maiúsculas (ex.: `CAUSABAS`, `CODMUNRES`, `SEXO`).
