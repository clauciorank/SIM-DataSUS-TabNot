-- Tratamento dos dados SIM (Declaração de Óbito) para camada silver
-- A view 'obitos' deve existir (criada antes). Placeholder: $output

-- Converte DTOBITO (ddmmaaaa) e DTNASC (ddmmaaaa) para DATE
-- Filtra registros com data inválida
COPY (
    SELECT
        ORIGEM,
        TIPOBITO,
        TRY_STRPTIME(DTOBITO, '%d%m%Y')::DATE AS dt_obito,
        HORAOBITO,
        "NATURAL",
        CODMUNNATU,
        TRY_STRPTIME(DTNASC, '%d%m%Y')::DATE AS dt_nascimento,
        IDADE,
        SEXO,
        RACACOR,
        ESTCIV,
        ESC,
        ESC2010,
        OCUP,
        CODMUNRES,
        LOCOCOR,
        CODMUNOCOR,
        CAUSABAS,
        CIRCOBITO,
        PESO,
        SEMAGESTAC,
        GESTACAO,
        PARTO,
        CONTADOR
    FROM obitos
    WHERE TRY_STRPTIME(DTOBITO, '%d%m%Y') IS NOT NULL
) TO $output (FORMAT PARQUET, COMPRESSION 'zstd');
