-- =============================================================================
-- analytics_queries.sql — Análises econômicas sobre dados IBGE
-- =============================================================================

-- 1. Top 20 paises por PIB per capita (indicador 77823) no ultimo ano
WITH pib_pc AS (
    SELECT
        dp.pais,
        dp.continente,
        fi.ano,
        fi.valor
    FROM analytics_marts.fato_indicador fi
    JOIN analytics_marts.dim_pais dp
        ON fi.pais_id = dp.pais_id
    WHERE fi.indicador_id = 77823
)
SELECT
    pais,
    continente,
    ano,
    ROUND(valor::numeric, 2) AS pib_per_capita
FROM pib_pc
WHERE ano = (SELECT MAX(ano) FROM pib_pc)
ORDER BY pib_per_capita DESC
LIMIT 20;

-- 2. Crescimento YoY de PIB por pais no ultimo ano (indicador 1)
WITH pib AS (
    SELECT
        dp.pais,
        dp.continente,
        fi.ano,
        fi.valor AS pib
    FROM analytics_marts.fato_indicador fi
    JOIN analytics_marts.dim_pais dp
        ON fi.pais_id = dp.pais_id
    WHERE fi.indicador_id = 1
),
current_year AS (
    SELECT pais, continente, ano, pib
    FROM pib
    WHERE ano = (SELECT MAX(ano) FROM pib)
),
previous_year AS (
    SELECT pais, ano + 1 AS ano, pib
    FROM pib
    WHERE ano = (SELECT MAX(ano) - 1 FROM pib)
)
SELECT
    c.pais,
    c.continente,
    ROUND(((c.pib - p.pib) / NULLIF(p.pib, 0) * 100)::numeric, 2) AS crescimento_yoy_pct,
    ROUND(c.pib::numeric, 0) AS pib_atual
FROM current_year c
LEFT JOIN previous_year p ON c.pais = p.pais
WHERE p.pib IS NOT NULL
ORDER BY crescimento_yoy_pct DESC;

-- 3. IPCA acumulado dos ultimos 12 meses (serie nacional, raw)
SELECT
    ROUND(SUM(valor)::numeric, 2) AS ipca_acumulado_12m
FROM raw_ibge.raw_ipca
WHERE periodo >= to_char((current_date - interval '11 months'), 'YYYYMM');

-- 4. Participacao percentual no PIB total (ultimo ano, indicador 1)
WITH pib AS (
    SELECT
        dp.pais,
        dp.continente,
        fi.ano,
        fi.valor AS pib
    FROM analytics_marts.fato_indicador fi
    JOIN analytics_marts.dim_pais dp
        ON fi.pais_id = dp.pais_id
    WHERE fi.indicador_id = 1
),
latest AS (
    SELECT *
    FROM pib
    WHERE ano = (SELECT MAX(ano) FROM pib)
)
SELECT
    pais,
    continente,
    ROUND(pib::numeric / 1000000, 2) AS pib_bilhoes_reais,
    ROUND(
        (pib::numeric / NULLIF((SELECT SUM(pib) FROM latest), 0) * 100),
        2
    ) AS pct_pib_total
FROM latest
ORDER BY pib DESC
LIMIT 15;

-- 5. Estatisticas do IPCA mensal (ultimos 12 meses, raw)
SELECT
    ROUND(AVG(valor)::numeric, 2) AS ipca_media_12m,
    ROUND(MIN(valor)::numeric, 2) AS ipca_min_12m,
    ROUND(MAX(valor)::numeric, 2) AS ipca_max_12m
FROM raw_ibge.raw_ipca
WHERE periodo >= to_char((current_date - interval '11 months'), 'YYYYMM');

-- 6. Evolucao anual do PIB agregado (ultimos 5 anos, indicador 1)
WITH pib_anual AS (
    SELECT
        fi.ano,
        SUM(fi.valor) AS pib_total
    FROM analytics_marts.fato_indicador fi
    WHERE fi.indicador_id = 1
    GROUP BY fi.ano
)
SELECT
    ano,
    ROUND(pib_total::numeric, 0) AS pib_total,
    ROUND((pib_total - LAG(pib_total) OVER (ORDER BY ano))::numeric, 0) AS variacao_absoluta
FROM pib_anual
WHERE ano >= (SELECT MAX(ano) - 4 FROM pib_anual)
ORDER BY ano DESC;

-- 7. Correlacao entre gasto em educacao (77819) e PIB per capita (77823)
WITH educacao AS (
    SELECT
        fi.pais_id,
        dp.pais,
        dp.continente,
        fi.ano,
        AVG(fi.valor) AS gasto_educacao
    FROM analytics_marts.fato_indicador fi
    JOIN analytics_marts.dim_pais dp
        ON fi.pais_id = dp.pais_id
    WHERE fi.indicador_id = 77819
      AND fi.ano >= (
          SELECT MAX(ano) - 5
          FROM analytics_marts.fato_indicador
          WHERE indicador_id = 77819
      )
    GROUP BY fi.pais_id, dp.pais, dp.continente, fi.ano
),
pib_per_capita AS (
    SELECT
        pais_id,
        ano,
        AVG(valor) AS pib_pc
    FROM analytics_marts.fato_indicador
    WHERE indicador_id = 77823
      AND ano >= (
          SELECT MAX(ano) - 5
          FROM analytics_marts.fato_indicador
          WHERE indicador_id = 77823
      )
    GROUP BY pais_id, ano
)
SELECT
    e.pais,
    e.continente,
    corr(e.gasto_educacao, p.pib_pc) AS corr_educacao_pibpc
FROM educacao e
JOIN pib_per_capita p
    ON e.pais_id = p.pais_id
   AND e.ano = p.ano
GROUP BY e.pais, e.continente
ORDER BY corr_educacao_pibpc DESC NULLS LAST;

-- 8. Ranking de estabilidade economica por pais (indicador 1)
-- Score base: menor volatilidade percentual do PIB
WITH pib_metrics AS (
    SELECT
        dp.pais,
        dp.continente,
        ROUND(AVG(fi.valor)::numeric, 0) AS pib_medio,
        ROUND((stddev_pop(fi.valor) / NULLIF(AVG(fi.valor), 0) * 100)::numeric, 2) AS volatilidade_pct
    FROM analytics_marts.fato_indicador fi
    JOIN analytics_marts.dim_pais dp
        ON fi.pais_id = dp.pais_id
    WHERE fi.indicador_id = 1
    GROUP BY dp.pais, dp.continente
)
SELECT
    pais,
    continente,
    pib_medio,
    volatilidade_pct,
    RANK() OVER (ORDER BY volatilidade_pct ASC) AS ranking_estabilidade
FROM pib_metrics
ORDER BY ranking_estabilidade ASC;
