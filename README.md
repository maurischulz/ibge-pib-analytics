# IBGE Analytics  PIB e Indicadores por Países

Pipeline de dados com API pública do IBGE, ingestão em PostgreSQL e transformações com dbt.

## Escopo Atual

O projeto usa o domínio de países da API do IBGE e coleta:

- `77827` Economia - Total do PIB
- `77831` Indicadores sociais - Índice de desenvolvimento humano
- `77819` Economia - Gastos públicos com educação
- `77820` Economia - Gastos públicos com saúde
- `77823` Economia - PIB per capita
- `77830` Indicadores sociais - Esperança de vida ao nascer
- `77840` Meio Ambiente - Áreas protegidas no total do território nacional
- `77849` População - População
- IPCA mensal nacional via API v3 agregados

## Estrutura

```text
ibge-pib-analytics/
 docker-compose.yml
 Makefile
 requirements.txt
 src/
    ibge_api_client.py
    data_loader.py
 dbt/
    dbt_project.yml
    profiles.yml
   data/
      country_continent_depara.csv
    models/
        staging/
           _stg_sources.yml
        marts/
         dim_pais.sql
         dim_indicador.sql
         fato_indicador.sql
         _marts_dim_fact.yml
 sql/
     analytics_queries.sql
```

## Como Executar (Windows PowerShell)

### 1) Ambiente Python

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### 2) Banco local

```powershell
docker compose up -d
```

### 3) Extração + carga raw

```powershell
python src/data_loader.py
```

Isso gera e carrega:

- `raw_ibge.raw_pib_paises`
- `raw_ibge.raw_indicadores_paises`
- `raw_ibge.raw_ipca`

### 4) Transformações dbt

```powershell
Set-Location dbt
$env:DBT_PROFILES_DIR = "."
..\.venv\Scripts\dbt.exe seed --select country_continent_depara --full-refresh
..\.venv\Scripts\dbt.exe run
..\.venv\Scripts\dbt.exe test
```

### 5) Consultas analíticas

Use o arquivo `sql/analytics_queries.sql` no PostgreSQL.

## Tabelas Marts (Nova Estrutura)

- `analytics_marts.dim_pais` (pais_id, pais, continente)
- `analytics_marts.dim_indicador` (id_indicador, indicador, unidade)
- `analytics_marts.fato_indicador` (pais_id, indicador_id, ano, valor)

## Convenções de Indicadores

- `1` = PIB total (derivado de `raw_ibge.raw_pib_paises`)
- Demais IDs = indicadores vindos de `raw_ibge.raw_indicadores_paises`
- IPCA mensal permanece em `raw_ibge.raw_ipca` para análises temporais

## Portal Web (Portfolio)

Com os dados carregados e transformados, execute o painel interativo:

```powershell
# execute a partir da raiz do projeto
.\.venv\Scripts\python -m streamlit run web/portal.py
```

Se voce estiver usando Anaconda ou outro ambiente global, garanta que o comando rode no mesmo ambiente onde as dependencias do projeto foram instaladas.

Se voce ja estiver dentro da pasta `.venv`, use:

```powershell
.\Scripts\python -m streamlit run ..\web\portal.py
```

O portal inclui:

- KPIs gerais (PIB total, pico de PIB per capita, IPCA 12m, total de paises)
- Graficos de PIB (top per capita, crescimento YoY, participacao no total, evolucao anual)
- Serie de inflacao (IPCA mensal)
- Correlacao de indicadores (educacao vs PIB per capita)
- Ranking de estabilidade economica por volatilidade do PIB

Consultas do portal usam `analytics_marts.dim_pais`, `analytics_marts.fato_indicador` e `raw_ibge.raw_ipca`.

## Observações

- A API do IBGE pode oscilar; o coletor possui fallback para dados simulados para permitir validação end-to-end.
- O projeto foi consolidado para nomenclatura por países.
