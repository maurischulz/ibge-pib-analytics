{{
    config(
        materialized='table',
        schema='marts'
    )
}}

with country_map as (
    select distinct
        upper(trim(pais_id)) as pais_id,
        initcap(trim(pais)) as pais
    from {{ source('raw_ibge', 'raw_indicadores_paises') }}
    where pais_id is not null
      and pais is not null

    union

    select distinct
        upper(trim(pais_id)) as pais_id,
        initcap(trim(pais)) as pais
    from {{ ref('country_continent_depara') }}
),
pib_fato as (
    select
        cm.pais_id,
        1 as indicador_id,
        cast(rp.ano as integer) as ano,
        cast(rp.pib as numeric(18, 4)) as valor
    from {{ source('raw_ibge', 'raw_pib_paises') }} rp
    left join country_map cm
        on initcap(trim(rp.pais)) = cm.pais
    where rp.pais is not null
      and rp.ano is not null
      and rp.pib is not null
),
indicadores_fato as (
    select
        upper(trim(ri.pais_id)) as pais_id,
        cast(ri.indicador_id as integer) as indicador_id,
        cast(ri.ano as integer) as ano,
        cast(ri.valor as numeric(18, 4)) as valor
    from {{ source('raw_ibge', 'raw_indicadores_paises') }} ri
    where ri.pais_id is not null
      and ri.indicador_id is not null
      and ri.ano is not null
      and ri.valor is not null
)

select
    pais_id,
    indicador_id,
    ano,
    valor
from pib_fato
where pais_id is not null

union all

select
    pais_id,
    indicador_id,
    ano,
    valor
from indicadores_fato
order by ano desc, pais_id, indicador_id
