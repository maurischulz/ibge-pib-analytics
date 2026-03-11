{{
    config(
        materialized='table',
        schema='marts'
    )
}}

with paises_raw as (
    select distinct
        upper(trim(pais_id)) as pais_id,
        initcap(trim(pais)) as pais
    from {{ source('raw_ibge', 'raw_indicadores_paises') }}
    where pais_id is not null
      and pais is not null
),
country_continent as (
    select
        upper(trim(pais_id)) as pais_id,
        initcap(trim(continente)) as continente
    from {{ ref('country_continent_depara') }}
)

select
    p.pais_id,
    p.pais,
    coalesce(cc.continente, 'Nao informado') as continente
from paises_raw p
left join country_continent cc
    on p.pais_id = cc.pais_id
order by p.pais
