{{
    config(
        materialized='table',
        schema='marts'
    )
}}

with indicadores_raw as (
    select distinct
        cast(indicador_id as integer) as id_indicador,
        trim(indicador) as indicador,
        trim(unidade) as unidade
    from {{ source('raw_ibge', 'raw_indicadores_paises') }}
    where indicador_id is not null
),
indicador_pib as (
    select
        1 as id_indicador,
        'Economia - PIB' as indicador,
        'USD' as unidade
),
unioned as (
    select * from indicadores_raw
    union all
    select * from indicador_pib
),
ranked as (
    select
        id_indicador,
        indicador,
        unidade,
        row_number() over (
            partition by id_indicador
            order by case when indicador = 'Economia - PIB' then 0 else 1 end, indicador
        ) as rn
    from unioned
)

select
    id_indicador,
    indicador,
    unidade
from ranked
where rn = 1
order by id_indicador
