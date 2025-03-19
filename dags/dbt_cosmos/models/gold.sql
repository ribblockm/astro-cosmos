{{ config(
    materialized='view'
    ) }}

select
    country,
    state,
    brewery_type,
    count(*) as breweries_counts
from
    {{ ref('silver') }}
group by
    country,
    state,
    brewery_type
