{{ config(materialized='table') }}

select
    *
from
    {{ source('local_delta', 'raw_delta') }}
