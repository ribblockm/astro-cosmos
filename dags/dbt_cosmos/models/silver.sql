{{ config(
    materialized='table',
    file_format='delta',
    partition_by='state'
    ) }}

select
    id,
    name,
    brewery_type,
    address_2,
    city,
    postal_code,
    country,
    longitude,
    latitude,
    phone,
    website_url,
    state,
    street
from
    {{ ref('bronze') }}
