{{ config(materialized='view') }}

select
  SAFE_CAST(_id       AS STRING)  as city_id,
  SAFE_CAST(name     AS STRING) as city_name,
  SAFE_CAST(_state AS STRING)  as state_id,
  SAFE_CAST(_country AS STRING)  as country_id
from {{ source('raw','cities') }}
where _id is not null


