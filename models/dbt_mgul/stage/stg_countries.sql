{{ config(materialized='view') }}

select
  SAFE_CAST(_id       AS STRING)  as city_id,
  SAFE_CAST(name     AS STRING) as city_name,
from {{ source('raw','countries') }}
where _id is not null