{{ config(materialized='view') }}

select
  SAFE_CAST(_id         AS INT64)  as state_id,
  SAFE_CAST(name       AS STRING) as state_name,
  SAFE_CAST(_country AS INT64)  as country_id
from {{ source('raw','states') }}
