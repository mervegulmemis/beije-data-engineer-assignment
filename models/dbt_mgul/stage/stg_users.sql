{{ config(materialized='view') }}

with src as (
  select * from {{ source('raw','users') }}
)
select
  SAFE_CAST(_id        AS STRING)    as user_id,
  SAFE_CAST(createdAt  AS TIMESTAMP) as created_at
from src
where _id is not null
