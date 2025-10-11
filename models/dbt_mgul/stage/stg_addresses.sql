{{ config(materialized='view') }}

select
  SAFE_CAST(_id           AS STRING)  as address_id,
  SAFE_CAST(_user         AS STRING)  as user_id,
  SAFE_CAST(_country      AS INT64)   as country_id,
  SAFE_CAST(_state        AS INT64)   as state_id,
  SAFE_CAST(_city         AS INT64)   as city_id,
  SAFE_CAST(_neighborhood AS INT64)   as neighborhood_id,
  SAFE_CAST(invoiceType   AS STRING)  as invoice_type
from {{ source('raw','addresses') }}
where _id is not null
