{{ config(materialized='view') }}

select
  SAFE_CAST(_id            AS STRING)    as subscription_id,
  SAFE_CAST(_user          AS STRING)    as user_id,
  SAFE_CAST(isActive       AS STRING)    as is_active,        -- olduğu gibi string
  SAFE_CAST(products       AS STRING)    as products,         -- JSON/array ise string olarak sakla
  SAFE_CAST(isSkip         AS STRING)    as is_skip,          -- olduğu gibi string
  SAFE_CAST(createdAt      AS TIMESTAMP) as create_date,
  SAFE_CAST(nextOrderDate  AS TIMESTAMP) as next_order_date,
  SAFE_CAST(startDate      AS TIMESTAMP) as start_date,
  SAFE_CAST(totalQuantity  AS NUMERIC)   as total_quantity
from {{ source('raw','subscriptions') }}
where _id is not null
