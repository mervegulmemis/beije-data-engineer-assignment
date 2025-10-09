{{ config(materialized='view') }}

with src as (
  select * from {{ source('raw','orders') }}
)

select
  SAFE_CAST(_id               AS STRING)    as order_id,
  SAFE_CAST(_user             AS STRING)    as user_id,
  SAFE_CAST(_deliveryAddress  AS STRING)    as delivery_address_id,
  SAFE_CAST(_invoiceAddress   AS STRING)    as invoice_address_id,
  SAFE_CAST(price             AS NUMERIC)   as items_total,
  SAFE_CAST(status            AS STRING)    as status,
  SAFE_CAST(subscriptions     AS STRING)    as subscription_ids,   -- düz metin ise
  SAFE_CAST(createdAt         AS TIMESTAMP) as order_ts,
  CAST(createdAt AS DATE)                   as order_date
from src
where _id is not null
