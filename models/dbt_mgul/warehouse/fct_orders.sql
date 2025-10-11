{{ config(
  materialized='table',
  partition_by = { 'field': 'order_date', 'data_type': 'date' },
  cluster_by   = ['customer_id','subscription_id']
) }}

with src as (
  select
    order_id,
    user_id           as customer_id,
    
SAFE_CAST(subscription_id AS STRING) as subscription_id,
    order_date,
    items_total,
    discount_total,
    shipping_fee,
    coalesce(payment_status, order_status)    as payment_status
  from {{ ref('stg_orders') }}
  where order_id is not null
)

select
  order_id,
  customer_id,
  subscription_id,
  order_date,
  items_total,
  discount_total,
  shipping_fee,
  case
    when lower(payment_status) = 'paid'
      then coalesce(items_total, 0)
         - coalesce(discount_total, 0)
         + coalesce(shipping_fee, 0)
    else 0
  end as net_revenue,
  payment_status
from src
