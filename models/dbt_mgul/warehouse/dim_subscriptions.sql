{{ config(materialized='table') }}

-- 1) stage tablo
with s as (
  select
    subscription_id,
    user_id,
    lower(trim(cast(is_active as string))) as is_active_raw,   -- stage'de string olabilir
    lower(trim(cast(is_skip   as string))) as is_skip_raw,     -- stage'de string olabilir
    products,                                                  -- string/JSON olarak saklı
    create_date,
    next_order_date,
    start_date,
    total_quantity
  from {{ ref('stg_subscriptions') }}
  where subscription_id is not null
),

-- 2) Boolean normalize edildi
norm as (
  select
    subscription_id,
    user_id,
    case when is_active_raw in ('true','1','yes','y') then true
         when is_active_raw in ('false','0','no','n') then false
         else null end as is_active,

    case when is_skip_raw in ('true','1','yes','y') then true
         when is_skip_raw in ('false','0','no','n') then false
         else null end as is_skip,

    products,
    create_date,
    next_order_date,
    start_date,
    total_quantity,
    row_number() over (
      partition by subscription_id
      order by coalesce(next_order_date, start_date, create_date) desc nulls last
    ) as rn
  from s
)

-- 3) Her subscription_id için en güncel tek satır
select
  subscription_id,
  user_id,
  is_active,
  is_skip,
  products,
  create_date,
  next_order_date,
  start_date,
  total_quantity
from norm
where rn = 1
