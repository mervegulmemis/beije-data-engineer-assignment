{{ config(materialized='table') }}

with u as (
  select user_id, created_at as signup_ts
  from {{ ref('stg_users') }}
),
a_ranked as (
  select
    address_id,
    user_id,
    invoice_type,
    neighborhood_id,
    city_id,
    state_id,
    country_id,
    row_number() over (partition by user_id order by address_id) as rn
  from {{ ref('stg_addresses') }}
)

select
  u.user_id,
  u.signup_ts,
  ar.address_id  as primary_address_id,
  ar.invoice_type,
  ar.neighborhood_id,
  ar.city_id,
  ar.state_id,
  ar.country_id
from u
left join a_ranked ar
  on u.user_id = ar.user_id
where ar.rn = 1
