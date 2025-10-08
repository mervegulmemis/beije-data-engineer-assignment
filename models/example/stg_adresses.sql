{{ config(materialized='view') }}

-- Adres verisinden son güncel kaydı almak için
select
  address_id,
  customer_id,
  trim(city) as city,
  trim(state) as state,
  trim(country) as country,
  safe_cast(updated_at as timestamp) as updated_at
from {{ source('Raw','addresses') }}
where customer_id is not null