{{ config(materialized='view') }}

-- Adres verisinden son güncel kaydı almak için
select
  invoiceType,
  trim(city) as city,
  trim(state) as state,
  trim(_neighborhood) as neighborhood,
  trim(_country) as country,
  safe_cast(updated_at as timestamp) as updated_at
from {{ source('Raw','addresses') }}
where _user is not null