{{ config(materialized='view') }}

select
  SAFE_CAST(_id      AS INT64)  as neighborhood_id,
  SAFE_CAST(name    AS STRING) as neighborhood_name,
  SAFE_CAST(_city    AS STRING) as city_name,
  SAFE_CAST(_country    AS STRING) as country_name,
  SAFE_CAST(postalCode    AS STRING) as postal_code

  
from {{ source('raw','neighborhoods') }}
