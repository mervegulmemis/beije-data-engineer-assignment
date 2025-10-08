{{ config(materialized='view') }}

-- Müşteri verisini normalize eder
select
  _id,
  safe_cast(signup_ts as timestamp) as signup_ts,
  createdAt
from Raw.users
where _id is not null

select * from users