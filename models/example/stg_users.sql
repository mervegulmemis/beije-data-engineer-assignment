{{ config(materialized='view') }}

-- Müşteri verisini normalize eder, email'i küçük harfe çeker, anonimleştirir
select
  customer_id,
  lower(trim(email)) as email,
  digest(lower(trim(email)), 'sha256') as email_hash,  -- anonimleştirme
  safe_cast(signup_ts as timestamp) as signup_ts,
  ingest_date
from Raw.users
where customer_id is not null

select * from users