select
  invoiceType, _user, _state, _id, _neighborhood, _country, _city  
from {{ source('raw','addresses') }}
where _id is not null