-- Gold must not contain a raw email, phone number or name column
select table_name, column_name
from {{ target.database }}.information_schema.columns
where table_schema = 'gold'
  and lower(column_name) in ('email', 'msisdn', 'first_name', 'last_name')
