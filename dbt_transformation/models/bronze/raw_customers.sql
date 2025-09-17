with raw_customers as (
SELECT *
FROM {{source('raw', 'customers')}}
)

select * from raw_customers
