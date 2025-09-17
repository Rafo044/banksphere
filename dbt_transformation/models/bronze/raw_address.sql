with raw_address as (
SELECT *
FROM {{source('raw', 'address')}}
)

select * from raw_address
