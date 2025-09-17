with raw_accstatus as (
SELECT *
FROM {{source('raw', 'accstatus')}}
)

select * from raw_accstatus
