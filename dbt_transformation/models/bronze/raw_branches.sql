with raw_branches as (
SELECT *
FROM {{source('raw', 'accstatus')}}
)

select * from raw_branches
