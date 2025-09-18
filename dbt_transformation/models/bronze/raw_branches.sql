with raw_branches as (
SELECT *
FROM {{source('raw', 'branches')}}
)

select * from raw_branches
