with raw_addrestypes as (
SELECT *
FROM {{source('raw', 'addrestypes')}}
)

select * from raw_addrestypes
