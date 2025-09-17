with raw_customtypes as (
SELECT *
FROM {{source('raw', 'customtypes')}}
)

select * from raw_customtypes
