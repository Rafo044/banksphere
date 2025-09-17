with raw_emposition as (
SELECT *
FROM {{source('raw', 'emposition')}}
)

select * from raw_emposition
