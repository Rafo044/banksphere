with raw_persons as (
SELECT *
FROM {{source('raw', 'persons')}}
)

select * from raw_persons
