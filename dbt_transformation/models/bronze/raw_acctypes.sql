with raw_acctypes as (
SELECT *
FROM {{source('raw', 'acctypes')}}
)

select * from raw_acctypes
