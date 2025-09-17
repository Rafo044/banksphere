with raw_transactiontypes as (
SELECT *
FROM {{source('raw', 'transactiontypes')}}
)

select * from raw_transactiontypes
