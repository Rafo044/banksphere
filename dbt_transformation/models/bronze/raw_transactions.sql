with raw_transactions as (
SELECT *
FROM {{source('raw', 'transactions')}}
)

select * from raw_transactions
