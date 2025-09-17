with raw_accounts as (
SELECT *
FROM {{source('raw', 'accounts')}}
)

select * from raw_accounts
