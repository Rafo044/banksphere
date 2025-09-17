with raw_loans as (
SELECT *
FROM {{source('raw', 'loans')}}
)

select * from raw_loans
