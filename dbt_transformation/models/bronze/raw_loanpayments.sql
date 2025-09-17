with raw_loanpayments as (
SELECT *
FROM {{source('raw', 'loanpayments')}}
)

select * from raw_loanpayments
