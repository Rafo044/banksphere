with raw_loanstatus as (
SELECT *
FROM {{source('raw', 'loanstatus')}}
)

select * from raw_loanstatus
