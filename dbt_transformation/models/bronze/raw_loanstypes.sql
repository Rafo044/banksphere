with raw_loanstypes as (
SELECT *
FROM {{source('raw', 'loanstypes')}}
)

select * from raw_loanstypes
