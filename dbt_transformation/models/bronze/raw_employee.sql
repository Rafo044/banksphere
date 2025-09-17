with raw_employee as (
SELECT *
FROM {{source('raw', 'employee')}}
)

select * from raw_employee
