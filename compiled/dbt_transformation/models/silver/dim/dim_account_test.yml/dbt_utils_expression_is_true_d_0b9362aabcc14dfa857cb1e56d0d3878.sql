



select
    1
from bank."main"."dim_account"

where not(account_number  '^[0-9]{10}$')

