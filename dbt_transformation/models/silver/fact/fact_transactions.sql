WITH fact_transactions AS (
    SELECT *
    FROM {{ ref('raw_transactions') }} t
    LEFT JOIN {{ ref('raw_transactiontypes') }} tt
    ON t.type_id = tt.type_id
)


select * from fact_transactions
