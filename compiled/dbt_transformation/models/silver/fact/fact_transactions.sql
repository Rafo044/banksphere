WITH fact_transactions AS (
    SELECT *
    FROM bank."main"."raw_transactions" t
    LEFT JOIN bank."main"."raw_transactiontypes" tt
    ON t.type_id = tt.type_id
)


select * from fact_transactions