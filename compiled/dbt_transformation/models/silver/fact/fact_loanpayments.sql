WITH fact_loanpayments AS (
    SELECT
        lp.loan_payment_id,
        lp.scheduled_amount,
        lp.principal,
        lp.interest,
        lp.actual_amount,
        lp.scheduled_date,
        lp.paid_date,
        lp.loan_id
    FROM bank."main"."raw_loanpayments" lp
)

SELECT * FROM fact_loanpayments