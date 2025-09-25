WITH dim_loan AS (
    SELECT
        l.loan_id,
        l.amount,
        l.interest_rate,
        l.term,
        l.loan_start_date,
        l.loan_end_date,
        lt.loan_type,
        ls.loan_status,
        l.customer_id
    FROM bank."main"."raw_loans" l
    LEFT JOIN bank."main"."raw_loanstypes" lt ON l.type_id = lt.type_id
    LEFT JOIN bank."main"."raw_loanstatus" ls ON l.status_id = ls.status_id)

    SELECT * FROM dim_loan