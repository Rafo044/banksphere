with dim_account AS (
    SELECT
        a.account_id,
        a.account_number,
        a.balance,
        a.date_opened,
        a.date_closed,
        t.account_type,
        s.account_status,
        s.reason,
        b.branch_id,
        b.branch_name
    FROM {{ ref('raw_accounts') }} a
    LEFT JOIN {{ ref('raw_acctypes') }} t ON a.type_id = t.type_id
    LEFT JOIN {{ ref('raw_accstatus') }} s ON a.status_id = s.status_id
    LEFT JOIN {{ ref('raw_branches') }} b ON a.branch_id = b.branch_id
)

SELECT * FROM dim_account
