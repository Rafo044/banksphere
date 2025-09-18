WITH dim_branch_employee AS (
    SELECT
        e.employee_id,
        p.employee_position,
        b.branch_id,
        b.branch_name,
        a.address_id,
        a.street,
        a.city,
        a.country
    FROM {{ ref('raw_employee') }} e
    LEFT JOIN {{ ref('raw_emposition') }} p ON e.position_id = p.position_id
    LEFT JOIN {{ ref('raw_branches') }} b ON e.branch_id = b.branch_id
    LEFT JOIN {{ ref('raw_address') }} a ON b.address_id = a.address_id
)

SELECT * FROM dim_branch_employee
