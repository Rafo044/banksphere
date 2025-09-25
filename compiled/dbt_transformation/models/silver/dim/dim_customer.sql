WITH dim_customer AS (
    SELECT
        c.customer_id,
        ct.customer_type,
        p.person_id,
        p.first_name,
        p.last_name,
        p.date_of_birth,
        p.email,
        p.phone_number,
        p.ssn,
        a.address_id,
        a.street,
        a.postal_code,
        a.city,
        a.country
    FROM bank."main"."raw_customers" c
    LEFT JOIN bank."main"."raw_customtypes" ct ON c.type_id = ct.type_id
    LEFT JOIN bank."main"."raw_persons" p ON c.customer_id = p.person_id
    LEFT JOIN bank."main"."raw_address" a ON p.address_id = a.address_id
)

SELECT *
FROM dim_customer