Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


columns :
[('accounts',), ('accstatus',), ('acctypes',), ('address',), ('addrestypes',), ('branches',), ('customers',), ('customtypes',), ('employee',), ('emposition',), ('loanpayments',), ('loans',), ('loanstatus',)]


| accounts        | accstatus      | acctypes       | address        | addresstypes    | branches       | customers     | customertupes  | emposition     | employee       | loanpayments   | loans          | loanstatus    | loantypes      | persons        | transactions   | transactionstypes |
|-----------------|----------------|----------------|----------------|-----------------|----------------|---------------|----------------|----------------|----------------|----------------|----------------|---------------|----------------|----------------|----------------|------------------|
| account_id      | status_id      | type_id        | address_id     | address_type_id | branch_id      | customer_id   | type_id        | position_id    | employee_id    | loan_payment_id| loan_id        | status_id     | type_id        | person_id      | transaction_id | type_id          |
| type_id         | account_status | account_type   | address_type_id| address_type    | address_id     | type_id       | customer_type  | employee_position| position_id   | loan_id        | type_id        | loan_status   | loan_type      | address_id     | type_id        | transaction_type |
| status_id       | reason         |                | street         |                 | branch_name    |               |                |                | branch_id      | scheduled_amount| status_id      |               |                | last_name      | loan_payment_id|                  |
| customer_id     |                |                | postal_code    |                 | swift_code     |               |                |                |                | principal      | customer_id    |               |                | first_name     | from_account_id|                  |
| branch_id       |                |                | city           |                 | phone_number   |               |                |                |                | interest       | amount         |               |                | date_of_birth  | to_account_id  |                  |
| account_number  |                |                | country        |                 |                |               |                |                |                | actual_amount  | interest_rate  |               |                | email          | amount         |                  |
| balance         |                |                |                |                 |                |               |                |                |                | scheduled_date | term           |               |                | phone_number   | transaction_date|                  |
| date_opened     |                |                |                |                 |                |               |                |                |                | paid_date      | loan_start_date|               |                | ssn            |                |                  |
| date_closed     |                |                |                |                 |                |               |                |                |                |                | loan_end_date  |               |                |                |                |                  |
