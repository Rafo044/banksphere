

```sql
INSTALL httpfs;
LOAD httpfs;

SET s3_endpoint='localhost:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_use_ssl='false';
SET s3_region='';
SET s3_url_style='path';
```


```sql

/*  | Parquet fayl        | Temp table adı | Description                                             |
| ------------------------- | -------------- | ------------------------------------------------------- |
| Accounts.parquet          | accounts       | Bütün bank hesablarının məlumatları                     |
| AccountStatus.parquet     | accstatus      | Hesabların aktiv, bloklanmış və ya digər statusları     |
| AccountTypes.parquet      | acctypes       | Hesab növlərinin siyahısı (məs: checking, savings)      |
| Addresses.parquet         | address        | Müştəri və ya filial ünvanlarının məlumatları           |
| AddressTypes.parquet      | addrestypes    | Ünvan növləri (məs: ev, iş, poçt)                       |
| Branches.parquet          | branches       | Bank filialları və onların atributları                  |
| Customers.parquet         | customers      | Müştəri məlumatları (ad, soyad, qeydiyyat və s.)        |
| CustomerTypes.parquet     | customtypes    | Müştəri növləri (məs: fərdi, korporativ)                |
| EmployeePositions.parquet | emposition     | İşçilərin vəzifə və mövqeləri                           |
| Employees.parquet         | employee       | İşçilərin məlumatları (ad, soyad, vəzifə, filial)       |
| LoanPayments.parquet      | loanpayments   | Kredit ödənişlərinin detalları                          |
| Loans.parquet             | loans          | Kreditlərin əsas məlumatları (məbləğ, faiz, müddət)     |
| LoanStatus.parquet        | loanstatus     | Kreditlərin statusları (aktiv, ödənmiş, gecikmiş və s.) |
                                   */
```


```sql

-- Read files
-- Accounts
CREATE TEMP TABLE accounts AS
SELECT *
FROM read_parquet('s3://bronz/Accounts.parquet');

-- AccountStatus
CREATE TEMP TABLE accstatus AS
SELECT *
FROM read_parquet('s3://bronz/AccountStatus.parquet');

-- AccountTypes
CREATE TEMP TABLE acctypes AS
SELECT *
FROM read_parquet('s3://bronz/AccountTypes.parquet');

-- Addresses
CREATE TEMP TABLE address AS
SELECT *
FROM read_parquet('s3://bronz/Addresses.parquet');

-- AddressTypes
CREATE TEMP TABLE addrestypes AS
SELECT *
FROM read_parquet('s3://bronz/AddressTypes.parquet');

-- Branches
CREATE TEMP TABLE branches AS
SELECT *
FROM read_parquet('s3://bronz/Branches.parquet');

-- Customers
CREATE TEMP TABLE customers AS
SELECT *
FROM read_parquet('s3://bronz/Customers.parquet');

-- CustomerTypes
CREATE TEMP TABLE customtypes AS
SELECT *
FROM read_parquet('s3://bronz/CustomerTypes.parquet');

-- EmployeePositions
CREATE TEMP TABLE emposition AS
SELECT *
FROM read_parquet('s3://bronz/EmployeePositions.parquet');

-- Employees
CREATE TEMP TABLE employee AS
SELECT *
FROM read_parquet('s3://bronz/Employees.parquet');

-- LoanPayments
CREATE TEMP TABLE loanpayments AS
SELECT *
FROM read_parquet('s3://bronz/LoanPayments.parquet');

-- Loans
CREATE TEMP TABLE loans AS
SELECT *
FROM read_parquet('s3://bronz/Loans.parquet');

-- LoanStatus
CREATE TEMP TABLE loanstatus AS
SELECT *
FROM read_parquet('s3://bronz/LoanStatus.parquet');
```




```sql

PRAGMA table_info('accstatus');
```
