with source as (
    select * from {{ source('RAW', 'QUOTES')}}
)

select
    "QuoteRef"::varchar as quote_ref,
    "CustomerRef"::varchar as customer_ref,
    "Application Ref"::varchar as application_ref,
    "Loan Ref"::varchar as loan_ref,
    "type"::varchar as type,
    "previous loan ref"::varchar as previous_loan_ref,
    " income"::integer as income,
    " mortgage"::integer as mortage,
    " bills"::integer as bills,
    "postcode"::varchar as postcode,
    "phone number"::varchar as phone_number,
    "Brand"::varchar as brand,
    "Value"::integer as value,
    "Term"::integer as term,
    "Income_Check_id"::varchar as income_check_id,
    case when "Income_Check" = 'Null' then null else "Income_Check" end::boolean as income_check,
    "Mortgage_check_id"::varchar as mortgage_check_id,
    case when "Mortgage_Check" = 'Null' then null else "Mortgage_Check" end::boolean as mortgage_check,
    "Bills_Check_id"::varchar as bills_check_id,
    case when "Bills_Check" = 'Null' then null else "Bills_Check" end::boolean as bills_check,
    "Status"::varchar as status,
    "Time"::timestamp as time,
    "ingested_at"::timestamp as ingested_at,
    "file_name"::varchar as file_name

from source
qualify row_number() over (partition by quote_ref order by ingested_at desc)=1 and quote_ref is not null