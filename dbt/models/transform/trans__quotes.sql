with sqq as (
    select * from {{ ref('stg__quotes_quotes') }}
)

select
    {{ remove_string_nulls('quote_ref') }} as quote_ref,
    {{ remove_string_nulls('customer_ref') }} as customer_ref,
    {{ remove_string_nulls('application_ref') }} as application_ref,
    {{ remove_string_nulls('loan_ref') }} as loan_ref,
    {{ remove_string_nulls('type') }} as type,
    {{ remove_string_nulls('previous_loan_ref') }} as previous_loan_ref,
    income,
    mortage,
    bills,
    {{ remove_string_nulls('postcode') }} as postcode,
    {{ remove_string_nulls('phone_number') }} as phone_number,
    {{ remove_string_nulls('brand') }} as brand,
    value,
    term,
    income_check_id,
    income_check,
    mortgage_check_id,
    mortgage_check,
    bills_check_id,
    bills_check,
    {{ remove_string_nulls('status') }} as status,
    time,
    ingested_at as ingested_at,
    file_name as file_name

from sqq