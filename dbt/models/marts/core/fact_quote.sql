{{
    config(
        materialized = 'incremental',
        unique_key = 'quote_ref'
    )
}}


with transform as (
    select * from {{ref('trans__quotes')}}
    {% if is_incremental() %}
    where time >= (select coalesce(max(time),'1900-01-01') from {{ this }})
    {% endif %}
)

select 
    quote_ref,
    customer_ref,
    application_ref,
    loan_ref,
    type,
    previous_loan_ref,
    income,
    mortage,
    bills,
    postcode,
    phone_number,
    brand,
    value,
    term,
    income_check_id,
    income_check,
    mortgage_check_id,
    mortgage_check,
    bills_check_id,
    bills_check,
    status,
    time,
    ingested_at as _ingested_at,
    current_localtimestamp() as _last_updated_at

from transform