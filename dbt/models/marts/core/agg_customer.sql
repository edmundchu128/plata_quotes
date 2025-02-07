with source as (
    select * from {{ref('fact_quote')}}
),
count_cte as (
    select
        customer_ref,
        brand,
        sum(
            case when loan_ref is not null and status = 'Disbursed' and type = 'New' then 1 else 0 end
        ) as new_loans_count,
        sum(
            case when loan_ref is not null and status = 'Disbursed' and type = 'Repeat' then 1 else 0 end
        ) as repeat_loans_count,
        sum(
            case when loan_ref is not null and status = 'Disbursed' and type = 'TopUp' then 1 else 0 end
        ) as topup_count,

        count(distinct application_ref) as application_ref_count,
    from source
    group by customer_ref, brand
),
window_cte as (
    select
        customer_ref,
        brand,
        first_value(application_ref IGNORE NULLS) over (partition by customer_ref, brand order by case when type = 'New' and application_ref is not null then 1 else 2 end, time asc) as first_application_ref,
        first_value(loan_ref IGNORE NULLS) over (partition by customer_ref, brand order by case when type = 'New' and loan_ref is not null then 1 else 2 end, time asc) as first_loan_ref
    from source
    qualify row_number() over (partition by customer_ref, brand order by time asc) =1
)
select 
    concat(cc.customer_ref,'-',cc.brand) as agg_customer_skey,
    cc.customer_ref,
    cc.brand,
    cc.new_loans_count,
    cc.repeat_loans_count,
    cc.topup_count,
    cc.application_ref_count,
    wc.first_application_ref,
    wc.first_loan_ref
from count_cte cc
left join window_cte wc
on cc.customer_ref = wc.customer_ref and cc.brand = wc.brand



-- the number of new loans, repeat loans and top-up for both brand for each customer.
-- - the number of previous applications within brand for each customer

-- commented [km5]: presumably new loans here means
-- loans where the status = disbursed

-- - the reference of the very first loan and application for each customer/brand, and the
-- reference of the first loan in a series (i.e. when there are top-ups)