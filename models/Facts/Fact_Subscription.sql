{{ config(materialized='table') }}

with Fact_Subscription as (

    select 
    cs.Customer_Subscription_Key,
    p.Product_Key,
    dd_s.Date_Key AS Start_Key,
    dd_e.Date_Key as End_Date_Key,
    tt_e.hour_24 as End_Time_Key,
    p.sku as sku,
    t.live as is_live,
    t.quantity,
    t.frequency_days,
    t.merchant_order_id as order_id,
    ifnull(t.price,'N/A') as price,
    t.updated as updated_datetime,
    t.customer,
    1 as TOTAL_SUBSCRIPTION
    from  {{ source('muniqlifebigcommerce','og_subs')}} t
    join {{ ref('Dim_Customer_Subscription') }} cs on t.customer=cs.customer
    join {{ ref('Dim_Product') }} p on t.product=p.sku
    join {{ ref('Dim_Date') }} dd_s on dd_s.full_date=PARSE_DATE("%Y-%m-%d",  t.start_date)
    left join {{ ref('Dim_Date') }} dd_e on dd_e.full_date= PARSE_DATE("%Y-%m-%d", substr(t.cancelled,1,11))
    left join {{ source('Urlabs_DW_Amy','Dim_TimeOfDay')}} tt_e on tt_e.hour_24= EXTRACT(HOUR FROM (timestamp (cancelled) ) )
    )

select *
from Fact_Subscription  