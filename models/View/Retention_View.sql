{{ config(materialized='view') }}

with Retention_View as (

    with cte as(
        select 
        s.Customer_Subscription_Key,
        min(s.Start_Key) as Start_day,
        max(s.End_Date_Key) as End_day
        from {{ ref('Fact_Subscription') }} s 
        group by 1
    )
    select 
    c.customer as customer_id,
    c.email,
    s.is_live,
    substring(cte.Start_day,0,4) as Start_Year,
    substring(cte.Start_day,6,2) as Start_Month,
    substring(cte.Start_day,9,2) as Start_Day,
    case when s.is_live = true then 'N/A' else substring(cte.End_day,0,4) end as End_Year,
    case when s.is_live = true then 'N/A' else substring(cte.End_day,6,2) end as End_Month,
    case when s.is_live = true then 'N/A' else substring(cte.End_day,9,2) end as End_Day,
    case when s.is_live != true and (s.End_Time_Key) in (0,1,2,3,4,5,6,7,8,9,10,11) then 'AM'
    when s.is_live != true and (s.End_Time_Key) not in (0,1,2,3,4,5,6,7,8,9,10,11) then 'PM'
    else 'N/A' end as End_Day_AM_PM,
    case when s.is_live = true then 'N/A' else safe_cast(s.End_Time_Key as string)end as End_Hour,
    case when p.product_name like '% Starter Pack' then 'Starter Pack'
    when do.Subscribe ='One Time Purchase' then 'One-Time'
    else 'Subscribe' end as Subscribe_type,
    p.flavor,
    do.Subscribe as Ship_Frequency,
    p.quantity as Serving_Size,
    p.product_name as Packaging,
    DATE_DIFF(CURRENT_DATE(),cast(cte.Start_day as DATE),DAY) as Days_from_sub_start,
    case when s.is_live = true then 999999 else DATE_DIFF(cast(cte.End_day as DATE),cast(cte.Start_day as DATE),DAY) end as Days_Active,
    CONCAT(substring(cte.Start_day,0,4), '-', substring(cte.Start_day,6,2)) AS Cohort,
    case when do.Subscribe != 'One Time Purchase' then true else false end as Valid
    from  {{ ref('Fact_Subscription') }} s 
    join {{ ref('Dim_Customer_Subscription') }} c on c.Customer_Subscription_Key=s.Customer_Subscription_Key
    join cte on cte.Customer_Subscription_Key=s.Customer_Subscription_Key
    join {{ ref('Fact_order') }} o on s.order_id=o.order_id
    join {{ ref('Dim_Product') }} p on o.Product_Key = p.Product_Key
    join {{ ref('Dim_Order') }} do on do.Order_Key = o.Order_Key

    )

select *
from Retention_View