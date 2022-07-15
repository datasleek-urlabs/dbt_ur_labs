
{{ config(materialized='table') }}

with Subscription_Fact_Report as (

    with cte as(
        select 
        s.Customer_Subscription_Key,
        min(s.Start_Key) as Start_day_key,
        max(s.End_Date_Key) as End_day_key,
        max(s.is_live) as is_live_customer
        
        from {{ ref('Fact_Subscription') }} s 
        group by 1
    )
    select 
    c.customer as customer_id,
    cte.is_live_customer,
    -- s.is_live as is_live_product
    -- s.sku,
    s.order_id,
    -- s_d.year as Start_Year,
    -- s_d.month as Start_Month,
    -- s_d.year_day as Start_Day,
    cte.Start_day_key,
    case when s.is_live = true then null else cte.End_day_key end as End_day_key,
    -- case when s.is_live = true then 'N/A' else cast(e_d.year as string) end as End_Year,
    -- case when s.is_live = true then 'N/A' else cast(e_d.month as string) end as End_Month,
    -- case when s.is_live = true then 'N/A' else cast(e_d.year_day as string) end as End_Day,
    -- case when s.is_live = true then 'N/A' else safe_cast(s.End_Time_Key as string)end as End_Hour,
    -- case when s.is_live != true and (s.End_Time_Key) in (0,1,2,3,4,5,6,7,8,9,10,11) then 'AM'
    -- when s.is_live != true and (s.End_Time_Key) not in (0,1,2,3,4,5,6,7,8,9,10,11) then 'PM'
    -- else 'N/A' end as End_Day_AM_PM,
    DATE_DIFF(CURRENT_DATE(),cast(cte.Start_day_key as DATE),DAY) as Days_from_sub_start,
    case when s.is_live = true then null else DATE_DIFF(cast(cte.End_day_key as DATE),cast(cte.Start_day_key as DATE),DAY) end as Days_Active
    -- CONCAT(cast(s_d.year as string), '-',  cast(s_d.month as string)) AS Cohort
    from  {{ ref('Fact_Subscription') }} s 
    join {{ ref('Dim_Customer_Subscription') }} c on c.Customer_Subscription_Key=s.Customer_Subscription_Key
    join cte on cte.Customer_Subscription_Key=s.Customer_Subscription_Key
    join {{ ref('Dim_Date') }} s_d on cte.Start_day_key=s_d.Date_Key
    left join {{ ref('Dim_Date') }} e_d on cte.End_day_key=e_d.Date_Key

    -- and flavor!='n/a'
    -- having Subscribe_type!='One-Time'


    )

    select 
    cast(customer_id as INT64) as customer_id,
    is_live_customer,
    -- order_id,
    min(Start_day_key) as Start_day_key,
    max(End_day_key) as End_day_key,
    max(Days_from_sub_start) as Days_from_sub_start,
    max(Days_Active) as Days_Active
    from Subscription_Fact_Report 
    -- where customer_id ='11333'
    -- where order_id='33479'
    group by customer_id, is_live_customer
    -- ,order_id
