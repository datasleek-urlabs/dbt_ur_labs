
{{ config(materialized='table') }}

with Subscription_Fact_Report as (

    with cte as(
        select 
        s.Customer_Subscription_Key,
        MIN(s.Start_Key)  as Start_Day_key,
        max(s.End_Date_Key)as End_Day_key,
        max(s.End_Time_Key)as End_Time_key,
        max(s.is_live) as is_live_customer,
        from {{ ref('Fact_Subscription') }} s 
        group by 1

    )

    select 
    c.customer as customer_id,
    cte.is_live_customer,
    s.order_id,
    cte.Start_Day_key,
    case when cte.is_live_customer = true then null else cte.End_Day_key end as End_Day_key,
    case when cte.is_live_customer = true then null else cte.End_Time_Key end as End_Time_Key,
    DATE_DIFF(CURRENT_DATE(),cast(cte.Start_Day_key as DATE),DAY) as Days_from_sub_start,
    case when s.is_live = true then null else DATE_DIFF(cast(cte.End_Day_key as DATE),cast(cte.Start_Day_key as DATE),DAY) end as Days_Active
    from  {{ ref('Fact_Subscription') }} s 
    join {{ ref('Dim_Customer_Subscription') }} c on c.Customer_Subscription_Key=s.Customer_Subscription_Key
    join cte on cte.Customer_Subscription_Key=s.Customer_Subscription_Key
 
    -- left join {{ ref('Dim_Date') }} e_d on cte.End_Day_key=e_d.Date_Key
    -- left join {{ source('Urlabs_DW_Amy','Dim_TimeOfDay') }} e_t on cte.End_Time_Key=e_t.hour_24 

)
select * from Subscription_Fact_Report 
-- where is_live_customer = true and  End_Day_key is not null