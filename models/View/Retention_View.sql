{{ config(materialized='view') }}

with Retention_View as (

    
    select 
    o.order_id,
    s.customer_id,
    s.is_live_customer,
    s.Start_day_key,
    s.End_day_key,
    s.Days_from_sub_start,
    s.Days_Active,
    -- o.order_id as order_fact_order_id,
    -- o.Order_type,
    -- o.flavor,
    -- o.Ship_Frequency,
    -- o.Serving_Size,
    -- -- o.Packaging,
    max(o.product_is_bar) as product_is_bar,
    max(o.product_is_fiber) as product_is_fiber,
    max(o.product_is_shake) as product_is_shake,
    max(o.product_is_starter_pack) as product_is_starter_pack,
    max(case when o.Order_type='One-Time'then 1 else 0 end) as is_one_time,
    max(case when o.Order_type='Starter Pack'then 1 else 0 end) as is_starter_pack,
    max(case when o.Order_type='Subscribe'then 1 else 0 end) as is_subscription
    -- o.Product_Category,
    -- o.sku,
    -- o.Valid,
    -- o.Product_Category,
    -- s.sku,
    -- s.Start_Year,
    -- s.Start_Month,
    -- s.Start_Day,
    -- s.End_Year,
    -- s.End_Month,
    -- s.End_Day,
    -- s.End_Day_AM_PM,
    -- s.End_Hour,
    
    -- sum(s.customer_id) over(s.Start_day_key) as Daily_Cohort
    from  {{ ref('Subscription_Fact_Report') }} s 
    join {{ ref('Subscription_Initial_Report') }} o on s.customer_id = o.customer_id
    group by 1,2,3,4,5,6,7
    -- group by s.customer_id
    -- s.order_id=o.order_id and s.sku=o.sku
    -- where do.initial_order=1 
    -- and flavor!='n/a'
    -- having Subscribe_type!='One-Time'
    )
select * from Retention_View 
-- where is_one_time=1
-- where customer_id=11333
-- where order_id='226402'
-- select count(*) 
-- select order_id1, count(*)
-- from Retention_View 
-- group by order_id1
-- having count(*)>1