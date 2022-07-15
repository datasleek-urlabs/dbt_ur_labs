{{ config(materialized='view') }}

with Retention_View as (

    
    select 
    
    s.customer_id,
    s.is_live_customer,
    s.Start_day_key,
    s.End_day_key,
    s.Days_from_sub_start,
    s.Days_Active,
    o.order_id,
    max(o.product_is_bar) as product_is_bar,
    max(o.product_is_fiber) as product_is_fiber,
    max(o.product_is_shake) as product_is_shake,
    max(o.product_is_starter_pack) as product_is_starter_pack,
    max(o.is_one_time) as is_one_time,
    max(o.is_starter_pack) as is_starter_pack,
    max(o.is_subscription) as is_subscription
    from  {{ ref('Subscription_Fact_Report') }} s 
    join {{ ref('Order_Fact_Report') }} o on cast(s.customer_id as string) = cast(o.customer_id as string)
    group by 1,2,3,4,5,6,7
    )
select * from Retention_View 
-- where is_one_time=1
-- where customer_id=16323
-- where order_id='226402'
-- select count(*) 
-- select order_id1, count(*)
-- from Retention_View 
-- group by order_id1
-- having count(*)>1