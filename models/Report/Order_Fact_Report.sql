{{ config(materialized='table') }}

with Order_Fact_Report as (
    with cte as (
    select
    o.order_id,
    p.sku,
    case when p.product_name like '% Starter Pack' then 'Starter Pack'
    when do.Subscribe like 'One Time%' then 'One-Time'
    else 'Subscribe' end as Order_type
    from {{ ref('Fact_Order') }} o 
    join {{ ref('Dim_Product') }} p on o.Product_Key = p.Product_Key
    join {{ ref('Dim_Order') }} do on do.Order_Key = o.Order_Key )
    
    select 
    o.order_id,
    cast(c.customer_id as INT64) as customer_id,
    cte.Order_type as Order_type,
    c.first_name,
    c.last_name,
    c.email as customer_email,
    o.Order_Date_Key,
    o.sku,
    -- p.flavor,
    -- do.Subscribe as Ship_Frequency,
    -- p.Serving_Size,
    -- p.product_name as Packaging,
    max(p.product_is_bar) as product_is_bar,
    max(p.product_is_fiber) as product_is_fiber,
    max(p.product_is_shake) as product_is_shake,
    -- max(p.product_is_shake) as product_is_shake,
    max(p.product_is_starter_pack) as product_is_starter_pack ,
    max(case when cte.Order_type='One-Time'then 1 else 0 end) as is_one_time,
    max(case when cte.Order_type='Starter Pack'then 1 else 0 end) as is_starter_pack,
    max(case when cte.Order_type='Subscribe'then 1 else 0 end) as is_subscription
    
    -- do.initial_order,
    -- p.Product_Category,
    -- case when do.Subscribe != 'One Time Purchase' then true else false end as Valid
    from
    {{ ref('Fact_Order') }} o 
    join cte on cte.order_id=o.order_id and cte.sku=o.sku
    join {{ ref('Dim_Product') }} p on o.Product_Key = p.Product_Key
    join {{ ref('Dim_Order') }} do on do.Order_Key = o.Order_Key
    join {{ ref('Dim_Customer') }} c on o.Customer_Key =c.Customer_Key
    where do.initial_order=1 
    group by 1,2,3,4,5,6,7,8
    -- and flavor!='n/a'
    -- having Subscribe_type!='One-Time'


    )

select *
from Order_Fact_Report 
-- where is_starter_pack =1 
-- where order_id='226402'
-- where customer_id =11333

-- -- order_id='where order_id='58966''
-- -- where order_id='17131'