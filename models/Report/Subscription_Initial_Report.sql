{{ config(materialized='table') }}
with sub_initial as (
    select cast(bc.order_id as string) as order_id,
        -- rank() over(partition by bc.customer_id order by bc.order_created_date_time ASC) as rank,
        bc.order_created_date_time as created_time
        from 
    join {{ ref('Fact_Order') }} o  on cast(bc.order_id as string)=o.order_id
    join {{ ref('Dim_Product') }} p on o.Product_Key = p.Product_Key
    join {{ ref('Dim_Order') }} do on do.Order_Key = o.Order_Key 
    where p.product_name not like '% Starter Pack' and do.Subscribe not like 'One Time%'
    -- where o.Customer_Key ='ba530cdf0a884348613f2aaa3a5ba5e8'
    )

  ,cte as (
    select
    o.order_id,
    o.Customer_Key,
    p.sku,
    si.created_time,
    rank() over(partition by o.Customer_Key order by o.Order_Date_Key ASC,o.Order_Time_Key ASC) as sub_initial_order
    from {{ ref('Fact_Order') }} o 
    join sub_initial si on si.order_id=o.order_id
    join {{ ref('Dim_Product') }} p on o.Product_Key = p.Product_Key
    join {{ ref('Dim_Order') }} do on do.Order_Key = o.Order_Key 

    )
    select 
    o.order_id,
    cast(c.customer_id as INT64) as customer_id,
    si.Order_type as Order_type,
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
    -- max(case when sub_initial.Order_type='One-Time'then 1 else 0 end) as is_one_time,
    max(case when si.Order_type='Starter Pack'then 1 else 0 end) as is_starter_pack,
    max(case when si.Order_type='Subscribe'then 1 else 0 end) as is_subscription
    from {{ ref('Fact_Order') }} o 
    join sub_initial si on si.order_id=o.order_id
    join cte on cte.order_id=o.order_id and cte.sku=o.sku
    join {{ ref('Dim_Product') }}  p on o.Product_Key = p.Product_Key
    join {{ ref('Dim_Order') }} do on do.Order_Key = o.Order_Key
    join {{ ref('Dim_Customer') }}c on o.Customer_Key =c.Customer_Key
    where cte.sub_initial_order=1 
    and c.customer_id=53125
    group by 1,2,3,4,5,6,7,8
