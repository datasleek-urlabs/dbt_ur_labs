{{ config(materialized='table') }}

with Dim_Order as (
    with cte as (
        select order_id,
        rank() over(partition by customer_id order by order_created_date_time ASC) as rank 
        from 
    {{ source('muniqlifebigcommerce','bc_order')}}
    )
    select 
    distinct {{ dbt_utils.surrogate_key(['o.order_id']) }} Order_Key,
    o.order_id,
    o.order_status_id,
    o.order_status,
    o.order_archived,
    case when cte.rank= 1 then 1 else 0 end as initial_order,
    case when cte.rank= 1 then 0 else 1 end as reorder_order,
    b.full_name as billing_full_name,
    b.first_name as billing_first_name,
    b.last_name as billing_last_name,
    b.street_1 as billing_street_1,
    b.street_2 as billing_street_2,
    b.city as billing_city,
    b.state as billing_state,
    b.zip as billing_zip,
    b.country as billing_country,
    b.country_code as billing_country_code,
    ifnull(b.phone,'N/A') as billing_phone,
    b.email as billing_email,
    s.full_name as shipping_full_name,
    s.first_name as shipping_first_name,
    s.last_name as shipping_last_name,
    s.street_1 as shipping_street_1,
    s.street_2 as shipping_street_2,
    s.city as shipping_city,
    s.state as shipping_state,
    s.postal_code as shipping_zip,
    s.country as shipping_country,
    s.country_code as shipping_country_code,
    ifnull(s.phone,'N/A') as shipping_phone,
    s.email as shipping_email,
    ifnull(o.coupon_discount,0.0) as coupon_discount,
    Case when (Case when JSON_VALUE(trim(ol.products,'[]'),'$.product_options[2].display_value') is null 
    and JSON_VALUE(trim(ol.products,'[]'),'$.product_options[1].display_value') in (
        'Send every 4 weeks','Send every 6 weeks'
    )
    then JSON_VALUE(trim(ol.products,'[]'),'$.product_options[1].display_value')
    when JSON_VALUE(trim(ol.products,'[]'),'$.product_options[2].display_value') is null then 'N/A'
    when JSON_VALUE(trim(ol.products,'[]'),'$.product_options[2].display_value') in ('Vanilla Cr\u00e8me', 'chocolate', 
    'Chocolate, Vanilla and Mocha Latte', 'mocha latte', 'vanilla cr\u00e8me', 'chocolate, vanilla and mocha latte', 
    'Chocolate', 'vegan chocolate', 'Vegan Vanilla', 'Vegan Chocolate', 'chocolate and vanilla', 'Vegan Chocolate and Vegan Vanilla', 
    'Chocolate and Vanilla', 'vegan vanilla', 'Mocha latte', 'Mocha Latte', 'Strawberry', 'Chocolate, Vanilla and Strawberry', 
    'Vanilla Cr\u00e8me and Strawberry','Chocolate and Mocha Latte') 
    then JSON_VALUE(trim(ol.products,'[]'),'$.product_options[0].display_value')
    when JSON_VALUE(trim(ol.products,'[]'),'$.product_options[1].display_value') in ('Send every 6 weeks','Send every 4 weeks')
    then JSON_VALUE(trim(ol.products,'[]'),'$.product_options[1].display_value')
    when JSON_VALUE(trim(ol.products,'[]'),'$.product_options[2].display_value') in ('14','42','28')
    then JSON_VALUE(trim(ol.products,'[]'),'$.product_options[0].display_value') 
    else JSON_VALUE(trim(ol.products,'[]'),'$.product_options[2].display_value') end )='N/A' then 'One Time Purchase' else 
    (Case when JSON_VALUE(trim(ol.products,'[]'),'$.product_options[2].display_value') is null 
    and JSON_VALUE(trim(ol.products,'[]'),'$.product_options[1].display_value') in (
        'Send every 4 weeks','Send every 6 weeks'
    )
    then JSON_VALUE(trim(ol.products,'[]'),'$.product_options[1].display_value')
    when JSON_VALUE(trim(ol.products,'[]'),'$.product_options[2].display_value') is null then 'N/A'
    when JSON_VALUE(trim(ol.products,'[]'),'$.product_options[2].display_value') in ('Vanilla Cr\u00e8me', 'chocolate', 
    'Chocolate, Vanilla and Mocha Latte', 'mocha latte', 'vanilla cr\u00e8me', 'chocolate, vanilla and mocha latte', 
    'Chocolate', 'vegan chocolate', 'Vegan Vanilla', 'Vegan Chocolate', 'chocolate and vanilla', 'Vegan Chocolate and Vegan Vanilla', 
    'Chocolate and Vanilla', 'vegan vanilla', 'Mocha latte', 'Mocha Latte', 'Strawberry', 'Chocolate, Vanilla and Strawberry', 
    'Vanilla Cr\u00e8me and Strawberry','Chocolate and Mocha Latte') 
    then JSON_VALUE(trim(ol.products,'[]'),'$.product_options[0].display_value')
    when JSON_VALUE(trim(ol.products,'[]'),'$.product_options[2].display_value') in ('14','42','28')
    then JSON_VALUE(trim(ol.products,'[]'),'$.product_options[0].display_value') 
    else JSON_VALUE(trim(ol.products,'[]'),'$.product_options[2].display_value') end) end
    as Subscribe
    from 
    {{ source('muniqlifebigcommerce','bc_order')}} o
    join cte on o.order_id=cte.order_id
    left join {{ source('muniqlifebigcommerce','bc_order_billing_addresses')}} b on o.order_id=b.order_id
    left join {{ source('muniqlifebigcommerce','bc_order_shipping_addresses')}} s on o.order_id=s.order_id
    left join {{ source('muniqlifebigcommerce','order')}} ol on o.order_id=ol.id
    
)

select *
from Dim_Order