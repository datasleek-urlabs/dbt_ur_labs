

{{ config(materialized='table') }}

with Fact_Order as (

    select 
    c.Customer_Key,
    -- ps.Product_Key,
    dd_s.Date_Key AS Order_Date_Key,
    dd_e.Date_Key as Shipped_Date_Key,
    tt_s.hour_24 as Order_Time_Key,
    tt_e.hour_24 as Shipped_Time_Key,
    do.Order_Key,
    ifnull(f.Refund_Key,'N/A') as Refund_Key,
    d.Discount_Key,
    p.Product_Key,
    cast(o.order_id as string) as order_id,
    p.sku,
    p.product_name,
    o.payment_status,
    o.payment_method_type,
    o.total_items,
    o.total_items_shipped,
    o.gift_certificate_amount_redeemed,
    o.store_credit_redeemed,
    o.sub_total_excluding_tax,
    o.sub_total_including_tax,
    o.sub_total_tax,
    o.default_currency_code,
    o.last_updated_datetime,
    do.initial_order,
    c.customer_id,
    1 as TOTAL_ORDER
    from  {{ source('muniqlifebigcommerce','bc_order')}} o
    join {{ ref('Dim_Customer') }} c on o.customer_id=c.customer_id
    join {{ ref('stg_order_product')}} stg on stg.order_id=o.order_id 
    -- join {{ source('muniqlifebigcommerce','order')}} l on l.id=o.order_id
    join {{ ref('Dim_Product') }} p on p.sku=stg.sku
    join {{ ref('Dim_Date') }} dd_s on dd_s.full_date=DATE(o.order_created_date_time)
    left join {{ ref('Dim_Date') }} dd_e on dd_e.full_date= DATE(o.date_shipped)
    join {{ source('Urlabs_DW_Amy','Dim_TimeOfDay')}} tt_s on tt_s.hour_24= EXTRACT(HOUR FROM (timestamp (o.order_created_date_time) ) )
    left join {{ source('Urlabs_DW_Amy','Dim_TimeOfDay')}} tt_e on tt_e.hour_24= EXTRACT(HOUR FROM (timestamp (o.date_shipped) ) )
    join {{ ref('Dim_Order') }} do on do.order_id=o.order_id and do.sku=stg.sku

    join {{ ref('Dim_Discount') }} d on ifnull(o.coupon_id,0)=d.discount_id
    left join {{ source('muniqlifebigcommerce','bc_refund')}} rt on rt.order_id=o.order_id
    left join {{ ref('Dim_Refund') }} f on ifnull(rt.refund_id,0)=f.refund_id
    where o.order_id !=11031 and do.order_status not in ('Declined', 'Pending','Incomplete','Cancelled')

    )

select *
from Fact_Order 
-- where order_id='226451'
-- where customer_id=11333
-- order by order_Date_Key limit 1
-- order by order_Date_Key