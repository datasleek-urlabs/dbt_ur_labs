

{{ config(materialized='view') }}

with Retention_View as (

    select 
    
    from  {{ source('muniqlifebigcommerce','bc_order')}} o
    join {{ ref('Dim_Customer') }} c on o.customer_id=c.customer_id
    join {{ source('muniqlifebigcommerce','bc_order_line_items')}} l on l.order_id=o.order_id
    join {{ ref('Dim_Product') }} p on p.variant_id=l.variant_id
    join {{ source('Urlabs_DW_Amy','Dim_Date')}} dd_s on dd_s.full_date=DATE(o.order_created_date_time)
    join {{ source('Urlabs_DW_Amy','Dim_Date')}} dd_e on dd_e.full_date= DATE(o.date_shipped)
    join {{ source('Urlabs_DW_Amy','Dim_TimeOfDay')}} tt_s on tt_s.hour_24= EXTRACT(HOUR FROM (timestamp (o.order_created_date_time) ) )
    join {{ source('Urlabs_DW_Amy','Dim_TimeOfDay')}} tt_e on tt_e.hour_24= EXTRACT(HOUR FROM (timestamp (o.date_shipped) ) )
    join {{ ref('Dim_Order') }} do on do.order_id=o.order_id
    join {{ ref('Dim_Discount') }} d on ifnull(o.coupon_id,0)=d.discount_id
    join {{ source('muniqlifebigcommerce','bc_refund')}} rt on rt.order_id=o.order_id
    left join {{ ref('Dim_Refund') }} f on ifnull(rt.refund_id,0)=f.refund_id
    where o.order_id !=11031

    )

select *
from Fact_Subscription