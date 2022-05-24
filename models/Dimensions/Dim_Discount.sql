{{ config(materialized='table') }}

with Dim_Discount as (

    select 
    distinct {{ dbt_utils.surrogate_key(['coupon_id','coupon_code']) }} Discount_Key,
    ifnull(coupon_id,0) as discount_id,
    ifnull(coupon_code,'N/A') as discount_code,
    from {{ source('muniqlifebigcommerce','bc_order')}} 
    
)
select *
from Dim_Discount