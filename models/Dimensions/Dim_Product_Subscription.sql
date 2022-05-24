{{ config(materialized='table') }}

with Dim_Product_Subscription as (

    select 
    distinct {{ dbt_utils.surrogate_key(['substring(product,0,3)','product_name']) }} Product_Subscription_Key,
    substring(product,0,3) as product_id,
    product_name
    from 
    {{ source('muniqlifebigcommerce','og_subs')}}
    
)
select *
from Dim_Product_Subscription