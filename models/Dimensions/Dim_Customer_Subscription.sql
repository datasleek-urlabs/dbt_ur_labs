{{ config(materialized='table') }}

with Dim_Customer_Subscription as (

    select 
    distinct {{ dbt_utils.surrogate_key(['customer']) }} Customer_Subscription_Key,
    customer,
    email,
    first_name,
    last_name
    from 
    {{ source('muniqlifebigcommerce','og_subs')}}
    
)

select *
from Dim_Customer_Subscription