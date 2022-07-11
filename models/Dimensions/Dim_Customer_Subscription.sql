{{ config(materialized='table') }}

with Dim_Customer_Subscription as (

    select 
    distinct {{ dbt_utils.surrogate_key(['customer']) }} Customer_Subscription_Key,
    customer,
    -- email,
    first_name,
    last_name
    from 
    {{ source('muniqlifebigcommerce','og_subs')}} 
    where email not in (
        select email from {{ source('Urlabs_DW_Amy','stg_exclude_email')}} 
    )
    
)

select *
from Dim_Customer_Subscription 
-- where customer='56350'