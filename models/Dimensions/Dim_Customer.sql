{{ config(materialized='table') }}

with Dim_Customer as (

    select 
    distinct {{ dbt_utils.surrogate_key(['c.customer_id']) }} Customer_Key,
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    ifnull(c.phone,'N/A') as phone,
    case when c.notes= 'isGuest' then 1 else 0 end as is_guest,
    c.date_created,
    c.date_modified,
    c.last_updated_datetime,
    c.store_credit_in_USD as credit
    from 
    {{ source('muniqlifebigcommerce','bc_customer')}} c
    -- join {{ source('muniqlifebigcommerce','bc_order')}} o on o.customer_id=c.customer_id
    where c.email not in (
        select email from {{ source('Urlabs_DW_Amy','stg_exclude_email')}} 
    )
    
)

select *
from Dim_Customer