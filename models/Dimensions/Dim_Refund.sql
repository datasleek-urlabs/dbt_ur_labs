{{ config(materialized='table') }}

with Dim_Refund as (

    select 
    distinct {{ dbt_utils.surrogate_key(['refund_id']) }} Refund_Key,
    refund_id,
    refund_created_date_time,
    refund_total_amount,
    last_updated_datetime
    from 
    {{ source('muniqlifebigcommerce','bc_refund')}}
  
    
)

select *
from Dim_Refund