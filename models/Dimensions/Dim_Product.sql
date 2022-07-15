{{ config(materialized='table') }}
with Dim_Product as (
    with cte as(
        select 
    product_name,
    cast(product_id as int64) as product_id,
    sku,
    order_sku,
    ifnull(Serving_Size,'0') as Serving_Size, 
    product_is_bar,
    product_is_fiber,
    product_is_shake,
    product_is_starter_pack,
    Product_Category,
    case when sku in ('145_20201603','145_20201607') and flavor= 'n/a' then 'error' else lower(flavor) end as flavor
    from  {{ ref('stg_product') }}
    where  product_name !='Default Product' and product_name != 'Chocolate' and product_id!='140' 
    and product_name not like'Balanced%' 
    and product_name not like 'welcome' and product_name not like 'Bundle%' and product_name not like '%Test%' and product_name not like '%Sample%'
    )
    select 
    distinct {{ dbt_utils.surrogate_key(['cte.sku']) }} Product_Key,
    product_name,
    product_id,
    sku,
    Product_Category,
    order_sku,
    flavor,
    Serving_Size, 
    product_is_bar,
    product_is_fiber,
    product_is_shake,
    product_is_starter_pack
    from cte 
    where flavor not like '%chocolate peanut butter%' and flavor not like '%vanilla crème%' 
)

select * from Dim_Product 
