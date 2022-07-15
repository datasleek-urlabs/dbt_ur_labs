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
    case when sku in ('145_20201603','145_20201607') and flavor= 'n/a' then 'error' else lower(flavor) end as flavor,
    -- case 
    -- -- when product_name='Muniq Starter Pack' and lower(flavor)='n/a' then 1
    -- -- when product_name='Empowerfull Prebiotic Fiber Blend' and lower(flavor)='n/a' then 1
    -- -- when product_name='Muniq Vegan Starter Pack' and lower(flavor)='n/a' then 1
    -- when product_name='Balanced Nutritional Bars' then 1
    -- -- when product_name= 'Muniq Bars' and lower(flavor)='n/a' then 1 
    -- -- when product_name= 'Prebiotic Resistant Starch Shake Single Serve' and lower(flavor)='n/a' then 1 
    -- -- when product_name= 'Prebiotic Resistant Starch Shake' and flavor ='n/a' then 1 
    -- else 0 end as error_name
    from  {{ ref('stg_product') }}
    where  product_name !='Default Product' and product_name != 'Chocolate' and product_id!='140' 
    and product_name not like'Balanced%' 
    and product_name not like 'welcome' and product_name not like 'Bundle%' and product_name not like '%Test%' and product_name not like '%Sample%'
    -- and product_name not like '%chocolate peanut butter%'
    -- group by 1,2,3,4,5,6,7,8,9,10,11,12
    -- having error_name!= 1
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
    -- and flavor like '%Mocha Latte%'
)

select * from Dim_Product 
-- where order_sku='10201109-3'
-- select product_key, count(*) from Dim_Product group by 1 having count(*)>1
-- select * from Dim_Product where product_key='d9b871278c92c84d8518475957adf61f'
-- where Product_Key= 'd1a9f312c913e18f5048964269bba119'
-- where flavor like 'mocha%'
-- select product_key,product_name,count(*)
-- from Dim_Product group by 1,2 having count(*)>1
-- -- where product_name='Balanced Nutritional Bars'
