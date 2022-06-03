{{ config(materialized='table') }}

with Dim_Product as (
    with cte as(
    select 
    id as id,
    JSON_VALUE(trim(products,'[]'),'$.name') as product_name,
    JSON_VALUE(trim(products,'[]'),'$.variant_id') as variant_id,
    JSON_VALUE(trim(products,'[]'),'$.product_id') as product_id,
    Case when JSON_VALUE(trim(products,'[]'),'$.product_options[0].display_value') in ('Vanilla Cr\u00e8me', 'chocolate', 
    'Chocolate, Vanilla and Mocha Latte', 'mocha latte', 'vanilla cr\u00e8me', 'chocolate, vanilla and mocha latte', 
    'Chocolate', 'vegan chocolate', 'Vegan Vanilla', 'Vegan Chocolate', 'chocolate and vanilla', 'Vegan Chocolate and Vegan Vanilla', 
    'Chocolate and Vanilla', 'vegan vanilla', 'Mocha latte', 'Mocha Latte', 'Strawberry', 'Chocolate, Vanilla and Strawberry', 
    'Vanilla Cr\u00e8me and Strawberry','Chocolate and Mocha Latte') 
    then JSON_VALUE(trim(products,'[]'),'$.product_options[1].display_value')
    when JSON_VALUE(trim(products,'[]'),'$.product_options[0].display_value') in ('12','14','24','36','42','28')
    then JSON_VALUE(trim(products,'[]'),'$.product_options[0].display_value')
    when JSON_VALUE(trim(products,'[]'),'$.product_options[1].display_value') in ('12','14','24','36','42','28')
    then JSON_VALUE(trim(products,'[]'),'$.product_options[1].display_value')
    when JSON_VALUE(trim(products,'[]'),'$.name')='Muniq Starter Pack' then '0'
    when JSON_VALUE(trim(products,'[]'),'$.name')='Muniq Vegan Starter Pack' then '0'
    else JSON_VALUE(trim(products,'[]'),'$.product_options[2].display_value') end as Quantity,

    Case when JSON_VALUE(trim(products,'[]'),'$.product_options[2].display_value') in ('Vanilla Cr\u00e8me', 'chocolate', 
    'Chocolate, Vanilla and Mocha Latte', 'mocha latte', 'vanilla cr\u00e8me', 'chocolate, vanilla and mocha latte', 
    'Chocolate', 'vegan chocolate', 'Vegan Vanilla', 'Vegan Chocolate', 'chocolate and vanilla', 'Vegan Chocolate and Vegan Vanilla', 
    'Chocolate and Vanilla', 'vegan vanilla', 'Mocha latte', 'Mocha Latte', 'Strawberry', 'Chocolate, Vanilla and Strawberry', 
    'Vanilla Cr\u00e8me and Strawberry','Chocolate and Mocha Latte') then 
    JSON_VALUE(trim(products,'[]'),'$.product_options[2].display_value')
    when JSON_VALUE(trim(products,'[]'),'$.product_options[1].display_value') not in ('Vanilla Cr\u00e8me', 'chocolate', 
    'Chocolate, Vanilla and Mocha Latte', 'mocha latte', 'vanilla cr\u00e8me', 'chocolate, vanilla and mocha latte', 
    'Chocolate', 'vegan chocolate', 'Vegan Vanilla', 'Vegan Chocolate', 'chocolate and vanilla', 'Vegan Chocolate and Vegan Vanilla', 
    'Chocolate and Vanilla', 'vegan vanilla', 'Mocha latte', 'Mocha Latte', 'Strawberry', 'Chocolate, Vanilla and Strawberry', 
    'Vanilla Cr\u00e8me and Strawberry','Chocolate and Mocha Latte') and
    JSON_VALUE(trim(products,'[]'),'$.product_options[0].display_value')
     not in ('Vanilla Cr\u00e8me', 'chocolate', 
    'Chocolate, Vanilla and Mocha Latte', 'mocha latte', 'vanilla cr\u00e8me', 'chocolate, vanilla and mocha latte', 
    'Chocolate', 'vegan chocolate', 'Vegan Vanilla', 'Vegan Chocolate', 'chocolate and vanilla', 'Vegan Chocolate and Vegan Vanilla', 
    'Chocolate and Vanilla', 'vegan vanilla', 'Mocha latte', 'Mocha Latte', 'Strawberry', 'Chocolate, Vanilla and Strawberry', 
    'Vanilla Cr\u00e8me and Strawberry','Chocolate and Mocha Latte') 
    then 'N/A'
    when JSON_VALUE(trim(products,'[]'),'$.product_options[1].display_value') not in ('Vanilla Cr\u00e8me', 'chocolate', 
    'Chocolate, Vanilla and Mocha Latte', 'mocha latte', 'vanilla cr\u00e8me', 'chocolate, vanilla and mocha latte', 
    'Chocolate', 'vegan chocolate', 'Vegan Vanilla', 'Vegan Chocolate', 'chocolate and vanilla', 'Vegan Chocolate and Vegan Vanilla', 
    'Chocolate and Vanilla', 'vegan vanilla', 'Mocha latte', 'Mocha Latte', 'Strawberry', 'Chocolate, Vanilla and Strawberry', 
    'Vanilla Cr\u00e8me and Strawberry','Chocolate and Mocha Latte') then 
    JSON_VALUE(trim(products,'[]'),'$.product_options[0].display_value')
    
    
    else JSON_VALUE(trim(products,'[]'),'$.product_options[1].display_value'
   ) end
--    ) = 'Send every 6 weeks' , 'N/A', JSON_VALUE(trim(products,'[]'),'$.product_options[1].display_value'))
    as flavor,
-- ‘send 4 week'
    from 
    {{ source('muniqlifebigcommerce','order')}} 
    where products is not null
    )
    -- join cte on JSON_VALUE(trim(o.products,'[]'),'$.name') = cte.name

    select 
    distinct {{ dbt_utils.surrogate_key(['variant_id']) }} Product_Key,
    product_name,
    cast(product_id as int64)as product_id,
    cast(variant_id as int64) as variant_id,
    ifnull(Quantity,'0') as Quantity, 
    case when variant_id in ('429','423','460','461','546','542','519','387','537','520','462') and flavor='N/A' then 'Error' else lower(flavor) end as flavor
    from cte 
    where variant_id != '0' and product_id!='140' and product_name !='Default Product' and product_name != 'Chocolate'
    group by 1,2,3,4,5,6 
    having flavor!='Error'
    
)

select 
*
from Dim_Product 
-- group by 1 having count(*)>1



-- select * from Dim_Product where variant_id=460