{{ config(materialized='table') }}

 with base as (
    select json_extract_array(products, '$') as products from `muniq-277202`.`muniqlifebigcommerce`.`order` 
    )
    select distinct
        JSON_VALUE(pro,'$.name') as product_name,
    JSON_VALUE(pro,'$.variant_id') as variant_id,
    JSON_VALUE(pro,'$.product_id') as product_id,
    CONCAT(cast(JSON_VALUE(pro,'$.product_id') as string) , '_',cast(JSON_VALUE(pro,'$.sku')as string)) as sku,
    cast(JSON_VALUE(pro,'$.sku')as string) as order_sku,
    Case when JSON_VALUE(pro,'$.product_options[0].display_value') in ('Vanilla Cr\u00e8me', 'chocolate', 
    'Chocolate, Vanilla and Mocha Latte', 'mocha latte', 'vanilla cr\u00e8me', 'chocolate, vanilla and mocha latte', 
    'Chocolate', 'vegan chocolate', 'Vegan Vanilla', 'Vegan Chocolate', 'chocolate and vanilla', 'Vegan Chocolate and Vegan Vanilla', 
    'Chocolate and Vanilla', 'vegan vanilla', 'Mocha latte', 'Mocha Latte', 'Strawberry', 'Chocolate, Vanilla and Strawberry', 
    'Vanilla Cr\u00e8me and Strawberry','Chocolate and Mocha Latte','Peanut Butter Chocolate and Strawberry Almond'
    ,'Chocolate Brownie, Peanut Butter Chocolate, and Strawberry Almond','Strawberry Almond','Peanut Butter Chocolate',
    'Chocolate Brownie and Peanut Butter Chocolate','Chocolate Brownie','Chocolate Brownie and Strawberry Almond','Chocolate Peanut Butter and Strawberry Almond') 
    then JSON_VALUE(pro,'$.product_options[1].display_value')
    when JSON_VALUE(pro,'$.product_options[0].display_value') in ('12','14','24','36','42','28','56')
    then JSON_VALUE(pro,'$.product_options[0].display_value')
    when JSON_VALUE(pro,'$.product_options[1].display_value') in ('12','14','24','36','42','28','56')
    then JSON_VALUE(pro,'$.product_options[1].display_value')
    when JSON_VALUE(pro,'$.name')='Muniq Starter Pack' then '0'
    when JSON_VALUE(pro,'$.name')='Muniq Vegan Starter Pack' then '0'
    else JSON_VALUE(pro,'$.product_options[2].display_value') end as Serving_size,

    Case 
    when cast(JSON_VALUE(pro,'$.sku')as string) = '10201711' then 'N/A'
    when JSON_VALUE(pro,'$.name')='Muniq Starter Pack' then 'N/A'
    when JSON_VALUE(pro,'$.name')='Empowerfull Prebiotic Fiber Blend' then 'N/A'
    when JSON_VALUE(pro,'$.name')='Supergut Prebiotic Fiber Blend' then 'N/A'
    when JSON_VALUE(pro,'$.product_options[0].display_value') in ('Vanilla Cr\u00e8me', 'chocolate', 
    'Chocolate, Vanilla and Mocha Latte', 'mocha latte', 'vanilla cr\u00e8me', 'chocolate, vanilla and mocha latte', 
    'Chocolate', 'vegan chocolate', 'Vegan Vanilla', 'Vegan Chocolate', 'chocolate and vanilla', 'Vegan Chocolate and Vegan Vanilla', 
    'Chocolate and Vanilla', 'vegan vanilla', 'Mocha latte', 'Mocha Latte', 'Strawberry', 'Chocolate, Vanilla and Strawberry', 
    'Vanilla Cr\u00e8me and Strawberry','Chocolate and Mocha Latte','Peanut Butter Chocolate and Strawberry Almond'
    ,'Chocolate Brownie, Peanut Butter Chocolate, and Strawberry Almond','Strawberry Almond','Peanut Butter Chocolate',
    'Chocolate Brownie and Peanut Butter Chocolate','Chocolate Brownie','Chocolate Brownie and Strawberry Almond',
    'Chocolate Peanut Butter and Strawberry Almond') then 
    JSON_VALUE(pro,'$.product_options[0].display_value')
    when JSON_VALUE(pro,'$.product_options[1].display_value') in ('Vanilla Cr\u00e8me', 'chocolate', 
    'Chocolate, Vanilla and Mocha Latte', 'mocha latte', 'vanilla cr\u00e8me', 'chocolate, vanilla and mocha latte', 
    'Chocolate', 'vegan chocolate', 'Vegan Vanilla', 'Vegan Chocolate', 'chocolate and vanilla', 'Vegan Chocolate and Vegan Vanilla', 
    'Chocolate and Vanilla', 'vegan vanilla', 'Mocha latte', 'Mocha Latte', 'Strawberry', 'Chocolate, Vanilla and Strawberry', 
    'Vanilla Cr\u00e8me and Strawberry','Chocolate and Mocha Latte','Peanut Butter Chocolate and Strawberry Almond'
    ,'Chocolate Brownie, Peanut Butter Chocolate, and Strawberry Almond','Strawberry Almond','Peanut Butter Chocolate',
    'Chocolate Brownie and Peanut Butter Chocolate','Chocolate Brownie','Chocolate Brownie and Strawberry Almond',
    'Chocolate Peanut Butter and Strawberry Almond') then 
    JSON_VALUE(pro,'$.product_options[1].display_value')
    when JSON_VALUE(pro,'$.product_options[2].display_value') in ('Vanilla Cr\u00e8me', 'chocolate', 
    'Chocolate, Vanilla and Mocha Latte', 'mocha latte', 'vanilla cr\u00e8me', 'chocolate, vanilla and mocha latte', 
    'Chocolate', 'vegan chocolate', 'Vegan Vanilla', 'Vegan Chocolate', 'chocolate and vanilla', 'Vegan Chocolate and Vegan Vanilla', 
    'Chocolate and Vanilla', 'vegan vanilla', 'Mocha latte', 'Mocha Latte', 'Strawberry', 'Chocolate, Vanilla and Strawberry', 
    'Vanilla Cr\u00e8me and Strawberry','Chocolate and Mocha Latte','Peanut Butter Chocolate and Strawberry Almond'
    ,'Chocolate Brownie, Peanut Butter Chocolate, and Strawberry Almond','Strawberry Almond','Peanut Butter Chocolate',
    'Chocolate Brownie and Peanut Butter Chocolate','Chocolate Brownie','Chocolate Brownie and Strawberry Almond',
    'Chocolate Peanut Butter and Strawberry Almond') then 
    JSON_VALUE(pro,'$.product_options[2].display_value')
    when JSON_VALUE(pro,'$.product_options[1].display_value') not in ('Vanilla Cr\u00e8me', 'chocolate', 
    'Chocolate, Vanilla and Mocha Latte', 'mocha latte', 'vanilla cr\u00e8me', 'chocolate, vanilla and mocha latte', 
    'Chocolate', 'vegan chocolate', 'Vegan Vanilla', 'Vegan Chocolate', 'chocolate and vanilla', 'Vegan Chocolate and Vegan Vanilla', 
    'Chocolate and Vanilla', 'vegan vanilla', 'Mocha latte', 'Mocha Latte', 'Strawberry', 'Chocolate, Vanilla and Strawberry', 
    'Vanilla Cr\u00e8me and Strawberry','Chocolate and Mocha Latte','Peanut Butter Chocolate and Strawberry Almond'
    ,'Chocolate Brownie, Peanut Butter Chocolate, and Strawberry Almond','Strawberry Almond','Peanut Butter Chocolate',
    'Chocolate Brownie and Peanut Butter Chocolate','Chocolate Brownie','Chocolate Brownie and Strawberry Almond',
    'Chocolate Peanut Butter and Strawberry Almond') then 
    JSON_VALUE(pro,'$.product_options[0].display_value')
    when JSON_VALUE(pro,'$.product_options[2].display_value') in ('Vanilla Cr\u00e8me', 'chocolate', 
    'Chocolate, Vanilla and Mocha Latte', 'mocha latte', 'vanilla cr\u00e8me', 'chocolate, vanilla and mocha latte', 
    'Chocolate', 'vegan chocolate', 'Vegan Vanilla', 'Vegan Chocolate', 'chocolate and vanilla', 'Vegan Chocolate and Vegan Vanilla', 
    'Chocolate and Vanilla', 'vegan vanilla', 'Mocha latte', 'Mocha Latte', 'Strawberry', 'Chocolate, Vanilla and Strawberry', 
    'Vanilla Cr\u00e8me and Strawberry','Chocolate and Mocha Latte','Peanut Butter Chocolate and Strawberry Almond'
    ,'Chocolate Brownie, Peanut Butter Chocolate, and Strawberry Almond','Strawberry Almond','Peanut Butter Chocolate',
    'Chocolate Brownie and Peanut Butter Chocolate','Chocolate Brownie','Chocolate Brownie and Strawberry Almond','Chocolate Peanut Butter and Strawberry Almond') then 
    JSON_VALUE(pro,'$.product_options[2].display_value')
    when JSON_VALUE(pro,'$.product_options[1].display_value') not in ('Vanilla Cr\u00e8me', 'chocolate', 
    'Chocolate, Vanilla and Mocha Latte', 'mocha latte', 'vanilla cr\u00e8me', 'chocolate, vanilla and mocha latte', 
    'Chocolate', 'vegan chocolate', 'Vegan Vanilla', 'Vegan Chocolate', 'chocolate and vanilla', 'Vegan Chocolate and Vegan Vanilla', 
    'Chocolate and Vanilla', 'vegan vanilla', 'Mocha latte', 'Mocha Latte', 'Strawberry', 'Chocolate, Vanilla and Strawberry', 
    'Vanilla Cr\u00e8me and Strawberry','Chocolate and Mocha Latte','Peanut Butter Chocolate and Strawberry Almond'
    ,'Chocolate Brownie, Peanut Butter Chocolate, and Strawberry Almond','Strawberry Almond','Peanut Butter Chocolate',
    'Chocolate Peanut Butter and Strawberry Almond', 'Chocolate Brownie and Peanut Butter Chocolate','Chocolate Brownie',
    'Chocolate Brownie and Strawberry Almond') and
    JSON_VALUE(pro,'$.product_options[0].display_value')
     not in ('Vanilla Cr\u00e8me', 'chocolate', 
    'Chocolate, Vanilla and Mocha Latte', 'mocha latte', 'vanilla cr\u00e8me', 'chocolate, vanilla and mocha latte', 
    'Chocolate', 'vegan chocolate', 'Vegan Vanilla', 'Vegan Chocolate', 'chocolate and vanilla', 'Vegan Chocolate and Vegan Vanilla', 
    'Chocolate and Vanilla', 'vegan vanilla', 'Mocha latte', 'Mocha Latte', 'Strawberry', 'Chocolate, Vanilla and Strawberry', 
    'Vanilla Cr\u00e8me and Strawberry','Chocolate and Mocha Latte','Peanut Butter Chocolate and Strawberry Almond'
    ,'Chocolate Brownie, Peanut Butter Chocolate, and Strawberry Almond','Strawberry Almond','Peanut Butter Chocolate',
    'Chocolate Brownie and Peanut Butter Chocolate','Chocolate Brownie','Chocolate Brownie and Strawberry Almond',
    'Chocolate Peanut Butter and Strawberry Almond') 
    then 'N/A'
    else JSON_VALUE(pro,'$.product_options[1].display_value'
   ) end
    as flavor,
    case 
    when JSON_VALUE(pro,'$.name') like '%Bar%' then 1 else 0 end as product_is_bar,
    case 
    when JSON_VALUE(pro,'$.name') like '%Fiber%' then 1 else 0 end as product_is_fiber,
    case 
    when JSON_VALUE(pro,'$.name') like '%Shake%' then 1 else 0 end as product_is_shake,
    case 
    when JSON_VALUE(pro,'$.name') like '%Starter%Pack%' then 1 else 0 end as product_is_starter_pack,
    case 
    when JSON_VALUE(pro,'$.name') like '%Bar%' then 'Bar' 
    when JSON_VALUE(pro,'$.name') like '%Fiber%' then 'Fiber' 
    when JSON_VALUE(pro,'$.name') like '%Shake%' then 'Shake'
    when JSON_VALUE(pro,'$.name') like '%Starter%Pack%' then 'Starter Pack'
    when JSON_VALUE(pro,'$.name') like '%Test%' then 'Test'
    when JSON_VALUE(pro,'$.name') like '%Sample%Packs%' then 'Sample Packs'
    when JSON_VALUE(pro,'$.name') like '%Bundle%' then 'Bundle'
    else 'Others'
    end as Product_Category
        from 
        base,
        unnest(products) as pro
        where base.products is not null and
        CONCAT(cast(JSON_VALUE(pro,'$.product_id') as string), '_',cast(JSON_VALUE(pro,'$.sku')as string)) is not null
        
        -- and JSON_VALUE(pro,'$.sku')='10201711'
        and JSON_VALUE(pro,'$.name') like 'Balanced%'
        -- and JSON_VALUE(pro,'$.sku') ='20201601'
        -- '10201613'
        -- and JSON_VALUE(pro,'$.name')  like '%Shake%'

