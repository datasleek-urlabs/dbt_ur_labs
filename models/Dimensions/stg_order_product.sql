{{ config(materialized='table') }}
    --  select distinct 
    -- id as order_id,
    -- JSON_VALUE(trim(products,'[]'),'$.product_id') as product_id,
    -- cast(JSON_VALUE(trim(products,'[]'),'$.sku')as string) as order_sku,
    -- CONCAT(cast(JSON_VALUE(trim(products,'[]'),'$.product_id') as string) , '_',cast(JSON_VALUE(trim(products,'[]'),'$.sku')as string)) as sku
    -- from 
    -- {{ source('muniqlifebigcommerce','order')}} 
    -- where products is not null
    -- and id=226451

    with base as (
    select id as order_id,json_extract_array(products, '$') as products from `muniq-277202`.`muniqlifebigcommerce`.`order` 
    --   where id = 226451
    )
    select distinct
        base.order_id,
        json_value(pro, '$.product_id') as product_id,
        json_value(pro, '$.sku') as order_sku,
        CONCAT(cast(json_value(pro, '$.product_id') as string) , '_',cast(json_value(pro, '$.sku') as string)) as sku
        from 
        base,
        unnest(products) as pro