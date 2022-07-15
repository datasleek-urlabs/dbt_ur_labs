{{ config(materialized='table') }}
    with base as (
    select id as order_id,json_extract_array(products, '$') as products from `muniq-277202`.`muniqlifebigcommerce`.`order` 
    )
    select distinct
        base.order_id,
        json_value(pro, '$.product_id') as product_id,
        json_value(pro, '$.sku') as order_sku,
        CONCAT(cast(json_value(pro, '$.product_id') as string) , '_',cast(json_value(pro, '$.sku') as string)) as sku
    from 
    base,
    unnest(products) as pro
    