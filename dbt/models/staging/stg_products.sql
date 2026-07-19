{{config(materialized='view')}}

select 
    id as product_id,
    title,
    price,
    description,
    category,
    image,
    rating_rate,
    rating_count
from {{source('api_ods', 'products')}}
where rating_count >= 100
order by product_id 