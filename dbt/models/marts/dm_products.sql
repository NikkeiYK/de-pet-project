{{config(materialized='table', engine='MergeTree()', order_by='category')}}

select 
    category, 
    avg(price) as avg_price,
    avg(rating_rate) as avg_rating,
    avg(rating_count) as avg_count
from {{ref('stg_products')}}
group by category
