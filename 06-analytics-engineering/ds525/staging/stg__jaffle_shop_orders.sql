select * from {{ source('jaffle_shop', 'jaffle_shop_orders')}}