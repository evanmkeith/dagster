select 
    c.id as customer_id,
    c.name as customer_name,
    o.id as order_id
from {{ source('raw_data', 'customers') }} c
left join {{ source('raw_data', 'orders') }} o 
    on c.id = o.customer_id