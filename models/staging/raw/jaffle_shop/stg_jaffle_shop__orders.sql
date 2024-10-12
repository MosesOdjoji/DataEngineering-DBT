with final as (
select
    id as order_id,
    customer as customer_id,
    ordered_at,
    store_id,
    subtotal,
    tax_paid,
    order_total
from raw.jaffle_shop.orders
)

select  
    order_id,
    customer_id,
    ordered_at,
    store_id,
    subtotal,
    tax_paid,
    order_total
from final