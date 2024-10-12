with final as (
select
    id as customer_id,
    name
from raw.jaffle_shop.customers
)

select  
    customer_id,
    name
from final