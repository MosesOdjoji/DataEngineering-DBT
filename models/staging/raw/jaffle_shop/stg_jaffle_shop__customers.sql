with final as (
select
    id as customer_id,
    first_name,
    last_name
from {{ source('jaffle_shop_customers', 'customers') }}
)

select  
    customer_id,
    first_name,
    last_name
from final