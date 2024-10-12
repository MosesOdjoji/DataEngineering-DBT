with final as (
    select * from {{ ref('int_dim_customer') }}
)
select
    customer_id,
    first_name,
    last_name,
    first_order_date,
    most_recent_order_date,
    number_of_orders
from final