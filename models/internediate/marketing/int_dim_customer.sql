with customers as (

     select * from {{ ref('stg_jaffle_shop__customers') }}

),

orders as ( 

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

customer_orders as (

    select
        customer_id,
        min(ordered_at) as first_order_date,
        max(ordered_at) as most_recent_order_date,
        count(ordered_at) as number_of_orders

    from orders

    group by 1

),

final as (

    select
        customers.customer_id,
        substring(customers.name, 1, charindex(' ', customers.name)-1 ) as first_name,
        substring(customers.name, charindex(' ', customers.name)+1, len(name) ) as last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce (customer_orders.number_of_orders, 0) 
        as number_of_orders

    from customers
    left join customer_orders using (customer_id)

)

select * from final