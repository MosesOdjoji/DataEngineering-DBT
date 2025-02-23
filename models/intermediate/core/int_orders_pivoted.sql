{%- set payments = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] -%}

with stripe_payment as (
    select * from {{ ref('stg_stripe__payments') }}
)
select
    order_id,
    {%- for payment in payments -%}
        sum(case when payment_method = '{{payment}}' then amount else 0 end) as {{payment}}_method
        {%- if not loop.last -%}
            ,
        {% endif -%}
    {% endfor %}
from stripe_payment
group by order_id