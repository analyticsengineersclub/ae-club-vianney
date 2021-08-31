with customer_orders as (
    select
        customer_id,
        count(*) as number_of_orders,
        min(created_at) as first_order_at,
        sum(total) as total_order_value
    from `analytics-engineers-club.coffee_shop.orders`
    group by 1
)

select
    c.id AS customer_id,
    c.name,
    c.email,
    co.first_order_at,
    co.number_of_orders,
    co.total_order_value

from `analytics-engineers-club.coffee_shop.customers` as c
left join customer_orders as co
    on c.id = co.customer_id