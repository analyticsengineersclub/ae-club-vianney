with customers as (
    select * 
    from `analytics-engineers-club.coffee_shop.customers`
),

orders as (
    select *
    from `analytics-engineers-club.coffee_shop.orders`
),

reaname_customers as (
    select 
        id as customer_id,
        name,
        email
   
    from customers 
),

rename_orders as (
    select  
        customer_id,
        created_at as first_order_at,
        total as number_of_orders

    from orders
),

base as (
select *
from reaname_customers
inner join rename_orders using (customer_id)
)

select * from rename_orders
--changed line 35 to select from a different base 
