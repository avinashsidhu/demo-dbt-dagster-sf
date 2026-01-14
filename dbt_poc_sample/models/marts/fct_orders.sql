{{
    config (
        schema = 'mart',
        materialized = 'table',
        unique_key = 'order_id',
    )
}}
with incremental_data as (
    select
        order_id,
        customer_id,
        order_status,
        total_price,
        order_date,
        order_priority,
        clerk_id,
        shipping_priority,
        order_description
    from
    {{ref('stg_orders')}} 

)
select * from incremental_data
