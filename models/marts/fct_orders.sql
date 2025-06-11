{{
    config (
        schema = 'mart',
        materialized = 'incremental',
        unique_key = 'order_id',
        on_schema_change = 'fail',
        incremental_strategy = 'merge'
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

    {% if is_incremental() %}
    where dbt_updated_at >= (select max(order_date) from {{ this }} )
    {% endif %}

)
select * from incremental_data
