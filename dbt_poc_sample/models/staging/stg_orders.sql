{{
    config (
        schema = 'staging',
        materialized = 'view'
    )
}}

with source as (

    select * from {{ source('sample_source', 'orders') }}

),

renamed as (

    select

        o_orderkey as order_id,
        o_custkey as customer_id,
        o_orderstatus as order_status,
        o_totalprice as total_price,
        o_orderdate as order_date,
        o_orderpriority as order_priority,
        o_clerk as clerk_id,
        o_shippriority as shipping_priority,
        o_comment as order_description,
        current_timestamp() as dbt_updated_at 

    from source

)

select * from renamed