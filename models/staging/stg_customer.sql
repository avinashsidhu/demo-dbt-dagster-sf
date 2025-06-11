{{
    config (
        schema = 'staging',
        materialized = 'view'
    )
}}

with source as (

    select * from {{ source('sample_source', 'customer') }}

),

final as (

    select

        c_custkey as customer_id,
        c_name as customer_name,
        c_address as customer_address,
        c_phone as customer_phone,
        c_acctbal as customer_account_balance,
        c_mktsegment as customer_market_segment,
        c_comment as customer_description,
        current_timestamp() as dbt_updated_at 

    from source

)

select * from final