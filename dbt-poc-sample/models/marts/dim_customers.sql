{{
    config (
        schema = 'mart',
        materialized = 'table',
        unique_key = 'customer_id',
    )
}}
with incremental_data as (
    select
        customer_id,
        customer_name,
        customer_address,
        customer_phone,
        customer_account_balance,
        customer_market_segment,
        customer_description
    from
    {{ref('stg_customer')}} 

)
select * from incremental_data
