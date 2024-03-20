{{ config(
    materialized="table",
    database="customer360",
    schema="intermediate"

) }}

with
    source_product_usage as (
        select
            r_num
            , salesforce_account_id
            , csr_user_name
            , billing_id
        from {{ ref('int_customers_unioned') }}
        where source = 'product usage'
        and coalesce(salesforce_account_id,'') != ''
        and coalesce(billing_id,'') != ''
    ),
    customers_unioned as (
        select
            source
            , r_num
            , dup_imp_proj_r_num
            , salesforce_account_id
            --, salesforce_opportunity_id
            --, project_id
            , billing_id
            --, billing_platform_account_id
            , csr_user_name
            , csr_user_id
            --, tenant_id
        from {{ ref('int_outerjoin_implementation_project') }} where
        source != 'product usage'
    )


select
    a.source
    , a.r_num
    , dup_imp_proj_r_num
    , b.r_num as dup_product_usg_r_num
    , coalesce(a.salesforce_account_id, b.salesforce_account_id) as salesforce_account_id
    --, a.salesforce_opportunity_id
    --, a.project_id
    , coalesce(a.billing_id, b.billing_id) as billing_id
    --, a.billing_platform_account_id
    , coalesce(a.csr_user_name, b.csr_user_name ) as  csr_user_name
    , a.csr_user_id
    --, a.tenant_id
from customers_unioned as a
full outer join
    source_product_usage as b on
    a.salesforce_account_id = b.salesforce_account_id
    and a.billing_id = b.billing_id
