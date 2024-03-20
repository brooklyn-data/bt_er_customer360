{{ config(
    materialized="table",
    database="customer360",
    schema="intermediate"

) }}

with
    source_sf_opp as (
        select
            r_num
            , salesforce_account_id
            --, salesforce_opportunity_id
        from {{ ref('int_customers_unioned') }}
        where source = 'salesforce opportunities'
        and coalesce(salesforce_account_id,'') != ''
    ),
    customers_unioned as (
        select
            source
            , r_num
            , dup_imp_proj_r_num
            , dup_product_usg_r_num
            , salesforce_account_id
            --, salesforce_opportunity_id
            --, project_id
            , billing_id
            --, billing_platform_account_id
            , csr_user_name
            , csr_user_id
            --, tenant_id
        from {{ ref('int_outerjoin_product_usage') }} where 
        source != 'salesforce opportunities'
    )


select
    a.source
    , a.r_num
    , dup_imp_proj_r_num
    , dup_product_usg_r_num
    , b.r_num as dup_sf_opp_r_num
    , coalesce(a.salesforce_account_id, b.salesforce_account_id) as salesforce_account_id
   -- , coalesce(a.salesforce_opportunity_id, b.salesforce_opportunity_id ) as  salesforce_opportunity_id
    --, a.project_id
    , a.billing_id
    --, a.billing_platform_account_id
    , a.csr_user_name
    , a.csr_user_id
    --, a.tenant_id
from customers_unioned as a
full outer join
    source_sf_opp as b on
    a.salesforce_account_id = b.salesforce_account_id
