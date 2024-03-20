{{ config(
    materialized="table",
    database="customer360",
    schema="intermediate",

) }}

with
    source_intacct_rev as (
        select
            r_num
            , csr_user_name
        from {{ ref('int_customers_unioned') }}
        where source = 'intacct revenue'
        and coalesce(csr_user_name,'') != ''
    ),
    customers_unioned as (
        select
            source
            , r_num
            , dup_imp_proj_r_num
            , dup_product_usg_r_num
            , dup_sf_opp_r_num
            , salesforce_account_id
            --, salesforce_opportunity_id
            --, project_id
            , billing_id
            --, billing_platform_account_id
            , csr_user_name
            , csr_user_id
            --, tenant_id
        from {{ ref('int_outerjoin_sf_opp') }} where 
        source != 'intacct revenue'
    )


select
    a.source
    , a.r_num
    , dup_imp_proj_r_num
    , dup_product_usg_r_num
    , dup_sf_opp_r_num
    , b.r_num as dup_intacct_revenue_r_num
    , a.salesforce_account_id
   --, a.salesforce_opportunity_id
    --, a.project_id
    , a.billing_id
    --, a.billing_platform_account_id
   , coalesce(a.csr_user_name, b.csr_user_name ) as  csr_user_name
    , a.csr_user_id
    --, a.tenant_id
from customers_unioned as a
full outer join
    source_intacct_rev as b on
    a.csr_user_name = b.csr_user_name
