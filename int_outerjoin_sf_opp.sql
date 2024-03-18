with
    source_sf_opp as (
        select
            source_id
            , salesforce_account_id
            , salesforce_opportunity_id
        from {{ ref('int_customers_unioned') }}
        where source = 'salesforce opportunities'
        and coalesce(salesforce_account_id,'') != ''
    ),
    customers_unioned as (
        select
            source
            , source_id
            , dup_imp_proj_source_id
            , dup_product_usg_source_id
            , salesforce_account_id
            , salesforce_opportunity_id
            , project_id
            , billing_id
            , billing_platform_account_id
            , csr_user_name
            , csr_user_id
            , tenant_id
        from {{ ref('int_outerjoin_product_usage') }} where 
        source != 'salesforce opportunities'
    )


select
    a.source
    , a.source_id
    , dup_imp_proj_source_id
    , dup_product_usg_source_id
    , b.source_id as dup_sf_opp_source_id
    , a.salesforce_account_id
    , coalesce(a.salesforce_opportunity_id, b.salesforce_opportunity_id ) as  salesforce_opportunity_id
    , a.project_id
    , a.billing_id
    , a.billing_platform_account_id
    , a.csr_user_name
    , a.csr_user_id
    , a.tenant_id
from customers_unioned as a
full outer join
    source_sf_opp as b on
    a.salesforce_account_id = b.salesforce_account_id
