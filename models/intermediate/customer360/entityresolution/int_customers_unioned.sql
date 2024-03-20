{{ config(
    materialized="table",
    database="customer360",
    schema="intermediate") }}

with unionedtables as (
    select
        'salesforce' as source,
        salesforce_account_nps_id as source_id,
        salesforce_account_id,
        --null as salesforce_opportunity_id,
        --null as project_id,
        salesforce_billing_id as billing_id,
        null as billing_platform_account_id,
        salesforce_csr_user_name as csr_user_name,
        salesforce_csr_user_id as csr_user_id,
        null as tenant_id
    from {{ ref('int_salesforce_accounts_detail') }}

        union all
    select
        'salesforce opportunities' as source,
        salesforce_account_opportunity_id as source_id,
        salesforce_account_id,
        --salesforce_opportunity_id,
        --null as project_id,
        null as billing_id,
        null as billing_platform_account_id,
        null as csr_user_name,
        null as csr_user_id,
        null as tenant_id
    from {{ ref('int_salesforce_opportunities') }}
    union all
    select
        'implementation projects' as source,
        implementation_project_id as source_id,
        salesforce_account_id,
        --salesforce_opportunity_id,
        --project_id,
        salesforce_account_billing_id as billing_id,
        null as billing_platform_account_id,
        null as csr_user_name,
        null as csr_user_id,
        null as tenant_id
    from {{ ref('int_implementation_projects') }}
    union all
    select
        'intacct revenue' as source,
        intacct_revenue_id as source_id,
        null as salesforce_account_id,
        --null as salesforce_opportunity_id,
        --null as project_id,
        null as billing_id,
        null as billing_platform_account_id,
        revenue_csr_user_name as csr_user_name,
        null as csr_user_id,
        null as tenant_id
    from {{ ref('int_intacct_revenue') }}
    union all
    select
        'product usage' as source,
        product_usage_id as source_id,
        fastlane_salesforce_account_id as salesforce_account_id,
        --null as salesforce_opportunity_id,
        --null as project_id,
        fastlane_billing_id as billing_id,
        null as billing_platform_account_id,
        fastlane_csr_user_name as csr_user_name,
        null as csr_user_id,
        null as tenant_id
    from 
{{ ref('int_product_usage') }}

)

, final as (select distinct
    source
    , salesforce_account_id
    , billing_id
    , csr_user_name
    , csr_user_id
from unionedtables)

select 
    *    
    , row_number() over (order by salesforce_account_id) as r_num 
from final
