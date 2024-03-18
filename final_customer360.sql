with  aggregating_rest_of_records as (select * from {{ ref('int_outerjoin_intacct_revenue') }}
union all
 select 
    source
    , source_id
    , null as dup_imp_proj_source_id
    , null as dup_product_usg_source_id
    , null as dup_sf_opp_source_id
    , null as dup_intacct_revenue_source_id
    , salesforce_account_id
    , salesforce_opportunity_id
    , project_id
    , billing_id
    , billing_platform_account_id
    , csr_user_name
    , csr_user_id
    , tenant_id
 
  from {{ ref('int_customers_unioned') }} where
source_id not in (select distinct dup_imp_proj_source_id from {{ ref('int_outerjoin_intacct_revenue') }})
and source_id not in (select distinct dup_product_usg_source_id from {{ ref('int_outerjoin_intacct_revenue') }})
and source_id not in (select distinct dup_sf_opp_source_id from {{ ref('int_outerjoin_intacct_revenue') }})
and source_id not in (select distinct dup_intacct_revenue_source_id from {{ ref('int_outerjoin_intacct_revenue') }}))
select 
    salesforce_account_id
    , project_id
    , billing_id
    , billing_platform_account_id
    , csr_user_name
    , csr_user_id
    , tenant_id
from aggregating_rest_of_records 
group by
    salesforce_account_id
    , project_id
    , billing_id
    , billing_platform_account_id
    , csr_user_name
    , csr_user_id
    , tenant_id
