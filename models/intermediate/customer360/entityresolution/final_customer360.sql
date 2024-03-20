{{ config(
    materialized="table",
    database="customer360",
    schema="intermediate",

) }}

with list_of_r_num as (
select r_num from {{ ref('int_outerjoin_intacct_revenue') }}
union all
select dup_imp_proj_r_num as r_num from {{ ref('int_outerjoin_intacct_revenue') }}
union all
select dup_product_usg_r_num as r_num from {{ ref('int_outerjoin_intacct_revenue') }}
union all
select dup_sf_opp_r_num as r_num from {{ ref('int_outerjoin_intacct_revenue') }}
union all
select dup_intacct_revenue_r_num as r_num from {{ ref('int_outerjoin_intacct_revenue') }}
), 

aggregating_rest_of_records as (
 select 
    source
    , r_num
    , null as dup_imp_proj_r_num
    , null as dup_product_usg_r_num
    , null as dup_sf_opp_r_num
    , null as dup_intacct_revenue_r_num
    , salesforce_account_id
    --, salesforce_opportunity_id
    --, project_id
    , billing_id
    --, billing_platform_account_id
    , csr_user_name
    , csr_user_id
    --, tenant_id
 
  from {{ ref('int_customers_unioned') }} where
r_num not in (select distinct r_num from list_of_r_num)
)
select 
    salesforce_account_id
    --, project_id
    , billing_id
    --, billing_platform_account_id
    , csr_user_name
    , csr_user_id
    --, tenant_id
from {{ ref('int_outerjoin_intacct_revenue') }} 
group by
    salesforce_account_id
    --, project_id
    , billing_id
    --, billing_platform_account_id
    , csr_user_name
    , csr_user_id
    --, tenant_id

union all

select 
    salesforce_account_id
    --, project_id
    , billing_id
   -- , billing_platform_account_id
    , csr_user_name
    , csr_user_id
    --, tenant_id
from aggregating_rest_of_records 
group by
    salesforce_account_id
    --, project_id
    , billing_id
    --, billing_platform_account_id
    , csr_user_name
    , csr_user_id
    --, tenant_id
