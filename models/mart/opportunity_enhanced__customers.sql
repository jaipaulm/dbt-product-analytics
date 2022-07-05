with opportunities as (

    select * from {{ ref('opportunity_enhanced') }}

)

, customer_accounts as (

    select * from {{ ref('customer_accounts') }}

)

select op.*
from opportunities op
    inner join customer_accounts ca on op.clari_org_id = ca.clari_org_id