with opps as (

    select * from {{ ref('stg_crm_opportunity') }}

)

, users as (

    select * from {{ ref('stg_crm_user') }}

)

select op.*
    , sl.name as sales_rep_name
    , sl.title as sales_rep_title
    , cs.name as csm_name
    , cs.title as csm_title
    , sl.territory as territory

from opps op
    left join users sl on op.sales_rep_id = sl.crm_user_id
    left join users cs on op.csm_id = cs.crm_user_id