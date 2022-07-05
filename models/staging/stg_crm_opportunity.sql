with opportunity_base as (

    select id
        , try_to_numeric(id) as casted_id
        , is_deleted
        , account_id
        , stage_name
        , amount
        , type
        , lead_source
        , sales_rep
        , csm_assigned

    from clari_raw.public.crm_opportunity

)

, opportunity_filtered as (


    select casted_id as opportunity_id
        , is_deleted
        , account_id as clari_org_id
        , stage_name
        , amount
        , type as opportunity_type
        , lead_source
        , sales_rep as sales_rep_id
        , csm_assigned as csm_id
    
    from opportunity_base

    where is_deleted = false
        and casted_id is not null
        and account_id is not null

)

select * from opportunity_filtered