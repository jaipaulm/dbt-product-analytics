with events as (

    select * from {{ ref('stg_product_telemetry') }}

)

, accounts as (

    select * from {{ ref('stg_crm_accounts') }}

)

, events_accounts_joined as (

    select event_id
        , event_name
        , event_occurred_at
        , e.clari_org_id
        , user_type
        , platform
        , os
        , browser
        , user_id
        , account_type
    from events e
        inner join accounts a on e.clari_org_id = a.clari_org_id

)

, events_modules_fielded as (

        select event_id

        , case when event_name like '%dashboard' then 'Dashboard'
            when event_name like '%forecasting%' then 'Forecasting'
            when event_name like '%flow' then 'Flow'
            when event_name like '%opportunity_grid' then 'Opportunity Grid'
            when event_name like '%trend' then 'Trend'
            when event_name like '%pulse' then 'Pulse'
        else 'module name missing'
        end as module_name
    
        , case when event_name like '%dashboard' then 'Loaded'
                when event_name like '%flow' then 'Loaded'
                when event_name like '%opportunity_grid' then 'Loaded'
                when event_name like '%trend' then 'Loaded'
                when event_name like '%pulse' then 'Loaded'
                when event_name like '%forecasting%' and event_name like '%loaded%' then 'Loaded'
                when event_name like '%forecasting%' and event_name like '%updated' then 'Field Updated'
                when event_name like '%forecasting%' and event_name like '%export' then 'Data Exported'
            else 'module name missing'
        end as event_action

        , event_name
        , event_occurred_at
        , clari_org_id
        , user_type
        , platform
        , os
        , browser
        , user_id
        , account_type

    from events_accounts_joined
)


, events_dates_dims_added as (

    select event_id
        , module_name
        , event_action
        , event_name

        , event_occurred_at
        , {{ dbt_date.day_name("event_occurred_at") }} as day_of_week_short_name
        , {{ dbt_date.month_name("event_occurred_at") }} as month_short_name

        , date_trunc('HOUR',event_occurred_at) as event_date_trunc_hour
        , date_trunc('Day',event_occurred_at) as event_date_trunc_day
        , date_trunc('Month',event_occurred_at) as event_date_trunc_month


        , clari_org_id
        , user_type
        , platform
        , os
        , browser
        , user_id
        , account_type

    from events_modules_fielded

)

select * from events_dates_dims_added
