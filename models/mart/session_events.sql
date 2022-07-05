with product_events as (

    select * from {{ ref('product_telemetry__enhanced') }}
)

-- gap in logic below: user events are grouped by hour truncated event timestamp. If user used two modules near the end of the hour - they'll appear as two different sessions in this model

select user_id
    , event_date_trunc_hour
    , min(event_id) as session_event_id
    , count(event_id) as modules_used_during_session
    , listagg( distinct MODULE_NAME, '-') within group (order by module_name) as session_summary
from product_events
where account_type = 'Customer'
group by 1, 2