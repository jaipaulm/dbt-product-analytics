select ID as event_id
    , EVENT_NAME
    , TIME AS event_occurred_at
    , CLARI_ORG_ID
    , USER_TYPE
    , PLATFORM
    , OS	
    , USER_ID 
    , BROWSER
    , REGION

from clari_raw.public.product_telemetry -- would use src function for live project 