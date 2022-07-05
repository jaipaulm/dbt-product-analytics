select id as clari_org_id
    , is_deleted
    , type as account_type
from clari_raw.public.crm_accounts
where is_deleted = false