{{
  config(
    alias = 'dim_users'
    )
}}

with

-- each source model is defined at the top of the file so it's clear for other users which models are being used.
-- Alternatively, you could check the upstream tables using the lineage feature within dbt-power-user but that
-- assumes they have it installed.
stg_monzo_datawarehouse__account_created as (
    select *
    from {{ ref('stg_monzo_datawarehouse__account_created') }}
)

-- I'm using fct_accounts_snapshot_day here to avoid repeating logic relating to transaction summaries.
-- This model should in theory never be joined directly to fct_accounts_snapshot_day as a fct table should not be denormalised
-- in my methodology. This means we won't get any circular dependencies in the future.
, fct_accounts_snapshot_day as (
    select *
    from {{ ref('fct_accounts_snapshot_day') }}
)

-- cte to bring in the last record for each account so we have the most up to date information in regards to activity.
, most_recent_account_record as (
    select *
    from fct_accounts_snapshot_day    
    qualify row_number() over (partition by user_id, account_id order by date_day desc) = 1
)

-- find the first account created for each user.
, base as (
    select
        user_id
        , min(created_at) as first_account_created_at
    from stg_monzo_datawarehouse__account_created
    group by 1
)

-- We have one record per account ever opened in the most_recent_account_record cte.
-- Now we can use this data to provide information about the total opened and active accounts as a user level.
, accounts_aggregated as (
    select
        user_id
        , count(distinct account_id) as total_accounts_opened
        , count(distinct case when is_account_open then account_id end) as total_open_accounts
        , count(distinct case when is_account_active_last_7_days then account_id end) as total_active_accounts_last_7_days
    from most_recent_account_record
    group by 1
)

, final as (
    select
        base.user_id
        , base.first_account_created_at
        , accounts_aggregated.total_accounts_opened
        , accounts_aggregated.total_open_accounts
        , accounts_aggregated.total_active_accounts_last_7_days
    from base
    left join accounts_aggregated
        on base.user_id = accounts_aggregated.user_id
)

select *
from final