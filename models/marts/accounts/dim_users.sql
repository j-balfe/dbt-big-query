{{
  config(
    alias = 'dim_users'
    )
}}

with

stg_monzo_datawarehouse__account_created as (
    select *
    from {{ ref('stg_monzo_datawarehouse__account_created') }}
)

, fct_accounts_snapshot_day as (
    select *
    from {{ ref('fct_accounts_snapshot_day') }}
)

, most_recent_accounts as (
    select *
    from fct_accounts_snapshot_day
    qualify row_number() over (partition by user_id, account_id order by date_day desc) = 1
)

, base as (
    select
        user_id
        , min(created_at) as first_account_opened_at
    from stg_monzo_datawarehouse__account_created
    group by 1
)

, accounts_aggregated as (
    select
        user_id
        , count(distinct account_id) as total_accounts_opened
        , count(distinct case when is_account_open then account_id end) as total_open_accounts
        , count(distinct case when is_account_active_last_7_days then account_id end) as total_active_accounts_last_7_days
    from most_recent_accounts
    group by 1
)

, final as (
    select
        base.user_id
        , base.first_account_opened_at
        , accounts_aggregated.total_accounts_opened
        , accounts_aggregated.total_open_accounts
        , accounts_aggregated.total_active_accounts_last_7_days
    from base
    left join accounts_aggregated
        on base.user_id = accounts_aggregated.user_id
)

select *
from final