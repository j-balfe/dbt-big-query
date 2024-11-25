{{
  config(
    alias = 'fct_accounts_snapshot_day'
  )
}}

with

date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2017-08-10' as date)",
        end_date="current_date"
    ) }}
)

, stg_monzo_datawarehouse__account_created as (
    select *
    from {{ ref('stg_monzo_datawarehouse__account_created') }}
)

, stg_monzo_datawarehouse__account_closed as (
    select *
    from {{ ref('stg_monzo_datawarehouse__account_closed') }}
)

, stg_monzo_datawarehouse__account_reopened as (
    select *
    from {{ ref('stg_monzo_datawarehouse__account_reopened') }}
)

, stg_monzo_datawarehouse__account_transactions as (
    select *
    from {{ ref('stg_monzo_datawarehouse__account_transactions') }}
)

, base as (
    select
        date_spine.date_day
        , stg_monzo_datawarehouse__account_created.account_id
        , stg_monzo_datawarehouse__account_created.created_at
    from date_spine
    left join stg_monzo_datawarehouse__account_created
        on date_spine.date_day between safe_cast(stg_monzo_datawarehouse__account_created.created_at as date) and current_date
)

, base_enriched_with_last_actions as (
    select
        base.date_day
        , base.account_id
        , base.created_at as account_created_at
        , max(stg_monzo_datawarehouse__account_closed.closed_at) as account_last_closed_at
        , max(stg_monzo_datawarehouse__account_reopened.reopened_at) as account_last_reopened_at
        , sum(stg_monzo_datawarehouse__account_transactions.number_of_transactions) as number_of_transactions
    from base
    left join stg_monzo_datawarehouse__account_closed
        on
            base.date_day >= safe_cast(stg_monzo_datawarehouse__account_closed.closed_at as date)
            and base.account_id = stg_monzo_datawarehouse__account_closed.account_id
    left join stg_monzo_datawarehouse__account_reopened
        on
            base.date_day >= safe_cast(stg_monzo_datawarehouse__account_reopened.reopened_at as date)
            and base.account_id = stg_monzo_datawarehouse__account_reopened.account_id
    left join stg_monzo_datawarehouse__account_transactions
        on
            base.date_day = stg_monzo_datawarehouse__account_transactions.transaction_date
            and base.account_id = stg_monzo_datawarehouse__account_transactions.account_id
    group by 1, 2, 3
)

, add_window_logic as (
    select
        base_enriched_with_last_actions.date_day
        , base_enriched_with_last_actions.account_id
        , base_enriched_with_last_actions.account_created_at
        , base_enriched_with_last_actions.account_last_closed_at
        , base_enriched_with_last_actions.account_last_reopened_at
        , base_enriched_with_last_actions.number_of_transactions
        , sum(base_enriched_with_last_actions.number_of_transactions)
            over (
                partition by base_enriched_with_last_actions.account_id
                order by base_enriched_with_last_actions.date_day
                rows between unbounded preceding and current row
        ) as number_of_account_transactions_to_date
        , sum(base_enriched_with_last_actions.number_of_transactions)
            over (
                partition by base_enriched_with_last_actions.account_id
                order by base_enriched_with_last_actions.date_day
                rows between 7 preceding and current row
        ) as number_of_account_transactions_last_7_days
    from base_enriched_with_last_actions
)

, final as (
    select
        add_window_logic.date_day
        , add_window_logic.account_id
        , stg_monzo_datawarehouse__account_created.user_id
        , add_window_logic.account_created_at
        , add_window_logic.account_last_closed_at
        , add_window_logic.account_last_reopened_at
        , add_window_logic.number_of_transactions
        , add_window_logic.number_of_account_transactions_to_date
        , add_window_logic.number_of_account_transactions_last_7_days
        , case
            when add_window_logic.account_last_closed_at is null then true
            when add_window_logic.account_last_reopened_at > add_window_logic.account_last_closed_at then true
            else false
        end as is_account_open
        , if(add_window_logic.number_of_account_transactions_last_7_days is not null, true, false) as is_account_active_last_7_days
    from add_window_logic
    left join stg_monzo_datawarehouse__account_created
        on add_window_logic.account_id = stg_monzo_datawarehouse__account_created.account_id
)

select *
from final