{{
  config(
    alias = 'fct_accounts_snapshot_day'
  )
}}

with

-- I've assumed that the date of the first account opening is immutable and therefore won't change.
-- If I were to build this in production I would define a variable which returns the date of the first account ever opened with Monzo.
-- For the purposes of the assessment I've picked a date before Monzo existed.
-- THis doesn't cause issues downstream as I fan out accounts between their creation date and the current date.
date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2010-01-01' as date)",
        end_date="current_date"
    ) }}
)

-- each source model is defined at the top of the file so it's clear for other users which models are being used.
-- Alternatively, you could check the upstream tables using the lineage feature within dbt-power-user but that
-- assumes they have it installed.
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

-- Creates a record per day for each account from the date of creation to the current date.
, base as (
    select
        date_spine.date_day
        , stg_monzo_datawarehouse__account_created.account_id
        , stg_monzo_datawarehouse__account_created.created_at
    from date_spine
    left join stg_monzo_datawarehouse__account_created
        on date_spine.date_day between safe_cast(stg_monzo_datawarehouse__account_created.created_at as date) and current_date
)

-- For simplicity I've kept the base cte separate from the below to make the logic easier to follow.
-- Here we join the closed, reopened and transaction events before aggregating to get the last_x for each day and account.
-- This means at any time we can calculate the status of any account at that specific point in time.
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

-- In order to calculate if an account is active within the last 7 days we need to know the number of transaction that have occurred.
-- As I have a record for every day for each account I can use the following window functions to sum the transactions.
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

-- Finally, as I can't reference the window function output in the same cte I've created a 'final' cte to add some booleans 
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
        -- Mirrors the is_account_open we see in dim_accounts
        -- where as here it's accurate for any day historically.
        -- If an account has never been closed it's open.
        -- If an account has been closed and since reopened it's open
        -- If an account does not fall into either of the above categories it must be closed.
        , case
            when add_window_logic.account_last_closed_at is null then true
            when add_window_logic.account_last_reopened_at > add_window_logic.account_last_closed_at then true
            else false
        end as is_account_open
        -- Boolean which returns true if there are transactions within the last 7 days.
        , if(add_window_logic.number_of_account_transactions_last_7_days is not null, true, false) as is_account_active_last_7_days
    from add_window_logic
    left join stg_monzo_datawarehouse__account_created
        on add_window_logic.account_id = stg_monzo_datawarehouse__account_created.account_id
)

select *
from final