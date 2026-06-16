{{
  config(
    materialized         = 'incremental',
    unique_key           = 'balance_sheet_key',
    incremental_strategy = 'merge',
    on_schema_change     = 'fail',
    schema               = 'GOLD',
    tags                 = ['gold', 'fact', 'balance_sheet'],
    pre_hook             = "{{ log('Running: ' ~ this.name, info=True) }}"
  )
}}

-- =============================================================================
-- Model       : fact_balance_sheet
-- Schema      : BUI_SUBBARAO_VEMURI_DB.GOLD
-- Grain       : One row per subsidiary × period × account (closing balance as at period end)
-- Source      : GOLD.FACT_GL_JOURNAL (filtered to Balance Sheet account types)
-- BS account types:
--   Asset     → Bank, AcctRec, OthCurrAsset, FixedAsset, OthAsset
--   Liability → AcctPay, OthCurrLiab, LongTermLiab, DeferRevenue
--   Equity    → Equity, RetainEarnings
-- CRITICAL — Cumulative balance logic:
--   Closing balance = SUM of ALL movements from earliest period through period-end date.
--   This is NOT period-movement logic. The most common modelling error is applying
--   period-filter only — which produces a "balance sheet" showing only this month's activity.
-- FX translation (per IFRS / US GAAP):
--   Assets / Liabilities → CURRENT_RATE  (period-end spot rate)
--   Equity               → HISTORICAL_RATE
-- KPIs supported: Total Assets | Fixed Assets | Intangible Assets | PP&E
--   Total Liabilities | Equity | Working Capital | Deferred Revenue
--   Current Ratio | Debt-to-Equity | Cash Balance | AR | AP
--   AFDA | Debt Service Coverage
-- Generator   : gold-script-generator | Snowflake + dbt | PCP Capstone | 2026-06
-- =============================================================================

WITH gl AS (
    SELECT
        gl.subsidiary_key,
        gl.period_key,
        gl.account_key,
        gl.account_type,
        gl.subsidiary_id,
        gl.period_id,
        gl.account_id,
        gl.reporting_amount_usd,
        gl.amount_functional,
        gl.amount_paid,
        gl.amount_unpaid,
        da.account_name,
        da.account_type_group,
        da.fx_translation_rate_type,
        -- Period end date needed for cumulative logic
        dp.period_end_date,
        dp.fiscal_year,
        dp.fiscal_quarter,
        dp.fiscal_month,
        dp.is_closed,
        dp.is_adjustment_period
    FROM {{ ref('fact_gl_journal') }}        gl
    LEFT JOIN {{ ref('dim_account') }}       da ON gl.account_key   = da.account_key
    LEFT JOIN {{ ref('dim_period') }}        dp ON gl.period_key    = dp.period_key
    WHERE gl.account_type IN (
        'Bank', 'AcctRec', 'OthCurrAsset', 'FixedAsset', 'OthAsset',
        'AcctPay', 'OthCurrLiab', 'LongTermLiab', 'DeferRevenue',
        'Equity', 'RetainEarnings'
    )
    {% if is_incremental() %}
    AND gl.dw_updated_at > (SELECT MAX(dw_updated_at) FROM {{ this }})
    {% endif %}
),

-- Exclude elimination subsidiaries from consolidated view
non_elim AS (
    SELECT gl.*
    FROM gl
    LEFT JOIN {{ ref('dim_subsidiary') }} ds ON gl.subsidiary_key = ds.subsidiary_key
    WHERE COALESCE(ds.is_elimination, FALSE) = FALSE
),

-- All period end dates — needed to compute cumulative balances as at each period-end
all_periods AS (
    SELECT period_key, period_id, period_end_date, fiscal_year, fiscal_quarter, fiscal_month,
           is_closed, is_adjustment_period
    FROM {{ ref('dim_period') }}
    WHERE is_posting_period = TRUE
),

-- Cumulative closing balance for each account × subsidiary as at each period-end
-- SUM over all movements from the beginning of time through and including each period
cumulative AS (
    SELECT
        p.period_key                                                             AS period_key,
        p.period_id,
        p.period_end_date,
        p.fiscal_year,
        p.fiscal_quarter,
        p.fiscal_month,
        p.is_closed,
        p.is_adjustment_period,
        ne.subsidiary_key,
        ne.subsidiary_id,
        ne.account_key,
        ne.account_id,
        ne.account_name,
        ne.account_type,
        ne.account_type_group,
        ne.fx_translation_rate_type,

        -- Period movement (this period only)
        SUM(CASE WHEN ne.period_end_date = p.period_end_date
                 THEN ne.reporting_amount_usd ELSE 0 END)                       AS period_movement_usd,

        -- Cumulative closing balance (SUM from inception to this period-end)
        -- This is the correct balance sheet balance — not period-only movement
        SUM(ne.reporting_amount_usd)                                            AS closing_balance_usd,

        SUM(ne.amount_functional)                                               AS closing_balance_functional,
        SUM(ne.amount_paid)                                                     AS total_amount_paid,
        SUM(ne.amount_unpaid)                                                   AS total_amount_unpaid

    FROM all_periods p
    -- Cross-join to all account×subsidiary combinations that have movements up to this period
    INNER JOIN non_elim ne
        ON ne.period_end_date <= p.period_end_date
    GROUP BY
        p.period_key, p.period_id, p.period_end_date,
        p.fiscal_year, p.fiscal_quarter, p.fiscal_month,
        p.is_closed, p.is_adjustment_period,
        ne.subsidiary_key, ne.subsidiary_id,
        ne.account_key, ne.account_id,
        ne.account_name, ne.account_type, ne.account_type_group,
        ne.fx_translation_rate_type
),

with_kpis AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['subsidiary_id', 'period_id', 'account_id']) }}
                                                                                AS balance_sheet_key,

        -- Dimension keys
        subsidiary_key,
        period_key,
        account_key,

        -- Raw IDs
        subsidiary_id,
        period_id,
        account_id,
        account_name,
        account_type,
        account_type_group,
        fx_translation_rate_type,

        -- Period metadata
        period_end_date,
        fiscal_year,
        fiscal_quarter,
        fiscal_month,
        is_closed,
        is_adjustment_period,

        -- Balance amounts (USD)
        period_movement_usd,
        closing_balance_usd,
        closing_balance_functional,

        -- Sign-corrected closing balance for display (Assets positive, Liabilities/Equity negative credit-normal)
        CASE
            WHEN account_type IN ('Bank','AcctRec','OthCurrAsset','FixedAsset','OthAsset')
                THEN closing_balance_usd          -- Assets: debit-normal, positive is correct
            WHEN account_type IN ('AcctPay','OthCurrLiab','LongTermLiab','DeferRevenue',
                                  'Equity','RetainEarnings')
                THEN -1 * closing_balance_usd     -- Liabilities & Equity: credit-normal, negate for display
            ELSE closing_balance_usd
        END                                                                     AS closing_balance_display_usd,

        -- AR/AP sub-totals (for aging, DSO, DPO KPIs)
        CASE WHEN account_type = 'AcctRec'
             THEN closing_balance_usd ELSE 0 END                                AS accounts_receivable_usd,
        CASE WHEN account_type = 'AcctPay'
             THEN -1 * closing_balance_usd ELSE 0 END                           AS accounts_payable_usd,

        -- Cash & equivalents (Bank accounts)
        CASE WHEN account_type = 'Bank'
             THEN closing_balance_usd ELSE 0 END                                AS cash_and_equivalents_usd,

        -- Fixed assets
        CASE WHEN account_type = 'FixedAsset'
             THEN closing_balance_usd ELSE 0 END                                AS fixed_assets_usd,

        -- Deferred revenue (liability)
        CASE WHEN account_type = 'DeferRevenue'
             THEN -1 * closing_balance_usd ELSE 0 END                           AS deferred_revenue_usd,

        -- Current vs non-current classification
        CASE WHEN account_type IN ('Bank','AcctRec','OthCurrAsset','DeferRevenue')
             THEN TRUE ELSE FALSE END                                            AS is_current_account,

        -- Paid / unpaid tracking (AR aging support)
        total_amount_paid,
        total_amount_unpaid,

        -- Audit
        CURRENT_TIMESTAMP()                                                     AS dw_created_at,
        CURRENT_TIMESTAMP()                                                     AS dw_updated_at,
        'NETSUITE'                                                              AS dw_source_system,
        '{{ invocation_id }}'                                                   AS dw_batch_id

    FROM cumulative
)

SELECT * FROM with_kpis
