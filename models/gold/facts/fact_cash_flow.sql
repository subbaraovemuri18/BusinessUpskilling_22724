{{
  config(
    materialized         = 'incremental',
    unique_key           = 'cash_flow_key',
    incremental_strategy = 'merge',
    on_schema_change     = 'fail',
    schema               = 'GOLD',
    tags                 = ['gold', 'fact', 'cash_flow'],
    pre_hook             = "{{ log('Running: ' ~ this.name, info=True) }}"
  )
}}

-- =============================================================================
-- Model       : fact_cash_flow
-- Schema      : BUI_SUBBARAO_VEMURI_DB.GOLD
-- Grain       : One row per subsidiary × period × cash_flow_category × account
-- Source      : GOLD.FACT_GL_JOURNAL + DIM_ACCOUNT.CASHFLOWRATE classification
-- Cash flow categories (from ACCOUNT.CASHFLOWRATE):
--   Operating  — day-to-day operating activities (revenue collections, expense payments)
--   Investing  — acquisition/disposal of long-term assets (CapEx)
--   Financing  — debt, equity raises, repayments
-- Accounts without a CASHFLOWRATE are excluded from this table.
-- FX: P&L-derived movements → AVERAGE_RATE | BS movements → CURRENT_RATE
-- KPIs supported: CapEx | Net Cash Flow | Free Cash Flow | Cash Balance
--   Unlevered Free Cash Flow | Operating/Investing/Financing subtotals
-- Reconciliation: Closing cash here must tie to Bank account balance in FACT_BALANCE_SHEET
-- Generator   : gold-script-generator | Snowflake + dbt | PCP Capstone | 2026-06
-- =============================================================================

WITH gl AS (
    SELECT
        gl.subsidiary_key,
        gl.period_key,
        gl.account_key,
        gl.account_type,
        gl.cash_flow_category,
        gl.subsidiary_id,
        gl.period_id,
        gl.account_id,
        gl.reporting_amount_usd,
        da.account_name,
        da.account_type_group,
        dp.period_end_date,
        dp.fiscal_year,
        dp.fiscal_quarter,
        dp.fiscal_month,
        dp.is_closed
    FROM {{ ref('fact_gl_journal') }}   gl
    LEFT JOIN {{ ref('dim_account') }}  da ON gl.account_key = da.account_key
    LEFT JOIN {{ ref('dim_period') }}   dp ON gl.period_key  = dp.period_key
    -- Only accounts with a cash flow classification
    WHERE gl.cash_flow_category IS NOT NULL
      AND gl.cash_flow_category <> ''
    {% if is_incremental() %}
    AND gl.dw_updated_at > (SELECT MAX(dw_updated_at) FROM {{ this }})
    {% endif %}
),

non_elim AS (
    SELECT gl.*
    FROM gl
    LEFT JOIN {{ ref('dim_subsidiary') }} ds ON gl.subsidiary_key = ds.subsidiary_key
    WHERE COALESCE(ds.is_elimination, FALSE) = FALSE
),

aggregated AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'subsidiary_id', 'period_id', 'cash_flow_category', 'account_id'
        ]) }}                                                                   AS cash_flow_key,

        subsidiary_key,
        period_key,
        account_key,
        subsidiary_id,
        period_id,
        account_id,
        account_name,
        account_type,
        account_type_group,
        cash_flow_category,
        period_end_date,
        fiscal_year,
        fiscal_quarter,
        fiscal_month,
        is_closed,

        SUM(reporting_amount_usd)                                               AS cash_movement_usd,

        -- CapEx identified by Investing classification + FixedAsset/OthAsset account types
        SUM(CASE
            WHEN cash_flow_category = 'Investing'
             AND account_type IN ('FixedAsset','OthAsset')
            THEN reporting_amount_usd ELSE 0
        END)                                                                    AS capex_usd,

        -- Interest expense within financing (for UFCF calculation)
        SUM(CASE
            WHEN cash_flow_category = 'Financing'
             AND LOWER(account_name) LIKE '%interest%'
            THEN reporting_amount_usd ELSE 0
        END)                                                                    AS interest_paid_usd,

        -- Audit
        CURRENT_TIMESTAMP()                                                     AS dw_created_at,
        CURRENT_TIMESTAMP()                                                     AS dw_updated_at,
        'NETSUITE'                                                              AS dw_source_system,
        '{{ invocation_id }}'                                                   AS dw_batch_id

    FROM non_elim
    GROUP BY
        subsidiary_id, period_id, account_id, cash_flow_category,
        subsidiary_key, period_key, account_key,
        account_name, account_type, account_type_group,
        period_end_date, fiscal_year, fiscal_quarter, fiscal_month, is_closed
)

SELECT * FROM aggregated
