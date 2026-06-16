-- =============================================================================
-- Model       : fact_trial_balance (view)
-- Schema      : BUI_SUBBARAO_VEMURI_DB.GOLD
-- Grain       : One row per account × subsidiary × period (opening + movement + closing)
-- Purpose     : Audit drill-down — every active account, every period, every subsidiary
--               Supports due diligence buyers drilling from statement line to TRANID
-- Generator   : gold-script-generator | Snowflake + dbt | PCP Capstone | 2026-06
-- =============================================================================
{{
  config(
    materialized = 'view',
    schema       = 'GOLD',
    tags         = ['gold', 'audit', 'trial_balance']
  )
}}

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
        gl.debit_functional,
        gl.credit_functional,
        gl.transaction_ref       AS tranid,
        gl.transaction_id,
        gl.transaction_line_id,
        gl.transaction_date,
        gl.transaction_type,
        gl.transaction_memo,
        da.account_number,
        da.account_name,
        da.account_type_group,
        ds.subsidiary_name,
        dp.period_name,
        dp.fiscal_year,
        dp.fiscal_quarter,
        dp.period_end_date
    FROM {{ ref('fact_gl_journal') }}   gl
    LEFT JOIN {{ ref('dim_account') }}  da ON gl.account_key    = da.account_key
    LEFT JOIN {{ ref('dim_subsidiary')}}ds ON gl.subsidiary_key = ds.subsidiary_key
    LEFT JOIN {{ ref('dim_period') }}   dp ON gl.period_key     = dp.period_key
),

-- Opening balance = cumulative sum up to (but not including) this period
opening AS (
    SELECT
        a.subsidiary_id,
        a.period_id,
        a.account_id,
        SUM(b.reporting_amount_usd) AS opening_balance_usd
    FROM gl a
    INNER JOIN gl b
        ON  a.subsidiary_id = b.subsidiary_id
        AND a.account_id    = b.account_id
        AND b.period_end_date < a.period_end_date
    GROUP BY a.subsidiary_id, a.period_id, a.account_id
),

period_movement AS (
    SELECT
        subsidiary_id,
        period_id,
        account_id,
        account_number,
        account_name,
        account_type,
        account_type_group,
        subsidiary_name,
        period_name,
        fiscal_year,
        fiscal_quarter,
        period_end_date,
        subsidiary_key,
        period_key,
        account_key,
        SUM(debit_functional)      AS total_debit,
        SUM(credit_functional)     AS total_credit,
        SUM(reporting_amount_usd)  AS period_movement_usd
    FROM gl
    GROUP BY
        subsidiary_id, period_id, account_id,
        account_number, account_name, account_type, account_type_group,
        subsidiary_name, period_name, fiscal_year, fiscal_quarter, period_end_date,
        subsidiary_key, period_key, account_key
)

SELECT
    pm.account_number,
    pm.account_name,
    pm.account_type,
    pm.account_type_group,
    pm.subsidiary_name,
    pm.period_name,
    pm.fiscal_year,
    pm.fiscal_quarter,
    pm.period_end_date,

    COALESCE(o.opening_balance_usd, 0)                              AS opening_balance_usd,
    pm.period_movement_usd,
    pm.total_debit,
    pm.total_credit,
    COALESCE(o.opening_balance_usd, 0) + pm.period_movement_usd    AS closing_balance_usd,

    -- Dimension keys for joins
    pm.subsidiary_key,
    pm.period_key,
    pm.account_key

FROM period_movement pm
LEFT JOIN opening o
    ON pm.subsidiary_id = o.subsidiary_id
   AND pm.period_id     = o.period_id
   AND pm.account_id    = o.account_id
