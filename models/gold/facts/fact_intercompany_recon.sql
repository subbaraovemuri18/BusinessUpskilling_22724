-- =============================================================================
-- Model       : fact_intercompany_recon (view)
-- Schema      : BUI_SUBBARAO_VEMURI_DB.GOLD
-- Grain       : One row per intercompany transaction pair (subsidiary A × subsidiary B × period)
-- Purpose     : Confirm elimination entries net to zero at group level.
--               Unreconciled balances here are a due diligence red flag.
-- Logic       : Surfaces transactions posted on the elimination subsidiary
--               and pairs them against the originating subsidiary entries.
-- Generator   : gold-script-generator | Snowflake + dbt | PCP Capstone | 2026-06
-- =============================================================================
{{
  config(
    materialized = 'view',
    schema       = 'GOLD',
    tags         = ['gold', 'audit', 'intercompany']
  )
}}

WITH elim_subs AS (
    -- Elimination subsidiaries only
    SELECT subsidiary_key, subsidiary_id, subsidiary_name
    FROM {{ ref('dim_subsidiary') }}
    WHERE is_elimination = TRUE
),

elim_entries AS (
    -- GL lines posted against elimination subsidiaries
    SELECT
        gl.period_key,
        gl.subsidiary_key,
        gl.account_key,
        gl.reporting_amount_usd,
        gl.transaction_ref AS tranid,
        gl.transaction_id,
        ds.subsidiary_name AS elimination_subsidiary_name,
        da.account_name,
        da.account_type
    FROM {{ ref('fact_gl_journal') }}     gl
    INNER JOIN elim_subs                  es ON gl.subsidiary_key = es.subsidiary_key
    LEFT JOIN {{ ref('dim_subsidiary') }} ds ON gl.subsidiary_key = ds.subsidiary_key
    LEFT JOIN {{ ref('dim_account') }}    da ON gl.account_key    = da.account_key
),

elimination_summary AS (
    SELECT
        period_key,
        account_key,
        account_name,
        account_type,
        elimination_subsidiary_name,
        SUM(reporting_amount_usd)                               AS elimination_amount_usd,
        COUNT(DISTINCT transaction_id)                          AS transaction_count
    FROM elim_entries
    GROUP BY
        period_key, account_key, account_name, account_type, elimination_subsidiary_name
)

SELECT
    dp.period_name,
    dp.fiscal_year,
    dp.fiscal_quarter,
    es.account_name,
    es.account_type,
    es.elimination_subsidiary_name,
    es.elimination_amount_usd,
    es.transaction_count,

    -- A non-zero elimination amount means intercompany entries did NOT net to zero
    -- This must be investigated before publishing consolidated financial statements
    CASE
        WHEN ABS(es.elimination_amount_usd) < 0.01 THEN 'RECONCILED'
        ELSE 'UNRECONCILED — INVESTIGATE'
    END                                                         AS reconciliation_status,

    es.period_key,
    es.account_key

FROM elimination_summary es
LEFT JOIN {{ ref('dim_period') }} dp ON es.period_key = dp.period_key
ORDER BY
    dp.fiscal_year DESC,
    dp.fiscal_quarter DESC,
    ABS(es.elimination_amount_usd) DESC
