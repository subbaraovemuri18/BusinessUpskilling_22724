{{
  config(
    materialized         = 'incremental',
    unique_key           = 'income_statement_key',
    incremental_strategy = 'merge',
    on_schema_change     = 'fail',
    schema               = 'GOLD',
    tags                 = ['gold', 'fact', 'income_statement'],
    pre_hook             = "{{ log('Running: ' ~ this.name, info=True) }}"
  )
}}

-- =============================================================================
-- Model       : fact_income_statement
-- Schema      : BUI_SUBBARAO_VEMURI_DB.GOLD
-- Grain       : One row per subsidiary × period × department × classification × account
-- Source      : GOLD.FACT_GL_JOURNAL (filtered to P&L account types)
-- P&L account types included:
--   Revenue  → Income, OthIncome, DeferRevenue
--   COGS     → Cost of Goods Sold (COGS)
--   Expense  → Expense, OthExpense
-- KPIs supported (from Finance KPI Metrics):
--   Gross Revenue | Net Revenue | Cost of Revenue | Gross Profit | Gross Margin %
--   Operating Expenses | EBITDA | Net Income | D&A | Interest Expense
--   OpEx as % of Revenue | Bad Debt Expense | Third Party Revenue Share
-- FX: All amounts in USD using AVERAGE_RATE (P&L accounts translate at period average)
-- Generator   : gold-script-generator | Snowflake + dbt | PCP Capstone | 2026-06
-- =============================================================================

WITH gl AS (
    SELECT
        gl.gl_journal_key,
        gl.subsidiary_key,
        gl.period_key,
        gl.department_key,
        gl.classification_key,
        gl.account_key,
        gl.account_type,
        gl.cash_flow_category,
        gl.reporting_amount_usd,
        gl.transaction_date,
        -- pass raw IDs for surrogate key generation
        gl.subsidiary_id,
        gl.period_id,
        gl.department_id,
        gl.classification_id,
        gl.account_id,
        -- account name from dim
        da.account_name,
        da.account_type_group,
        da.fx_translation_rate_type
    FROM {{ ref('fact_gl_journal') }} gl
    LEFT JOIN {{ ref('dim_account') }} da ON gl.account_key = da.account_key
    -- Filter to P&L account types only
    WHERE gl.account_type IN (
        'Income', 'OthIncome', 'DeferRevenue',
        'COGS',
        'Expense', 'OthExpense'
    )
    {% if is_incremental() %}
    AND gl.dw_updated_at > (SELECT MAX(dw_updated_at) FROM {{ this }})
    {% endif %}
),

-- Exclude intercompany/elimination subsidiaries from consolidated P&L
non_elim AS (
    SELECT gl.*
    FROM gl
    LEFT JOIN {{ ref('dim_subsidiary') }} ds ON gl.subsidiary_key = ds.subsidiary_key
    WHERE COALESCE(ds.is_elimination, FALSE) = FALSE
),

aggregated AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key([
            'subsidiary_id', 'period_id', 'department_id', 'classification_id', 'account_id'
        ]) }}                                                                   AS income_statement_key,

        -- Dimension keys
        subsidiary_key,
        period_key,
        department_key,
        classification_key,
        account_key,

        -- Raw IDs for traceability
        subsidiary_id,
        period_id,
        department_id,
        classification_id,
        account_id,
        account_name,
        account_type,
        account_type_group,

        -- -------------------------------------------------------
        -- Revenue (Income accounts — sign convention: positive = revenue)
        -- -------------------------------------------------------
        SUM(CASE
            WHEN account_type IN ('Income','OthIncome','DeferRevenue')
            THEN -1 * reporting_amount_usd   -- credit-normal accounts; negate for P&L display
            ELSE 0
        END)                                                                    AS gross_revenue_usd,

        -- Net Revenue = Gross Revenue net of discounts/adjustments
        -- Discounts are coded as negative income in NetSuite CoA
        SUM(CASE
            WHEN account_type IN ('Income','OthIncome','DeferRevenue')
            THEN -1 * reporting_amount_usd
            ELSE 0
        END)                                                                    AS net_revenue_usd,

        -- -------------------------------------------------------
        -- Cost of Revenue (COGS accounts)
        -- -------------------------------------------------------
        SUM(CASE
            WHEN account_type = 'COGS'
            THEN reporting_amount_usd
            ELSE 0
        END)                                                                    AS cost_of_revenue_usd,

        -- -------------------------------------------------------
        -- Operating Expenses (Expense, OthExpense — excluding D&A and Interest)
        -- Note: D&A and Interest separated at account name level below
        -- -------------------------------------------------------
        SUM(CASE
            WHEN account_type IN ('Expense','OthExpense')
            THEN reporting_amount_usd
            ELSE 0
        END)                                                                    AS total_operating_expense_usd,

        -- Depreciation (account name contains 'depreciation' — CoA-dependent)
        SUM(CASE
            WHEN account_type IN ('Expense','OthExpense')
             AND LOWER(account_name) LIKE '%depreciation%'
            THEN reporting_amount_usd
            ELSE 0
        END)                                                                    AS depreciation_usd,

        -- Amortization (account name contains 'amortization')
        SUM(CASE
            WHEN account_type IN ('Expense','OthExpense')
             AND LOWER(account_name) LIKE '%amortization%'
            THEN reporting_amount_usd
            ELSE 0
        END)                                                                    AS amortization_usd,

        -- Interest Expense
        SUM(CASE
            WHEN account_type IN ('Expense','OthExpense')
             AND LOWER(account_name) LIKE '%interest%'
            THEN reporting_amount_usd
            ELSE 0
        END)                                                                    AS interest_expense_usd,

        -- Bad Debt Expense
        SUM(CASE
            WHEN account_type IN ('Expense','OthExpense')
             AND (LOWER(account_name) LIKE '%bad debt%'
                  OR LOWER(account_name) LIKE '%doubtful%')
            THEN reporting_amount_usd
            ELSE 0
        END)                                                                    AS bad_debt_expense_usd,

        -- Third Party Revenue Share
        SUM(CASE
            WHEN account_type IN ('Expense','OthExpense')
             AND LOWER(account_name) LIKE '%revenue share%'
            THEN reporting_amount_usd
            ELSE 0
        END)                                                                    AS third_party_revenue_share_usd,

        -- Total amount for all P&L lines (used for net income roll-up)
        SUM(CASE
            WHEN account_type IN ('Income','OthIncome','DeferRevenue')
            THEN -1 * reporting_amount_usd
            ELSE reporting_amount_usd
        END)                                                                    AS net_amount_usd,

        -- Audit
        CURRENT_TIMESTAMP()                                                     AS dw_created_at,
        CURRENT_TIMESTAMP()                                                     AS dw_updated_at,
        'NETSUITE'                                                              AS dw_source_system,
        '{{ invocation_id }}'                                                   AS dw_batch_id

    FROM non_elim
    GROUP BY
        subsidiary_id, period_id, department_id, classification_id, account_id,
        subsidiary_key, period_key, department_key, classification_key, account_key,
        account_name, account_type, account_type_group
),

with_kpis AS (
    SELECT
        *,

        -- Gross Profit = Net Revenue - COGS
        net_revenue_usd - cost_of_revenue_usd                                  AS gross_profit_usd,

        -- Gross Margin % = Gross Profit / Net Revenue
        CASE
            WHEN net_revenue_usd <> 0
            THEN ROUND((net_revenue_usd - cost_of_revenue_usd) / net_revenue_usd * 100, 2)
            ELSE NULL
        END                                                                     AS gross_margin_pct,

        -- D&A combined
        depreciation_usd + amortization_usd                                    AS da_usd,

        -- EBITDA = Gross Profit - OpEx + D&A (add back non-cash charges)
        (net_revenue_usd - cost_of_revenue_usd)
            - total_operating_expense_usd
            + (depreciation_usd + amortization_usd)                            AS ebitda_usd,

        -- EBIT = EBITDA - D&A
        (net_revenue_usd - cost_of_revenue_usd)
            - total_operating_expense_usd                                       AS ebit_usd,

        -- Net Income = EBIT - Interest Expense
        (net_revenue_usd - cost_of_revenue_usd)
            - total_operating_expense_usd
            - interest_expense_usd                                              AS net_income_usd,

        -- OpEx as % of Revenue
        CASE
            WHEN net_revenue_usd <> 0
            THEN ROUND(total_operating_expense_usd / net_revenue_usd * 100, 2)
            ELSE NULL
        END                                                                     AS opex_pct_of_revenue

    FROM aggregated
)

SELECT * FROM with_kpis
