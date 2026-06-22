{{
  config(
    materialized         = 'incremental',
    unique_key           = 'account_key',
    incremental_strategy = 'merge',
    on_schema_change     = 'fail',
    schema               = 'GOLD',
    tags                 = ['gold', 'dimension', 'account'],
    pre_hook             = "{{ log('Running: ' ~ this.name, info=True) }}"
  )
}}

-- =============================================================================
-- Model       : dim_account
-- Schema      : BUI_SUBBARAO_VEMURI_DB.GOLD
-- Grain       : One row per active GL account (SCD1 — overwrite on change)
-- Source      : SILVER.STG_NS_ACCOUNT
-- Acct types  : Income | Expense | Cost of Goods Sold | Bank | Accounts Receivable
--               Other Current Asset | Fixed Asset | Other Asset | Accounts Payable
--               Other Current Liability | Long Term Liability | Equity | Deferred Revenue
-- FX note     : GENERAL_RATE drives translation method in FACT_GL_JOURNAL
--               (Average = P&L accounts, Current = BS asset/liability, Historical = equity)
-- Surrogate key: dbt_utils.generate_surrogate_key(['id'])
-- Fix (2026-06): CASHFLOWRATE is NULL in source NetSuite — Cash Flow categories
--               are derived from ACCOUNT_TYPE instead:
--               Operating  → Income, COGS, Expense, Bank, AcctRec, OthCurrAsset, AcctPay, OthCurrLiab
--               Investing  → FixedAsset, OthAsset
--               Financing  → LongTermLiab, Equity, RetainEarnings
-- Generator   : gold-script-generator | Snowflake + dbt | PCP Capstone | 2026-06
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ ref('stg_ns_account') }}
    {% if is_incremental() %}
    WHERE SILVER_UPDATED_ON_TS_UTC > (SELECT MAX(dw_updated_at) FROM {{ this }})
    {% endif %}
),

renamed AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['ID']) }}          AS account_key,

        -- Natural key
        ID                                                       AS account_id,

        -- Account identity
        ACCOUNT_NUMBER                                           AS account_number,
        ACCOUNT_SEARCH_DISPLAY_NAME                             AS account_name,
        FULL_NAME                                               AS account_full_name,
        DISPLAY_NAME_WITH_HIERARCHY                             AS account_display_name_hierarchy,
        DESCRIPTION                                             AS account_description,

        -- Account classification — drives financial statement routing
        ACCOUNT_TYPE                                            AS account_type,
        CASE
            WHEN ACCOUNT_TYPE IN ('Income','OthIncome','DeferRevenue')
                THEN 'Revenue'
            WHEN ACCOUNT_TYPE IN ('COGS')
                THEN 'COGS'
            WHEN ACCOUNT_TYPE IN ('Expense','OthExpense','DeferExpense')
                THEN 'Expense'
            WHEN ACCOUNT_TYPE IN ('Bank','AcctRec','OthCurrAsset','FixedAsset','OthAsset',
                                  'UnbilledRec','CredCard')
                THEN 'Asset'
            WHEN ACCOUNT_TYPE IN ('AcctPay','OthCurrLiab','LongTermLiab','DeferRevenue')
                THEN 'Liability'
            WHEN ACCOUNT_TYPE IN ('Equity','RetainEarnings')
                THEN 'Equity'
            WHEN ACCOUNT_TYPE IN ('NonPosting','Stat')
                THEN 'Non-Posting'
            ELSE 'Other'
        END                                                             AS account_type_group,

        -- P&L vs Balance Sheet routing flag
        CASE
            WHEN ACCOUNT_TYPE IN (
                'Income','OthIncome','COGS','Expense','OthExpense','DeferRevenue'
            ) THEN 'Income Statement'
            WHEN ACCOUNT_TYPE IN (
                'Bank','AcctRec','OthCurrAsset','FixedAsset','OthAsset',
                'AcctPay','OthCurrLiab','LongTermLiab','Equity','RetainEarnings'
            ) THEN 'Balance Sheet'
            ELSE 'Other'
        END                                                     AS financial_statement,

        -- FX translation rate type (drives multi-currency consolidation)
        -- Average = P&L | Current = BS asset/liability | Historical = equity
        GENERAL_RATE                                            AS fx_translation_rate_type,

        -- =================================================================
        -- Cash flow classification — DERIVED from ACCOUNT_TYPE
        -- Source CASHFLOWRATE is NULL in this NetSuite instance (not configured)
        -- Mapping follows indirect method cash flow classification:
        --   Operating  → P&L accounts + working capital BS accounts
        --   Investing  → Long-term asset accounts (CapEx)
        --   Financing  → Long-term liabilities + equity accounts
        -- =================================================================
        CASE
            WHEN ACCOUNT_TYPE IN (
                'Income', 'OthIncome',
                'COGS',
                'Expense', 'OthExpense',
                'Bank', 'AcctRec', 'OthCurrAsset',
                'AcctPay', 'OthCurrLiab',
                'UnbilledRec', 'CredCard'
            ) THEN 'Operating'
            WHEN ACCOUNT_TYPE IN (
                'FixedAsset', 'OthAsset'
            ) THEN 'Investing'
            WHEN ACCOUNT_TYPE IN (
                'LongTermLiab',
                'Equity', 'RetainEarnings'
            ) THEN 'Financing'
            ELSE NULL  -- NonPosting, Stat, DeferRevenue excluded from Cash Flow
        END                                                     AS cash_flow_rate,

        -- Account hierarchy
        PARENT                                                  AS parent_account_id,
        IS_SUMMARY                                              AS is_summary_account,

        -- Flags
        NOT IS_INACTIVE                                         AS is_active,
        ELIMINATE                                               AS is_elimination,
        INVENTORY                                               AS is_inventory,

        -- Subsidiary scope
        SUBSIDIARY                                              AS subsidiary_scope,

        -- Audit columns
        CURRENT_TIMESTAMP()                                     AS dw_created_at,
        CURRENT_TIMESTAMP()                                     AS dw_updated_at,
        'NETSUITE'                                              AS dw_source_system,
        '{{ invocation_id }}'                                   AS dw_batch_id

    FROM source
    WHERE IS_INACTIVE = FALSE
)

SELECT * FROM renamed