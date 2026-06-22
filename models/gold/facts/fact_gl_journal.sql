{{
  config(
    materialized         = 'incremental',
    unique_key           = 'gl_journal_key',
    incremental_strategy = 'merge',
    on_schema_change     = 'fail',
    schema               = 'GOLD',
    tags                 = ['gold', 'fact', 'gl_journal'],
    pre_hook             = "{{ log('Running: ' ~ this.name, info=True) }}"
  )
}}

-- =============================================================================
-- Model       : fact_gl_journal
-- Schema      : BUI_SUBBARAO_VEMURI_DB.GOLD
-- Grain       : One row per posted accounting line
--               (TRANSACTION × TRANSACTIONLINE × ACCOUNTINGBOOK)
-- Sources     : SILVER.STG_NS_TRANSACTION_ACCOUNTING_LINE (driver)
--               SILVER.STG_NS_TRANSACTION                 (header)
--               SILVER.STG_NS_TRANSACTION_LINE            (line context)
-- Critical filters:
--   1. POSTING = TRUE   — only lines that hit the general ledger
--   2. VOID   = FALSE   — exclude voided transaction headers
-- FX translation:
--   P&L accounts  → AVERAGE_RATE
--   BS accounts   → CURRENT_RATE
--   Equity        → HISTORICAL_RATE
-- Note: SUBSIDIARY sourced from TRANSACTION_LINE (not on TRANSACTION header)
-- Fix (2026-06): cash_flow_category now sourced from GOLD.dim_account.cash_flow_rate
--               instead of SILVER.stg_ns_account.CASHFLOWRATE which is NULL in source.
--               dim_account derives cash_flow_category from ACCOUNT_TYPE mapping.
-- Column names verified against Silver scripts 2026-06-16
-- =============================================================================

WITH tal AS (
    SELECT * FROM {{ ref('stg_ns_transaction_accounting_line') }}
    WHERE POSTING = TRUE
    {% if is_incremental() %}
      AND LAST_MODIFIED_DATE > (SELECT MAX(dw_updated_at) FROM {{ this }})
    {% endif %}
),

txn AS (
    SELECT
        ID,
        TR_AN_ID,
        "TYPE",
        ABBREVTYPE,
        TR_AN_DATE,
        POSTING_PERIOD,
        ENTITY,
        CURRENCY,
        EXCHANGE_RATE,
        MEMO,
        VOID,
        POSTING,
        STATUS,
        JOURNAL_TYPE,
        CREATED_DATE,
        LAST_MODIFIED_DATE
    FROM {{ ref('stg_ns_transaction') }}
    WHERE VOID    = FALSE
      AND POSTING = TRUE
),

tl AS (
    SELECT
        TRANSACTION,
        ID                  AS line_id,
        DEPARTMENT,
        CLASS,
        LOCATION,
        ENTITY              AS line_entity,
        SUBSIDIARY          AS line_subsidiary,
        FOREIGN_AMOUNT,
        NET_AMOUNT          AS line_amount,
        MEMO                AS line_memo,
        MAIN_LINE,
        ISCOGS,
        EXPENSE_ACCOUNT,
        TAX_LINE
    FROM {{ ref('stg_ns_transaction_line') }}
    WHERE MAIN_LINE = FALSE
      AND TAX_LINE  = FALSE
),

acct AS (
    SELECT
        ID,
        ACCOUNT_TYPE,
        GENERAL_RATE,
        IS_SUMMARY          AS is_summary_account,
        FULL_NAME           AS account_full_name
    FROM {{ ref('stg_ns_account') }}
),

fx AS (
    SELECT
        POSTING_PERIOD,
        FROM_CURRENCY,
        FROM_SUBSIDIARY,
        TO_CURRENCY,
        TO_SUBSIDIARY,
        AVERAGE_RATE,
        CURRENT_RATE,
        HISTORICAL_RATE
    FROM {{ ref('stg_ns_consolidated_exchange_rate') }}
    WHERE IS_ELIMINATION_SUBSIDIARY = FALSE
      AND IS_PERIOD_CLOSED          = TRUE
),

-- Pull cash_flow_rate from dim_account (derived from ACCOUNT_TYPE mapping)
-- This replaces the NULL CASHFLOWRATE from Silver stg_ns_account
dim_acct   AS (SELECT account_id, account_key, cash_flow_rate  FROM {{ ref('dim_account') }}),
dim_sub    AS (SELECT subsidiary_id,      subsidiary_key       FROM {{ ref('dim_subsidiary') }}),
dim_period AS (SELECT period_id,          period_key           FROM {{ ref('dim_period') }}),
dim_dept   AS (SELECT department_id,      department_key       FROM {{ ref('dim_department') }}),
dim_class  AS (SELECT classification_id,  classification_key   FROM {{ ref('dim_classification') }}),
dim_curr   AS (SELECT currency_id,        currency_key         FROM {{ ref('dim_currency') }}),

joined AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['tal.SURROGATE_KEY']) }}   AS gl_journal_key,

        -- Source drill-through reference
        txn.TR_AN_ID                                                    AS transaction_ref,
        tal.TRANSACTION                                                 AS transaction_id,
        tal.TRANSACTIONLINE                                             AS transaction_line_id,
        tal.ACCOUNTINGBOOK                                              AS accounting_book_id,

        -- Transaction metadata
        txn."TYPE"                                                      AS transaction_type,
        txn.ABBREVTYPE                                                  AS transaction_abbrev_type,
        txn.TR_AN_DATE                                                  AS transaction_date,
        txn.MEMO                                                        AS transaction_memo,
        tl.line_memo,

        -- Dimension foreign keys
        dim_acct.account_key                                            AS account_key,
        dim_sub.subsidiary_key                                          AS subsidiary_key,
        dim_period.period_key                                           AS period_key,
        dim_dept.department_key                                         AS department_key,
        dim_class.classification_key                                    AS classification_key,
        dim_curr.currency_key                                           AS currency_key,

        -- Raw IDs
        tal.ACCOUNT                                                     AS account_id,
        tl.line_subsidiary                                              AS subsidiary_id,
        txn.POSTING_PERIOD                                              AS period_id,
        tl.DEPARTMENT                                                   AS department_id,
        tl.CLASS                                                        AS classification_id,
        txn.CURRENCY                                                    AS currency_id,

        -- Account classification
        acct.ACCOUNT_TYPE                                               AS account_type,
        acct.GENERAL_RATE                                               AS fx_translation_rate_type,

        -- Cash flow category — sourced from dim_account (derived from ACCOUNT_TYPE)
        -- NOT from Silver CASHFLOWRATE which is NULL in this NetSuite instance
        dim_acct.cash_flow_rate                                         AS cash_flow_category,

        -- Amounts in functional currency
        tal.AMOUNT                                                      AS amount_functional,
        tal.DEBIT                                                       AS debit_functional,
        tal.CREDIT                                                      AS credit_functional,
        tal.NET_AMOUNT                                                  AS net_amount_functional,

        -- FX rate from source
        tal.EXCHANGE_RATE                                               AS source_exchange_rate,

        -- USD reporting amount — rate driven by GENERAL_RATE on account
        CASE
            WHEN acct.GENERAL_RATE = 'AVERAGE'
                THEN tal.AMOUNT * COALESCE(fx.AVERAGE_RATE,    tal.EXCHANGE_RATE, 1)
            WHEN acct.GENERAL_RATE = 'CURRENT'
                THEN tal.AMOUNT * COALESCE(fx.CURRENT_RATE,    tal.EXCHANGE_RATE, 1)
            WHEN acct.GENERAL_RATE = 'HISTORICAL'
                THEN tal.AMOUNT * COALESCE(fx.HISTORICAL_RATE, tal.EXCHANGE_RATE, 1)
            ELSE tal.AMOUNT * COALESCE(tal.EXCHANGE_RATE, 1)
        END                                                             AS reporting_amount_usd,

        CASE
            WHEN acct.GENERAL_RATE = 'AVERAGE'
                THEN COALESCE(fx.AVERAGE_RATE,    tal.EXCHANGE_RATE, 1)
            WHEN acct.GENERAL_RATE = 'CURRENT'
                THEN COALESCE(fx.CURRENT_RATE,    tal.EXCHANGE_RATE, 1)
            WHEN acct.GENERAL_RATE = 'HISTORICAL'
                THEN COALESCE(fx.HISTORICAL_RATE, tal.EXCHANGE_RATE, 1)
            ELSE COALESCE(tal.EXCHANGE_RATE, 1)
        END                                                             AS applied_exchange_rate,

        -- AR/AP aging support
        tal.AMOUNT_LINKED                                               AS amount_linked,
        tal.AMOUNT_PAID                                                 AS amount_paid,
        tal.AMOUNTUNPAID                                                AS amount_unpaid,

        -- Flags
        tal.POSTING                                                     AS is_posting,
        tl.ISCOGS                                                       AS is_cogs_line,
        txn.JOURNAL_TYPE                                                AS journal_type,

        -- Audit
        CURRENT_TIMESTAMP()                                             AS dw_created_at,
        CURRENT_TIMESTAMP()                                             AS dw_updated_at,
        'NETSUITE'                                                      AS dw_source_system,
        '{{ invocation_id }}'                                           AS dw_batch_id

    FROM tal
    INNER JOIN txn
        ON tal.TRANSACTION = txn.ID
    LEFT JOIN tl
        ON tal.TRANSACTION    = tl.TRANSACTION
       AND tal.TRANSACTIONLINE = tl.line_id
    LEFT JOIN acct
        ON tal.ACCOUNT = acct.ID
    LEFT JOIN fx
        ON txn.POSTING_PERIOD  = fx.POSTING_PERIOD
       AND txn.CURRENCY        = fx.FROM_CURRENCY
       AND tl.line_subsidiary  = fx.FROM_SUBSIDIARY
    LEFT JOIN dim_acct
        ON tal.ACCOUNT         = dim_acct.account_id
    LEFT JOIN dim_sub
        ON tl.line_subsidiary  = dim_sub.subsidiary_id
    LEFT JOIN dim_period
        ON txn.POSTING_PERIOD  = dim_period.period_id
    LEFT JOIN dim_dept
        ON tl.DEPARTMENT       = dim_dept.department_id
    LEFT JOIN dim_class
        ON tl.CLASS            = dim_class.classification_id
    LEFT JOIN dim_curr
        ON txn.CURRENCY        = dim_curr.currency_id
)

SELECT * FROM joined