# The output have been generated with the assistance of Claude at 2026-06-12T10:00:00Z UTC. The content has been verified by the designated engineer.
# Schema tests for Batch 1 Silver staging models.
# Test coverage: not_null + unique on SURROGATE_KEY per confirmed Phase 1D matrix.
# Note: No config: block in this file — all model config lives in the .sql config() block.

version: 2

models:

  - name: stg_ns_account
    description: >
      Silver staging model for the NetSuite Chart of Accounts (ACCOUNT).
      One row per GL account. ACCOUNT_TYPE drives financial statement routing.
      CASHFLOWRATE drives Cash Flow Statement category.
      GENERAL_RATE controls FX translation method for consolidation.
    columns:
      - name: SURROGATE_KEY
        description: "Surrogate primary key — MD5 hash of ID || 'ACCOUNT'."
        data_tests:
          - not_null
          - unique
      - name: SILVER_CREATED_ON_TS_UTC
        description: "Audit — row first inserted timestamp (UTC)."
        data_tests:
          - not_null
      - name: SILVER_UPDATED_ON_TS_UTC
        description: "Audit — row last updated timestamp (UTC)."
        data_tests:
          - not_null

  - name: stg_ns_accounting_period
    description: >
      Silver staging model for the NetSuite fiscal calendar (ACCOUNTINGPERIOD).
      One row per posting period (month/quarter/year).
      IS_POSTING, CLOSED, and ISADJUST are critical filter flags for financial statement generation.
    columns:
      - name: SURROGATE_KEY
        description: "Surrogate primary key — MD5 hash of ID || 'ACCOUNTINGPERIOD'."
        data_tests:
          - not_null
          - unique
      - name: SILVER_CREATED_ON_TS_UTC
        description: "Audit — row first inserted timestamp (UTC)."
        data_tests:
          - not_null
      - name: SILVER_UPDATED_ON_TS_UTC
        description: "Audit — row last updated timestamp (UTC)."
        data_tests:
          - not_null

  - name: stg_ns_classification
    description: >
      Silver staging model for the NetSuite Classification segment (CLASSIFICATION).
      One row per business line / product group.
      Used to slice P&L by revenue stream within and across subsidiaries.
    columns:
      - name: SURROGATE_KEY
        description: "Surrogate primary key — MD5 hash of ID || 'CLASSIFICATION'."
        data_tests:
          - not_null
          - unique
      - name: SILVER_CREATED_ON_TS_UTC
        description: "Audit — row first inserted timestamp (UTC)."
        data_tests:
          - not_null
      - name: SILVER_UPDATED_ON_TS_UTC
        description: "Audit — row last updated timestamp (UTC)."
        data_tests:
          - not_null

  - name: stg_ns_consolidated_exchange_rate
    description: >
      Silver staging model for period-level FX rates (CONSOLIDATEDEXCHANGERATE).
      One row per period + currency pair combination.
      AVERAGE_RATE applies to P&L, CURRENT_RATE to Balance Sheet, HISTORICAL_RATE to Equity.
      Critical for translating CAD (ProSport Direct) and GBP (OutdoorEdge Co.) to USD.
    columns:
      - name: SURROGATE_KEY
        description: "Surrogate primary key — MD5 hash of ID || 'CONSOLIDATEDEXCHANGERATE'."
        data_tests:
          - not_null
          - unique
      - name: SILVER_CREATED_ON_TS_UTC
        description: "Audit — row first inserted timestamp (UTC)."
        data_tests:
          - not_null
      - name: SILVER_UPDATED_ON_TS_UTC
        description: "Audit — row last updated timestamp (UTC)."
        data_tests:
          - not_null

  - name: stg_ns_currency
    description: >
      Silver staging model for the NetSuite currency master (CURRENCY).
      One row per currency. IS_BASE_CURRENCY identifies USD as group reporting currency.
      SYMBOL used for labelling amounts in all financial statement views.
    columns:
      - name: SURROGATE_KEY
        description: "Surrogate primary key — MD5 hash of ID || 'CURRENCY'."
        data_tests:
          - not_null
          - unique
      - name: SILVER_CREATED_ON_TS_UTC
        description: "Audit — row first inserted timestamp (UTC)."
        data_tests:
          - not_null
      - name: SILVER_UPDATED_ON_TS_UTC
        description: "Audit — row last updated timestamp (UTC)."
        data_tests:
          - not_null
