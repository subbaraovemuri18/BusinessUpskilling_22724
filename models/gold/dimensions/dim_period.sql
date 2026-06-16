{{
  config(
    materialized         = 'incremental',
    unique_key           = 'period_key',
    incremental_strategy = 'merge',
    on_schema_change     = 'fail',
    schema               = 'GOLD',
    tags                 = ['gold', 'dimension', 'period'],
    pre_hook             = "{{ log('Running: ' ~ this.name, info=True) }}"
  )
}}

-- =============================================================================
-- Model       : dim_period
-- Schema      : BUI_SUBBARAO_VEMURI_DB.GOLD
-- Grain       : One row per posting accounting period (SCD1)
-- Source      : SILVER.STG_NS_ACCOUNTING_PERIOD
-- Key filters : ISPOSTING = TRUE (only periods that accept journal entries)
--               ISADJUST periods handled but flagged — require separate P&L treatment
-- Derived     : FISCAL_YEAR, FISCAL_QUARTER, FISCAL_MONTH extracted from PERIOD_NAME
-- Surrogate key: dbt_utils.generate_surrogate_key(['id'])
-- Generator   : gold-script-generator | Snowflake + dbt | PCP Capstone | 2026-06
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ ref('stg_ns_accounting_period') }}
    {% if is_incremental() %}
    WHERE SILVER_UPDATED_ON_TS_UTC > (SELECT MAX(dw_updated_at) FROM {{ this }})
    {% endif %}
),

renamed AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['ID']) }}           AS period_key,

        -- Natural key
        ID                                                        AS period_id,

        -- Period identity
        PERIOD_NAME                                              AS period_name,
        START_DATE                                               AS period_start_date,
        END_DATE                                                 AS period_end_date,

        -- Fiscal calendar derivations
        YEAR(START_DATE)                                         AS fiscal_year,
        QUARTER(START_DATE)                                      AS fiscal_quarter,
        MONTH(START_DATE)                                        AS fiscal_month,
        CONCAT('Q', QUARTER(START_DATE), ' ', YEAR(START_DATE)) AS fiscal_quarter_label,

        -- Period type flags
        IS_YEAR                                                  AS is_year_period,
        IS_QUARTER                                               AS is_quarter_period,
        ISADJUST                                                 AS is_adjustment_period,
        IS_POSTING                                                AS is_posting_period,

        -- Close status — used to confirm month-end lock before extracting statements
        CLOSED                                                   AS is_closed,
        ALLLOCKED                                                AS is_all_locked,
        APLOCKED                                                 AS is_ap_locked,
        ARLOCKED                                                 AS is_ar_locked,
        CLOSED_ON_DATE                                           AS closed_on_date,

        -- Audit columns
        CURRENT_TIMESTAMP()                                      AS dw_created_at,
        CURRENT_TIMESTAMP()                                      AS dw_updated_at,
        'NETSUITE'                                               AS dw_source_system,
        '{{ invocation_id }}'                                    AS dw_batch_id

    FROM source
    WHERE IS_POSTING = TRUE
      AND IS_INACTIVE = FALSE
)

SELECT * FROM renamed
