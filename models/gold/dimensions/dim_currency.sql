{{
  config(
    materialized         = 'incremental',
    unique_key           = 'currency_key',
    incremental_strategy = 'merge',
    on_schema_change     = 'fail',
    schema               = 'GOLD',
    tags                 = ['gold', 'dimension', 'currency'],
    pre_hook             = "{{ log('Running: ' ~ this.name, info=True) }}"
  )
}}

-- =============================================================================
-- Model       : dim_currency
-- Schema      : BUI_SUBBARAO_VEMURI_DB.GOLD
-- Grain       : One row per currency (SCD1)
-- Source      : SILVER.STG_NS_CURRENCY
-- Key fields  : IS_BASE_CURRENCY identifies USD as group reporting currency
--               Used to label transactional amounts alongside USD-translated equivalents
-- Functional currencies in scope: USD (base), CAD (ProSport Direct), GBP (OutdoorEdge)
-- Surrogate key: dbt_utils.generate_surrogate_key(['id'])
-- Generator   : gold-script-generator | Snowflake + dbt | PCP Capstone | 2026-06
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ ref('stg_ns_currency') }}
    {% if is_incremental() %}
    WHERE SILVER_UPDATED_ON_TS_UTC > (SELECT MAX(dw_updated_at) FROM {{ this }})
    {% endif %}
),

renamed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['ID']) }}          AS currency_key,
        ID                                                       AS currency_id,
        NAME                                                     AS currency_name,
        SYMBOL                                                  AS currency_symbol,
        DISPLAY_SYMBOL                                          AS currency_display_symbol,
        IS_BASE_CURRENCY                                        AS is_base_currency,
        EXCHANGE_RATE                                           AS default_exchange_rate,
        NOT IS_INACTIVE                                         AS is_active,
        CURRENT_TIMESTAMP()                                     AS dw_created_at,
        CURRENT_TIMESTAMP()                                     AS dw_updated_at,
        'NETSUITE'                                              AS dw_source_system,
        '{{ invocation_id }}'                                   AS dw_batch_id
    FROM source
    WHERE IS_INACTIVE = FALSE
)

SELECT * FROM renamed
