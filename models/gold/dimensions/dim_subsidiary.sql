{{
  config(
    materialized         = 'incremental',
    unique_key           = 'subsidiary_key',
    incremental_strategy = 'merge',
    on_schema_change     = 'fail',
    schema               = 'GOLD',
    tags                 = ['gold', 'dimension', 'subsidiary'],
    pre_hook             = "{{ log('Running: ' ~ this.name, info=True) }}"
  )
}}

-- =============================================================================
-- Model       : dim_subsidiary
-- Schema      : BUI_SUBBARAO_VEMURI_DB.GOLD
-- Grain       : One row per legal entity / subsidiary (SCD1)
-- Source      : SILVER.STG_NS_SUBSIDIARY
-- Key fields  : IS_ELIMINATION flags intercompany netting subsidiary
--               PARENT_SUBSIDIARY_ID enables roll-up to PCP Holdings group level
-- Portfolio   : Apex Apparel (USD) | HomeBase Retail (USD) | ProSport Direct (CAD)
--               TechEdge Accessories (USD) | OutdoorEdge Co. (GBP) | PCP Holdings (USD)
-- Surrogate key: dbt_utils.generate_surrogate_key(['id'])
-- Generator   : gold-script-generator | Snowflake + dbt | PCP Capstone | 2026-06
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ ref('stg_ns_subsidiary') }}
    {% if is_incremental() %}
    WHERE SILVER_UPDATED_ON_TS_UTC > (SELECT MAX(dw_updated_at) FROM {{ this }})
    {% endif %}
),

renamed AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['ID']) }}          AS subsidiary_key,

        -- Natural key
        ID                                                       AS subsidiary_id,

        -- Identity
        NAME                                                     AS subsidiary_name,
        FULL_NAME                                               AS subsidiary_full_name,
        LEGAL_NAME                                              AS subsidiary_legal_name,

        -- Hierarchy — parent links subsidiary to PCP Holdings consolidation node
        PARENT                                                  AS parent_subsidiary_id,
        CASE WHEN PARENT IS NULL THEN TRUE ELSE FALSE END       AS is_top_level,

        -- Currency
        CURRENCY                                                AS functional_currency_id,
        COUNTRY                                                 AS country_code,

        -- Elimination flag — used to exclude intercompany entries from consolidated P&L
        IS_ELIMINATION                                           AS is_elimination,

        -- Fiscal calendar
        FISCAL_CALENDAR                                         AS fiscal_calendar_id,

        -- Flags
        NOT IS_INACTIVE                                         AS is_active,

        -- Audit columns
        CURRENT_TIMESTAMP()                                     AS dw_created_at,
        CURRENT_TIMESTAMP()                                     AS dw_updated_at,
        'NETSUITE'                                              AS dw_source_system,
        '{{ invocation_id }}'                                   AS dw_batch_id

    FROM source
    WHERE IS_INACTIVE = FALSE
)

SELECT * FROM renamed
