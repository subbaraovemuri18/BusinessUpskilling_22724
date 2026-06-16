{{
  config(
    materialized         = 'incremental',
    unique_key           = 'classification_key',
    incremental_strategy = 'merge',
    on_schema_change     = 'fail',
    schema               = 'GOLD',
    tags                 = ['gold', 'dimension', 'classification'],
    pre_hook             = "{{ log('Running: ' ~ this.name, info=True) }}"
   
  )
}}

-- =============================================================================
-- Model       : dim_classification
-- Schema      : BUI_SUBBARAO_VEMURI_DB.GOLD
-- Grain       : One row per business line / class segment (SCD1)
-- Source      : SILVER.STG_NS_CLASSIFICATION
-- Key fields  : Used to slice P&L by revenue stream (Services vs Products vs SaaS)
--               PARENT supports two-level hierarchy within each subsidiary
-- Surrogate key: dbt_utils.generate_surrogate_key(['id'])
-- Generator   : gold-script-generator | Snowflake + dbt | PCP Capstone | 2026-06
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ ref('stg_ns_classification') }}
    {% if is_incremental() %}
    WHERE SILVER_UPDATED_ON_TS_UTC > (SELECT MAX(dw_updated_at) FROM {{ this }})
    {% endif %}
),

renamed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['ID']) }}          AS classification_key,
        ID                                                       AS classification_id,
        NAME                                                     AS classification_name,
        FULL_NAME                                               AS classification_full_name,
        PARENT                                                  AS parent_classification_id,
        SUBSIDIARY                                              AS subsidiary_scope,
        NOT IS_INACTIVE                                         AS is_active,
        CURRENT_TIMESTAMP()                                     AS dw_created_at,
        CURRENT_TIMESTAMP()                                     AS dw_updated_at,
        'NETSUITE'                                              AS dw_source_system,
        '{{ invocation_id }}'                                   AS dw_batch_id
    FROM source
    WHERE IS_INACTIVE = FALSE
)

SELECT * FROM renamed
