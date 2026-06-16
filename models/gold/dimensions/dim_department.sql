{{
  config(
    materialized         = 'incremental',
    unique_key           = 'department_key',
    incremental_strategy = 'merge',
    on_schema_change     = 'fail',
    schema               = 'GOLD',
    tags                 = ['gold', 'dimension', 'department'],
    pre_hook             = "{{ log('Running: ' ~ this.name, info=True) }}"
  )
}}

-- =============================================================================
-- Model       : dim_department
-- Schema      : BUI_SUBBARAO_VEMURI_DB.GOLD
-- Grain       : One row per department / cost centre (SCD1)
-- Source      : SILVER.STG_NS_DEPARTMENT
-- Key fields  : PARENT enables multi-level hierarchy (e.g. Sales > NA Sales > Enterprise)
--               Primary segmentation dimension for departmental P&L slice
-- Surrogate key: dbt_utils.generate_surrogate_key(['id'])
-- Generator   : gold-script-generator | Snowflake + dbt | PCP Capstone | 2026-06
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ ref('stg_ns_department') }}
    {% if is_incremental() %}
    WHERE SILVER_UPDATED_ON_TS_UTC > (SELECT MAX(dw_updated_at) FROM {{ this }})
    {% endif %}
),

renamed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['ID']) }}          AS department_key,
        ID                                                       AS department_id,
        NAME                                                     AS department_name,
        FULL_NAME                                               AS department_full_name,
        PARENT                                                  AS parent_department_id,
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
