{{
    config(
        materialized        = 'incremental',
        unique_key          = 'SURROGATE_KEY',
        incremental_strategy= 'merge',
        on_schema_change    = 'fail',
        tags                = ['silver', 'staging', 'netsuite', 'accounting_period']
    )
}}

{#
  Model  : stg_ns_accounting_period
  Layer  : Silver — Staging
  Grain  : One row per NetSuite fiscal period (ACCOUNTINGPERIOD.ID)
  Schema : BUI_SUBBARAO_VEMURI_DB.SILVER
  Source : {{ source('ns', 'ACCOUNTINGPERIOD') }}
  Cleaning: Inline (LLD TransformationRule applied directly)
  Note   : IS_POSTING, CLOSED, and ISADJUST are critical filter flags used in
           all downstream financial statement Gold models. Ensure these pass through correctly.
  Watermark: _FIVETRAN_SYNCED (Bronze) vs MAX(SILVER_UPDATED_ON_TS_UTC) on target.
#}

WITH source AS (

    SELECT *
    FROM {{ source('ns', 'ACCOUNTINGPERIOD') }}
    {% if is_incremental() %}
    WHERE _FIVETRAN_SYNCED > (SELECT MAX(SILVER_UPDATED_ON_TS_UTC) FROM {{ this }})
    {% endif %}

),

renamed AS (

    SELECT
        -- Surrogate key — must be first column
        MD5(CAST(ID AS VARCHAR) || '|' || 'ACCOUNTINGPERIOD')   AS SURROGATE_KEY,

        -- Natural key
        CAST(ID AS NUMBER(38,0))                                AS ID,

        -- Period flags
        ALLLOCKED                                               AS ALLLOCKED,
        ALLOWNONGCHANGES                                        AS ALLOW_NONGL_CHANGES,
        APLOCKED                                                AS APLOCKED,
        ARLOCKED                                                AS ARLOCKED,
        CLOSED                                                  AS CLOSED,
        TRY_TO_DATE(CLOSEDONDATE)                               AS CLOSED_ON_DATE,
        ENDDATE                                                 AS END_DATE,
        ISADJUST                                                AS ISADJUST,
        ISINACTIVE                                              AS IS_INACTIVE,
        ISPOSTING                                               AS IS_POSTING,
        ISQUARTER                                               AS IS_QUARTER,
        ISYEAR                                                  AS IS_YEAR,
        LASTMODIFIEDDATE                                        AS LAST_MODIFIED_DATE,
        NULLIF(TRIM(PERIODNAME), '')                            AS PERIOD_NAME,
        STARTDATE                                               AS START_DATE,
        _FIVETRAN_DELETED                                       AS FIVETRAN_DELETED,
        _FIVETRAN_SYNCED                                        AS FIVETRAN_SYNCED

    FROM source

),

final AS (

    SELECT
        renamed.*,

        {% if is_incremental() %}
        COALESCE(
            (SELECT MIN(existing.SILVER_CREATED_ON_TS_UTC)
             FROM {{ this }} existing
             WHERE existing.SURROGATE_KEY = renamed.SURROGATE_KEY),
            {{ dbt.current_timestamp_in_utc() }}
        )                                                       AS SILVER_CREATED_ON_TS_UTC,
        {% else %}
        {{ dbt.current_timestamp_in_utc() }}                    AS SILVER_CREATED_ON_TS_UTC,
        {% endif %}
        {{ dbt.current_timestamp_in_utc() }}                    AS SILVER_UPDATED_ON_TS_UTC,
        CAST(NULL AS TIMESTAMP_NTZ)                             AS SILVER_DELETED_ON_TS_UTC

    FROM renamed

)

SELECT * FROM final
