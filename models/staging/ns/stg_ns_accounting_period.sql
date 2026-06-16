-- The output have been generated with the assistance of Claude at 2026-06-16T10:00:00Z UTC. The content has been verified by the designated engineer.

{{
  config(
    materialized         = 'incremental',
    unique_key           = 'SURROGATE_KEY',
    incremental_strategy = 'merge',
    on_schema_change     = 'fail',
    tags                 = ['silver', 'staging', 'netsuite']
  )
}}

{#
  Model   : stg_ns_accounting_period
  Layer   : staging
  Grain   : 1 row per STG_NS_ACCOUNTING_PERIOD
  Schema  : static (explicit column list from Silver LLD)
  Cleaning: inline (TransformationRule expressions from Silver LLD)
  Source  : {{ source('ns', 'ACCOUNTINGPERIOD') }}
  Watermark: LASTMODIFIEDDATE
#}

WITH source AS (

    SELECT *
    FROM {{ source('ns', 'ACCOUNTINGPERIOD') }}
    {% if is_incremental() %}
    WHERE LASTMODIFIEDDATE > (SELECT MAX(SILVER_UPDATED_ON_TS_UTC) FROM {{ this }})
    {% endif %}

),

renamed AS (

    SELECT
        MD5(CAST(ID AS VARCHAR))                                                    AS SURROGATE_KEY,
        CAST(ID AS NUMBER(38,0))                                                    AS ID,
        ALLLOCKED                                                                   AS ALLLOCKED,
        ALLOWNONGLCHANGES                                                           AS ALLOW_NONGL_CHANGES,
        APLOCKED                                                                    AS APLOCKED,
        ARLOCKED                                                                    AS ARLOCKED,
        CLOSED                                                                      AS CLOSED,
        TRY_TO_DATE(CLOSEDONDATE)                                                   AS CLOSED_ON_DATE,
        ENDDATE                                                                     AS END_DATE,
        ISADJUST                                                                    AS ISADJUST,
        ISINACTIVE                                                                  AS IS_INACTIVE,
        ISPOSTING                                                                   AS IS_POSTING,
        ISQUARTER                                                                   AS IS_QUARTER,
        ISYEAR                                                                      AS IS_YEAR,
        LASTMODIFIEDDATE                                                            AS LAST_MODIFIED_DATE,
        NULLIF(TRIM(PERIODNAME), '')                                                AS PERIOD_NAME,
        STARTDATE                                                                   AS START_DATE,
        _FIVETRAN_DELETED                                                           AS FIVETRAN_DELETED,
        _FIVETRAN_SYNCED                                                            AS FIVETRAN_SYNCED

    FROM source

),

final AS (

    SELECT
        renamed.*,

        -- Audit metadata
        {% if is_incremental() %}
        COALESCE(
            (SELECT MIN(existing.SILVER_CREATED_ON_TS_UTC)
             FROM {{ this }} existing
             WHERE existing.SURROGATE_KEY = renamed.SURROGATE_KEY),
            CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())
        )                                                                       AS SILVER_CREATED_ON_TS_UTC,
        {% else %}
        CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())                                    AS SILVER_CREATED_ON_TS_UTC,
        {% endif %}
        CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())                                    AS SILVER_UPDATED_ON_TS_UTC,
        CAST(NULL AS TIMESTAMP_NTZ)                               AS SILVER_DELETED_ON_TS_UTC

    FROM renamed

)

SELECT * FROM final
