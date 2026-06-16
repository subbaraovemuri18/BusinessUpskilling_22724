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
  Model   : stg_ns_consolidated_exchange_rate
  Layer   : staging
  Grain   : 1 row per STG_NS_CONSOLIDATED_EXCHANGE_RATE
  Schema  : static (explicit column list from Silver LLD)
  Cleaning: inline (TransformationRule expressions from Silver LLD)
  Source  : {{ source('ns', 'CONSOLIDATEDEXCHANGERATE') }}
  Watermark: _FIVETRAN_SYNCED
#}

WITH source AS (

    SELECT *
    FROM {{ source('ns', 'CONSOLIDATEDEXCHANGERATE') }}
    {% if is_incremental() %}
    WHERE _FIVETRAN_SYNCED > (SELECT MAX(SILVER_UPDATED_ON_TS_UTC) FROM {{ this }})
    {% endif %}

),

renamed AS (

    SELECT
        MD5(CAST(ID AS VARCHAR))                                                    AS SURROGATE_KEY,
        CAST(ID AS NUMBER(38,0))                                                    AS ID,
        CAST(ACCOUNTINGBOOK AS NUMBER(38,0))                                        AS ACCOUNTING_BOOK,
        CAST(AVERAGERATE AS NUMBER(38,6))                                           AS AVERAGE_RATE,
        CAST(CURRENTRATE AS NUMBER(38,6))                                           AS CURRENT_RATE,
        NULLIF(TRIM(EXTERNALID), '')                                                AS EXTERNAL_ID,
        CAST(FROMCURRENCY AS NUMBER(38,0))                                          AS FROM_CURRENCY,
        CAST(FROMSUBSIDIARY AS NUMBER(38,0))                                        AS FROM_SUBSIDIARY,
        CAST(HISTORICALRATE AS NUMBER(38,6))                                        AS HISTORICAL_RATE,
        ISDERIVED                                                                   AS IS_DERIVED,
        ISELIMINATIONSUBSIDIARY                                                     AS IS_ELIMINATION_SUBSIDIARY,
        ISPERIODCLOSED                                                              AS IS_PERIOD_CLOSED,
        PERIODSTARTDATE                                                             AS PERIOD_START_DATE,
        CAST(POSTINGPERIOD AS NUMBER(38,0))                                         AS POSTING_PERIOD,
        CAST(TOCURRENCY AS NUMBER(38,0))                                            AS TO_CURRENCY,
        CAST(TOSUBSIDIARY AS NUMBER(38,0))                                          AS TO_SUBSIDIARY,
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
