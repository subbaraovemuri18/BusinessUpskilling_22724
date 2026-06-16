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
  Model   : stg_ns_transaction_status
  Layer   : staging
  Grain   : 1 row per STG_NS_TRANSACTION_STATUS
  Schema  : static (explicit column list from Silver LLD)
  Cleaning: inline (TransformationRule expressions from Silver LLD)
  Source  : {{ source('ns', 'TRANSACTIONSTATUS') }}
  Watermark: INGESTION_TIME
#}

WITH source AS (

    SELECT *
    FROM {{ source('ns', 'TRANSACTIONSTATUS') }}
    {% if is_incremental() %}
    WHERE INGESTION_TIME > (SELECT MAX(SILVER_UPDATED_ON_TS_UTC) FROM {{ this }})
    {% endif %}

),

renamed AS (

    SELECT
        MD5(CAST(TRAN_CUSTOM_TYPE_ID AS VARCHAR) || '|' || CAST(TRANSACTION_STATUS_ID AS VARCHAR)) AS SURROGATE_KEY,
        CAST(TRANSACTION_STATUS_ID AS NUMBER(38,0))                                 AS TRANSACTION_STATUS_ID,
        NULLIF(TRIM(TRANSACTION_STATUS_FULL_NAME), '')                              AS TRANSACTION_STATUS_FULL_NAME,
        NULLIF(TRIM(TRANSACTION_STATUS_NAME), '')                                   AS TRANSACTION_STATUS_NAME,
        CAST(TRAN_CUSTOM_TYPE_ID AS NUMBER(38,0))                                   AS TRANSACTION_CUSTOM_TYPE_ID,
        NULLIF(TRIM(TRANSACTION_TYPE), '')                                          AS TRANSACTION_TYPE,
        INGESTION_TIME                                                              AS INGESTION_TIME

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
