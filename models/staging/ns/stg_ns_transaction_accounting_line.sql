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
  Model   : stg_ns_transaction_accounting_line
  Layer   : staging
  Grain   : 1 row per STG_NS_TRANSACTION_ACCOUNTING_LINE
  Schema  : static (explicit column list from Silver LLD)
  Cleaning: inline (TransformationRule expressions from Silver LLD)
  Source  : {{ source('ns', 'TRANSACTIONACCOUNTINGLINE') }}
  Watermark: LASTMODIFIEDDATE
#}

WITH source AS (

    SELECT *
    FROM {{ source('ns', 'TRANSACTIONACCOUNTINGLINE') }}
    {% if is_incremental() %}
    WHERE LASTMODIFIEDDATE > (SELECT MAX(SILVER_UPDATED_ON_TS_UTC) FROM {{ this }})
    {% endif %}

),

renamed AS (

    SELECT
        MD5(CAST(TRANSACTION AS VARCHAR) || '|' || CAST(TRANSACTIONLINE AS VARCHAR) || '|' || CAST(ACCOUNTINGBOOK AS VARCHAR)) AS SURROGATE_KEY,
        CAST("TRANSACTION" AS NUMBER(38,0))                                         AS TRANSACTION,
        CAST(ACCOUNTINGBOOK AS NUMBER(38,0))                                        AS ACCOUNTINGBOOK,
        CAST(TRANSACTIONLINE AS NUMBER(38,0))                                       AS TRANSACTIONLINE,
        CAST("ACCOUNT" AS NUMBER(38,0))                                             AS ACCOUNT,
        NULLIF(TRIM(ACCOUNTTYPE), '')                                               AS ACCOUNT_TYPE,
        CAST(AMOUNT AS NUMBER(38,6))                                                AS AMOUNT,
        CAST(AMOUNTLINKED AS NUMBER(38,6))                                          AS AMOUNT_LINKED,
        CAST(AMOUNTPAID AS NUMBER(38,0))                                            AS AMOUNT_PAID,
        CAST(AMOUNTUNPAID AS NUMBER(38,0))                                          AS AMOUNTUNPAID,
        NULLIF(TRIM(CATCHUPPERIOD), '')                                             AS CATCHUPPERIOD,
        CAST(CREDIT AS NUMBER(38,6))                                                AS CREDIT,
        CAST(DEBIT AS NUMBER(38,6))                                                 AS DEBIT,
        DEFERREVREC                                                                 AS DEFERREVREC,
        CAST(EXCHANGERATE AS NUMBER(38,6))                                          AS EXCHANGE_RATE,
        LASTMODIFIEDDATE                                                            AS LAST_MODIFIED_DATE,
        CAST(NETAMOUNT AS NUMBER(38,6))                                             AS NET_AMOUNT,
        NULLIF(TRIM(OVERHEADPARENTITEM), '')                                        AS OVERHEADPARENTITEM,
        CAST(PAYMENTAMOUNTUNUSED AS NUMBER(38,6))                                   AS PAYMENT_AMOUNT_UNUSED,
        CAST(PAYMENTAMOUNTUSED AS NUMBER(38,6))                                     AS PAYMENT_AMOUNT_USED,
        POSTING                                                                     AS POSTING,
        PROCESSEDBYREVCOMMIT                                                        AS PROCESSEDBYREVCOMMIT,
        REVENUECOMMITTEDDATE                                                        AS REVENUE_COMMITTED_DATE,
        REVRECENDDATE                                                               AS REVREC_END_DATE,
        CAST(REVRECSCHEDULE AS NUMBER(38,0))                                        AS REVREC_SCHEDULE,
        REVRECSTARTDATE                                                             AS REVREC_START_DATE,
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
