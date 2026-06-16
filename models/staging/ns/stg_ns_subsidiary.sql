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
  Model   : stg_ns_subsidiary
  Layer   : staging
  Grain   : 1 row per STG_NS_SUBSIDIARY
  Schema  : static (explicit column list from Silver LLD)
  Cleaning: inline (TransformationRule expressions from Silver LLD)
  Source  : {{ source('ns', 'SUBSIDIARY') }}
  Watermark: LASTMODIFIEDDATE
#}

WITH source AS (

    SELECT *
    FROM {{ source('ns', 'SUBSIDIARY') }}
    {% if is_incremental() %}
    WHERE LASTMODIFIEDDATE > (SELECT MAX(SILVER_UPDATED_ON_TS_UTC) FROM {{ this }})
    {% endif %}

),

renamed AS (

    SELECT
        MD5(CAST(ID AS VARCHAR))                                                    AS SURROGATE_KEY,
        CAST(ID AS NUMBER(38,0))                                                    AS ID,
        NULLIF(TRIM(COUNTRY), '')                                                   AS COUNTRY,
        CAST(CURRENCY AS NUMBER(38,0))                                              AS CURRENCY,
        NULLIF(TRIM(CUSTRECORD_BM_OFLD_BUDGET_CAT), '')                             AS CUSTRECORD_BM_OFLD_BUDGET_CAT,
        NULLIF(TRIM(CUSTRECORD_BVA_ALLOW_SAVE_NO_BUDGET), '')                       AS CUSTRECORD_BVA_ALLOW_SAVE_NO_BUDGET,
        NULLIF(TRIM(CUSTRECORD_NSPBCS_EPM_APPLICATION_NAME), '')                    AS CUSTRECORD_NSPBCS_EPM_APPLICATION_NAME,
        NULLIF(TRIM(CUSTRECORD_NSPBCS_EPM_URL), '')                                 AS CUSTRECORD_NSPBCS_EPM_URL,
        NULLIF(TRIM(CUSTRECORD_NSPBCS_EPM_USERNAME), '')                            AS CUSTRECORD_NSPBCS_EPM_USERNAME,
        NULLIF(TRIM(CUSTRECORD_SUBSIDIARY_ENABLE_BUDGET), '')                       AS CUSTRECORD_SUBSIDIARY_ENABLE_BUDGET,
        NULLIF(TRIM(DROPDOWNSTATE), '')                                             AS DROPDOWNSTATE,
        NULLIF(TRIM(EDITION), '')                                                   AS EDITION,
        NULLIF(TRIM(EMAIL), '')                                                     AS EMAIL,
        NULLIF(TRIM(EXTERNALID), '')                                                AS EXTERNAL_ID,
        NULLIF(TRIM(FAX), '')                                                       AS FAX,
        NULLIF(TRIM(FEDERALIDNUMBER), '')                                           AS FEDERALIDNUMBER,
        CAST(FISCALCALENDAR AS NUMBER(38,0))                                        AS FISCAL_CALENDAR,
        NULLIF(TRIM(FULLNAME), '')                                                  AS FULL_NAME,
        NULLIF(TRIM(INTERCOACCOUNT), '')                                            AS INTERCO_ACCOUNT,
        ISELIMINATION                                                               AS IS_ELIMINATION,
        ISINACTIVE                                                                  AS IS_INACTIVE,
        NULLIF(TRIM(LANGUAGELOCALE), '')                                            AS LANGUAGE_LOCALE,
        LASTMODIFIEDDATE                                                            AS LAST_MODIFIED_DATE,
        NULLIF(TRIM(LEGALNAME), '')                                                 AS LEGAL_NAME,
        CAST(MAINADDRESS AS NUMBER(38,0))                                           AS MAIN_ADDRESS,
        NULLIF(TRIM("NAME"), '')                                                    AS NAME,
        CAST(PARENT AS NUMBER(38,0))                                                AS PARENT,
        CAST(PURCHASEORDERAMOUNT AS NUMBER(38,6))                                   AS PURCHASE_ORDER_AMOUNT,
        TRY_TO_NUMBER(PURCHASEORDERQUANTITY, 38, 6)                                 AS PURCHASE_ORDER_QUANTITY,
        NULLIF(TRIM(PURCHASEORDERQUANTITYDIFF), '')                                 AS PURCHASEORDERQUANTITYDIFF,
        TRY_TO_NUMBER(RECEIPTAMOUNT, 38, 6)                                         AS RECEIPT_AMOUNT,
        TRY_TO_NUMBER(RECEIPTQUANTITY, 38, 6)                                       AS RECEIPT_QUANTITY,
        NULLIF(TRIM(RECEIPTQUANTITYDIFF), '')                                       AS RECEIPTQUANTITYDIFF,
        CAST(REPRESENTINGCUSTOMER AS NUMBER(38,0))                                  AS REPRESENTINGCUSTOMER,
        CAST(REPRESENTINGVENDOR AS NUMBER(38,0))                                    AS REPRESENTINGVENDOR,
        CAST(RETURNADDRESS AS NUMBER(38,0))                                         AS RETURN_ADDRESS,
        CAST(SHIPPINGADDRESS AS NUMBER(38,0))                                       AS SHIPPING_ADDRESS,
        SHOWSUBSIDIARYNAME                                                          AS SHOW_SUBSIDIARY_NAME,
        NULLIF(TRIM(SSNORTIN), '')                                                  AS SSN_OR_TIN,
        NULLIF(TRIM(STATE), '')                                                     AS STATE,
        CAST(STATE1TAXNUMBER AS NUMBER(38,6))                                       AS STATE_1_TAX_NUMBER,
        NULLIF(TRIM(TRANPREFIX), '')                                                AS TR_AN_PREFIX,
        NULLIF(TRIM(URL), '')                                                       AS URL,
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
