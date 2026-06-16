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
  Model   : stg_ns_account
  Layer   : staging
  Grain   : 1 row per STG_NS_ACCOUNT
  Schema  : static (explicit column list from Silver LLD)
  Cleaning: inline (TransformationRule expressions from Silver LLD)
  Source  : {{ source('ns', 'ACCOUNT') }}
  Watermark: LASTMODIFIEDDATE
#}

WITH source AS (

    SELECT *
    FROM {{ source('ns', 'ACCOUNT') }}
    {% if is_incremental() %}
    WHERE LASTMODIFIEDDATE > (SELECT MAX(SILVER_UPDATED_ON_TS_UTC) FROM {{ this }})
    {% endif %}

),

renamed AS (

    SELECT
        MD5(CAST(ID AS VARCHAR))                                                    AS SURROGATE_KEY,
        CAST(ID AS NUMBER(38,0))                                                    AS ID,
        NULLIF(TRIM(ACCOUNTSEARCHDISPLAYNAME), '')                                  AS ACCOUNT_SEARCH_DISPLAY_NAME,
        NULLIF(TRIM(ACCOUNTSEARCHDISPLAYNAMECOPY), '')                              AS ACCOUNT_SEARCH_DISPLAY_NAME_COPY,
        NULLIF(TRIM(ACCTNUMBER), '')                                                AS ACCOUNT_NUMBER,
        NULLIF(TRIM(ACCTTYPE), '')                                                  AS ACCOUNT_TYPE,
        NULLIF(TRIM(BILLABLEEXPENSESACCT), '')                                      AS BILLABLE_EXPENSES_ACCOUNT,
        TRY_TO_NUMBER(CASHFLOWRATE, 38, 6)                                          AS CASHFLOWRATE,
        CAST(CATEGORY1099MISC AS NUMBER(38,0))                                      AS CATEGORY_1099_MISC,
        CAST(CLASS AS NUMBER(38,0))                                                 AS CLASS,
        CAST(CURRENCY AS NUMBER(38,0))                                              AS CURRENCY,
        CUSTRECORD_ABI_AP_ACCOUNT                                                   AS CUSTRECORD_ABI_AP_ACCOUNT,
        NULLIF(TRIM(CUSTRECORD_BM_BUDGETACCOUNT), '')                               AS CUSTRECORD_BM_BUDGETACCOUNT,
        CAST(DEFERRALACCT AS NUMBER(38,0))                                          AS DEFERRAL_ACCOUNT,
        NULLIF(TRIM(DEPARTMENT), '')                                                AS DEPARTMENT,
        NULLIF(TRIM(DESCRIPTION), '')                                               AS DESCRIPTION,
        NULLIF(TRIM(DISPLAYNAMEWITHHIERARCHY), '')                                  AS DISPLAY_NAME_WITH_HIERARCHY,
        ELIMINATE                                                                   AS ELIMINATE,
        NULLIF(TRIM(EXTERNALID), '')                                                AS EXTERNAL_ID,
        NULLIF(TRIM(FULLNAME), '')                                                  AS FULL_NAME,
        TRY_TO_NUMBER(GENERALRATE, 38, 6)                                           AS GENERAL_RATE,
        INCLUDECHILDREN                                                             AS INCLUDECHILDREN,
        INVENTORY                                                                   AS INVENTORY,
        ISINACTIVE                                                                  AS IS_INACTIVE,
        ISSUMMARY                                                                   AS IS_SUMMARY,
        LASTMODIFIEDDATE                                                            AS LAST_MODIFIED_DATE,
        CAST(LOCATION AS NUMBER(38,0))                                              AS LOCATION,
        CAST(PARENT AS NUMBER(38,0))                                                AS PARENT,
        RECONCILEWITHMATCHING                                                       AS RECONCILE_WITH_MATCHING,
        NULLIF(TRIM(RESTRICTTOACCOUNTINGBOOK), '')                                  AS RESTRICT_TO_ACCOUNTING_BOOK,
        REVALUE                                                                     AS REVALUE,
        NULLIF(TRIM(SBANKNAME), '')                                                 AS SBANKNAME,
        CAST(SBANKROUTINGNUMBER AS NUMBER(38,0))                                    AS SBANKROUTINGNUMBER,
        NULLIF(TRIM(SSPECACCT), '')                                                 AS SSPECACCT,
        NULLIF(TRIM(SUBSIDIARY), '')                                                AS SUBSIDIARY,
        CAST(UNIT AS NUMBER(38,0))                                                  AS UNIT,
        CAST(UNITSTYPE AS NUMBER(38,0))                                             AS UNITS_TYPE,
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
