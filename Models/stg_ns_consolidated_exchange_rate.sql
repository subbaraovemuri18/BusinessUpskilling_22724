{{
    config(
        materialized        = 'incremental',
        unique_key          = 'SURROGATE_KEY',
        incremental_strategy= 'merge',
        on_schema_change    = 'fail',
        tags                = ['silver', 'staging', 'netsuite', 'account']
    )
}}

{#
  Model  : stg_ns_account
  Layer  : Silver — Staging
  Grain  : One row per NetSuite GL account (ACCOUNT.ID)
  Schema : BUI_SUBBARAO_VEMURI_DB.SILVER
  Source : {{ source('ns', 'ACCOUNT') }} — BUI_SUBBARAO_VEMURI_DB.RAW_NETSUITE_22724.ACCOUNT
  Cleaning: Inline (LLD TransformationRule applied directly)
  FX Note: CASHFLOWRATE (Operating/Investing/Financing) and GENERAL_RATE (Average/Current/Historical)
           are critical for downstream Gold layer financial statement routing and FX translation.
  Watermark: _FIVETRAN_SYNCED (Bronze column) compared against MAX(SILVER_UPDATED_ON_TS_UTC) on target.
#}

WITH source AS (

    SELECT *
    FROM {{ source('ns', 'ACCOUNT') }}
    {% if is_incremental() %}
    WHERE _FIVETRAN_SYNCED > (SELECT MAX(SILVER_UPDATED_ON_TS_UTC) FROM {{ this }})
    {% endif %}

),

renamed AS (

    SELECT
        -- Surrogate key — must be first column
        MD5(CAST(ID AS VARCHAR) || '|' || 'ACCOUNT')            AS SURROGATE_KEY,

        -- Natural key
        CAST(ID AS NUMBER(38,0))                                AS ID,

        -- Descriptive attributes
        NULLIF(TRIM(ACCOUNTSEARCHDISPLAYNAME), '')               AS ACCOUNT_SEARCH_DISPLAY_NAME,
        NULLIF(TRIM(ACCOUNTSEARCHDISPLAYNAMECOPY), '')           AS ACCOUNT_SEARCH_DISPLAY_NAME_COPY,
        NULLIF(TRIM(ACCTNUMBER), '')                             AS ACCOUNT_NUMBER,
        NULLIF(TRIM(ACCTTYPE), '')                               AS ACCOUNT_TYPE,
        NULLIF(TRIM(BILLABLEEXPENSESACCT), '')                   AS BILLABLE_EXPENSES_ACCOUNT,
        TRY_TO_NUMBER(CASHFLOWRATE, 38, 6)                      AS CASHFLOWRATE,
        CAST(CATEGORY1099MISC AS NUMBER(38,0))                   AS CATEGORY_1099_MISC,
        CAST(CLASS AS NUMBER(38,0))                              AS CLASS,
        CAST(CURRENCY AS NUMBER(38,0))                           AS CURRENCY,
        CUSTRECORD_ABI_AP_ACCOUNT                                AS CUSTRECORD_ABI_AP_ACCOUNT,
        NULLIF(TRIM(CUSTRECORD_BM_BUDGETACCOUNT), '')            AS CUSTRECORD_BM_BUDGETACCOUNT,
        CAST(DEFERRALACCT AS NUMBER(38,0))                       AS DEFERRAL_ACCOUNT,
        NULLIF(TRIM(DEPARTMENT), '')                             AS DEPARTMENT,
        NULLIF(TRIM(DESCRIPTION), '')                            AS DESCRIPTION,
        NULLIF(TRIM(DISPLAYNAMEWITHHEIERARCHY), '')              AS DISPLAY_NAME_WITH_HIERARCHY,
        ELIMINATE                                                AS ELIMINATE,
        NULLIF(TRIM(EXTERNALID), '')                             AS EXTERNAL_ID,
        NULLIF(TRIM(FULLNAME), '')                               AS FULL_NAME,
        TRY_TO_NUMBER(GENERALRATE, 38, 6)                       AS GENERAL_RATE,
        INCLUDECHILDREN                                          AS INCLUDECHILDREN,
        INVENTORY                                                AS INVENTORY,
        ISINACTIVE                                               AS IS_INACTIVE,
        ISSUMMARY                                                AS IS_SUMMARY,
        LASTMODIFIEDDATE                                         AS LAST_MODIFIED_DATE,
        CAST(LOCATION AS NUMBER(38,0))                           AS LOCATION,
        CAST(PARENT AS NUMBER(38,0))                             AS PARENT,
        RECONCILEWITHMATCHING                                    AS RECONCILE_WITH_MATCHING,
        NULLIF(TRIM(RESTRICTTOACCTBOOK), '')                     AS RESTRICT_TO_ACCOUNTING_BOOK,
        REVALUE                                                  AS REVALUE,
        NULLIF(TRIM(SBANKNAME), '')                              AS SBANKNAME,
        CAST(SBANKROUTINGNUMBER AS NUMBER(38,0))                 AS SBANKROUTINGNUMBER,
        NULLIF(TRIM(SSPECACCT), '')                              AS SSPECACCT,
        NULLIF(TRIM(SUBSIDIARY), '')                             AS SUBSIDIARY,
        CAST(UNIT AS NUMBER(38,0))                               AS UNIT,
        CAST(UNITSTYPE AS NUMBER(38,0))                          AS UNITS_TYPE,
        _FIVETRAN_DELETED                                        AS FIVETRAN_DELETED,
        _FIVETRAN_SYNCED                                         AS FIVETRAN_SYNCED

    FROM source

),

final AS (

    SELECT
        renamed.*,

        -- Audit metadata — INSERT-only for created, always updated for updated
        {% if is_incremental() %}
        COALESCE(
            (SELECT MIN(existing.SILVER_CREATED_ON_TS_UTC)
             FROM {{ this }} existing
             WHERE existing.SURROGATE_KEY = renamed.SURROGATE_KEY),
            {{ dbt.current_timestamp_in_utc() }}
        )                                                        AS SILVER_CREATED_ON_TS_UTC,
        {% else %}
        {{ dbt.current_timestamp_in_utc() }}                     AS SILVER_CREATED_ON_TS_UTC,
        {% endif %}
        {{ dbt.current_timestamp_in_utc() }}                     AS SILVER_UPDATED_ON_TS_UTC,
        CAST(NULL AS TIMESTAMP_NTZ)                              AS SILVER_DELETED_ON_TS_UTC

    FROM renamed

)

SELECT * FROM final
