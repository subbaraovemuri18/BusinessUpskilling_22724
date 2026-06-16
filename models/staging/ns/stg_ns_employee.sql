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
  Model   : stg_ns_employee
  Layer   : staging
  Grain   : 1 row per STG_NS_EMPLOYEE
  Schema  : static (explicit column list from Silver LLD)
  Cleaning: inline (TransformationRule expressions from Silver LLD)
  Source  : {{ source('ns', 'EMPLOYEE') }}
  Watermark: LASTMODIFIEDDATE
#}

WITH source AS (

    SELECT *
    FROM {{ source('ns', 'EMPLOYEE') }}
    {% if is_incremental() %}
    WHERE LASTMODIFIEDDATE > (SELECT MAX(SILVER_UPDATED_ON_TS_UTC) FROM {{ this }})
    {% endif %}

),

renamed AS (

    SELECT
        MD5(CAST(ID AS VARCHAR))                                                    AS SURROGATE_KEY,
        CAST(ID AS NUMBER(38,0))                                                    AS ID,
        NULLIF(TRIM(ACCOUNTNUMBER), '')                                             AS ACCOUNT_NUMBER,
        NULLIF(TRIM(ALIENNUMBER), '')                                               AS ALIENNUMBER,
        CAST(APPROVALLIMIT AS NUMBER(38,0))                                         AS APPROVAL_LIMIT,
        CAST(APPROVER AS NUMBER(38,0))                                              AS APPROVER,
        TRY_TO_DATE(AUTHWORKDATE)                                                   AS AUTHWORKDATE,
        CAST(BILLINGCLASS AS NUMBER(38,0))                                          AS BILLING_CLASS,
        BIRTHDATE                                                                   AS BIRTH_DATE,
        BTEMPLATE                                                                   AS BTEMPLATE,
        CAST(CLASS AS NUMBER(38,0))                                                 AS CLASS,
        NULLIF(TRIM(COMMENTS), '')                                                  AS COMMENTS,
        NULLIF(TRIM(COMMISSIONPAYMENTPREFERENCE), '')                               AS COMMISSION_PAYMENT_PREFERENCE,
        CAST(CURRENCY AS NUMBER(38,0))                                              AS CURRENCY,
        CUSTENTITY2                                                                 AS CUSTENTITY2,
        CAST(CUSTENTITY_COMMPCT AS NUMBER(38,6))                                    AS CUSTENTITY_COMMPCT,
        NULLIF(TRIM(CUSTENTITY_NSPBCS_EMP_PLANNING_CAT), '')                        AS CUSTENTITY_NSPBCS_EMP_PLANNING_CAT,
        NULLIF(TRIM(CUSTENTITY_RSS_LINKEDIN), '')                                   AS CUSTENTITY_RSS_LINKEDIN,
        NULLIF(TRIM(CUSTENTITY_RSS_WEBSITE), '')                                    AS CUSTENTITY_RSS_WEBSITE,
        CAST(CUSTENTITY_RSS_YOE AS NUMBER(38,0))                                    AS CUSTENTITY_RSS_YOE,
        DATECREATED                                                                 AS DATE_CREATED,
        NULLIF(TRIM(DEFAULTACCTCORPCARDEXP), '')                                    AS DEFAULTACCTCORPCARDEXP,
        CAST(DEFAULTEXPENSEREPORTCURRENCY AS NUMBER(38,0))                          AS DEFAULT_EXPENSE_REPORT_CURRENCY,
        CAST(DEFAULTJOBRESOURCEROLE AS NUMBER(38,0))                                AS DEFAULT_JOB_RESOURCEROLE,
        CAST(DEPARTMENT AS NUMBER(38,0))                                            AS DEPARTMENT,
        ELIGIBLEFORCOMMISSION                                                       AS ELIGIBLE_FOR_COMMISSION,
        NULLIF(TRIM(EMAIL), '')                                                     AS EMAIL,
        CAST(EMPLOYEESTATUS AS NUMBER(38,0))                                        AS EMPLOYEE_STATUS,
        CAST(EMPLOYEETYPE AS NUMBER(38,0))                                          AS EMPLOYEE_TYPE,
        NULLIF(TRIM(ENTITYID), '')                                                  AS ENTITY_ID,
        CAST(ETHNICITY AS NUMBER(38,0))                                             AS ETHNICITY,
        CAST(EXPENSELIMIT AS NUMBER(38,0))                                          AS EXPENSE_LIMIT,
        NULLIF(TRIM(EXTERNALID), '')                                                AS EXTERNAL_ID,
        NULLIF(TRIM(FAX), '')                                                       AS FAX,
        NULLIF(TRIM(FIRSTNAME), '')                                                 AS FIRST_NAME,
        NULLIF(TRIM(GENDER), '')                                                    AS GENDER,
        GIVEACCESS                                                                  AS GIVEACCESS,
        CAST(GLOBALSUBSCRIPTIONSTATUS AS NUMBER(38,0))                              AS GLOBAL_SUBSCRIPTION_STATUS,
        HIREDATE                                                                    AS HIRE_DATE,
        NULLIF(TRIM(HOMEPHONE), '')                                                 AS HOME_PHONE,
        I9VERIFIED                                                                  AS I_9_VERIFIED,
        NULLIF(TRIM(INITIALS), '')                                                  AS INITIALS,
        ISINACTIVE                                                                  AS IS_INACTIVE,
        ISJOBMANAGER                                                                AS IS_JOB_MANAGER,
        ISJOBRESOURCE                                                               AS IS_JOBRESOURCE,
        ISSALESREP                                                                  AS IS_SALES_REP,
        ISSUPPORTREP                                                                AS IS_SUPPORT_REP,
        NULLIF(TRIM(JOBDESCRIPTION), '')                                            AS JOB_DESCRIPTION,
        CAST(LABORCOST AS NUMBER(38,6))                                             AS LABORCOST,
        LASTMODIFIEDDATE                                                            AS LAST_MODIFIED_DATE,
        NULLIF(TRIM(LASTNAME), '')                                                  AS LAST_NAME,
        LASTREVIEWDATE                                                              AS LAST_REVIEW_DATE,
        CAST(LOCATION AS NUMBER(38,0))                                              AS LOCATION,
        CAST(MARITALSTATUS AS NUMBER(38,0))                                         AS MARITAL_STATUS,
        NULLIF(TRIM(MIDDLENAME), '')                                                AS MIDDLE_NAME,
        NULLIF(TRIM(MOBILEPHONE), '')                                               AS MOBILE_PHONE,
        NEXTREVIEWDATE                                                              AS NEXT_REVIEW_DATE,
        NULLIF(TRIM(OFFICEPHONE), '')                                               AS OFFICEPHONE,
        NULLIF(TRIM(PHONE), '')                                                     AS PHONE,
        CAST(PURCHASEORDERAPPROVALLIMIT AS NUMBER(38,0))                            AS PURCHASE_ORDER_APPROVAL_LIMIT,
        CAST(PURCHASEORDERAPPROVER AS NUMBER(38,0))                                 AS PURCHASE_ORDER_APPROVER,
        CAST(PURCHASEORDERLIMIT AS NUMBER(38,0))                                    AS PURCHASE_ORDER_LIMIT,
        TRY_TO_DATE(RELEASEDATE)                                                    AS RELEASE_DATE,
        CAST(RESIDENTSTATUS AS NUMBER(38,0))                                        AS RESIDENT_STATUS,
        NULLIF(TRIM(ROLESFORSEARCH), '')                                            AS ROLESFORSEARCH,
        NULLIF(TRIM(SALUTATION), '')                                                AS SALUTATION,
        CAST(SUBSIDIARY AS NUMBER(38,0))                                            AS SUBSIDIARY,
        CAST(SUPERVISOR AS NUMBER(38,0))                                            AS SUPERVISOR,
        CAST(TARGETUTILIZATION AS NUMBER(38,0))                                     AS TARGET_UTILIZATION,
        CAST(TIMEAPPROVER AS NUMBER(38,0))                                          AS TIME_APPROVER,
        NULLIF(TRIM(TITLE), '')                                                     AS TITLE,
        UNSUBSCRIBE                                                                 AS UNSUBSCRIBE,
        TRY_TO_DATE(VISAEXPDATE)                                                    AS VISAEXPDATE,
        NULLIF(TRIM(VISATYPE), '')                                                  AS VISA_TYPE,
        CAST(WORKCALENDAR AS NUMBER(38,0))                                          AS WORK_CALENDAR,
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
