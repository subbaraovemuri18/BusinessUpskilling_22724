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
  Model   : stg_ns_entity
  Layer   : staging
  Grain   : 1 row per STG_NS_ENTITY
  Schema  : static (explicit column list from Silver LLD)
  Cleaning: inline (TransformationRule expressions from Silver LLD)
  Source  : {{ source('ns', 'ENTITY') }}
  Watermark: LASTMODIFIEDDATE
#}

WITH source AS (

    SELECT *
    FROM {{ source('ns', 'ENTITY') }}
    {% if is_incremental() %}
    WHERE LASTMODIFIEDDATE > (SELECT MAX(SILVER_UPDATED_ON_TS_UTC) FROM {{ this }})
    {% endif %}

),

renamed AS (

    SELECT
        MD5(CAST(ID AS VARCHAR))                                                    AS SURROGATE_KEY,
        NULLIF(TRIM(ALTEMAIL), '')                                                  AS ALTEMAIL,
        NULLIF(TRIM(ALTNAME), '')                                                   AS ALTNAME,
        NULLIF(TRIM(ALTPHONE), '')                                                  AS ALTPHONE,
        NULLIF(TRIM(COMMENTS), '')                                                  AS COMMENTS,
        CAST(CONTACT AS NUMBER(38,0))                                               AS CONTACT,
        CAST(CUSTOMER AS NUMBER(38,0))                                              AS CUSTOMER,
        DATECREATED                                                                 AS DATE_CREATED,
        NULLIF(TRIM(DEFAULTTAXREG), '')                                             AS DEFAULT_TAX_REG,
        NULLIF(TRIM(EMAIL), '')                                                     AS EMAIL,
        CAST(EMPLOYEE AS NUMBER(38,0))                                              AS EMPLOYEE,
        CAST(ENTITYNUMBER AS NUMBER(38,0))                                          AS ENTITY_NUMBER,
        NULLIF(TRIM(ENTITYTITLE), '')                                               AS ENTITY_TITLE,
        NULLIF(TRIM(EXTERNALID), '')                                                AS EXTERNAL_ID,
        NULLIF(TRIM(FAX), '')                                                       AS FAX,
        NULLIF(TRIM(FIRSTNAME), '')                                                 AS FIRST_NAME,
        CAST("GROUP" AS NUMBER(38,0))                                               AS "GROUP",
        NULLIF(TRIM(HOMEPHONE), '')                                                 AS HOME_PHONE,
        CAST(ID AS NUMBER(38,0))                                                    AS ID,
        LASTMODIFIEDDATE                                                            AS LAST_MODIFIED_DATE,
        NULLIF(TRIM(LASTNAME), '')                                                  AS LAST_NAME,
        NULLIF(TRIM(MIDDLENAME), '')                                                AS MIDDLE_NAME,
        NULLIF(TRIM(MOBILEPHONE), '')                                               AS MOBILE_PHONE,
        CAST(OTHERNAME AS NUMBER(38,0))                                             AS OTHERNAME,
        CAST(PARENT AS NUMBER(38,0))                                                AS PARENT,
        CAST(PARTNER AS NUMBER(38,0))                                               AS "PARTNER",
        NULLIF(TRIM(PHONE), '')                                                     AS PHONE,
        CAST(PROJECT AS NUMBER(38,0))                                               AS PROJECT,
        NULLIF(TRIM(SALUTATION), '')                                                AS SALUTATION,
        NULLIF(TRIM(TITLE), '')                                                     AS TITLE,
        CAST(TOPLEVELPARENT AS NUMBER(38,0))                                        AS TOPLEVELPARENT,
        NULLIF(TRIM("TYPE"), '')                                                    AS "TYPE",
        CAST(VENDOR AS NUMBER(38,0))                                                AS VENDOR,
        NULLIF(TRIM(CUSTOMER_NAME), '')                                             AS CUSTOMER_NAME,
        NULLIF(TRIM(CUSTOMER_TYPE), '')                                             AS CUSTOMER_TYPE,
        NULLIF(TRIM(CUSTOMER_STATUS), '')                                           AS CUSTOMER_STATUS,
        NULLIF(TRIM(CREDIT_LIMIT), '')                                              AS CREDIT_LIMIT,
        ISINACTIVE                                                                  AS IS_INACTIVE,
        NULLIF(TRIM(ENTITYID), '')                                                  AS ENTITY_ID,
        CAST(LABORCOST AS NUMBER(38,6))                                             AS LABORCOST,
        ISPERSON                                                                    AS ISPERSON,
        CAST(GENERICRESOURCE AS NUMBER(38,0))                                       AS GENERIC_RESOURCE,
        ISUNAVAILABLE                                                               AS ISUNAVAILABLE,
        _FIVETRAN_DELETED                                                           AS FIVETRAN_DELETED,
        NULLIF(TRIM(SOURCE), '')                                                    AS SOURCE,
        CAST(PROJECTTEMPLATE AS NUMBER(38,0))                                       AS PROJECT_TEMPLATE,
        CAST(GLOBALSUBSCRIPTIONSTATUS AS NUMBER(38,0))                              AS GLOBAL_SUBSCRIPTION_STATUS,
        _FIVETRAN_SYNCED                                                            AS FIVETRAN_SYNCED,
        UNSUBSCRIBE                                                                 AS UNSUBSCRIBE,
        NULLIF(TRIM(FULLNAME), '')                                                  AS FULL_NAME

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
