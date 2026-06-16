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
  Model   : stg_ns_location
  Layer   : staging
  Grain   : 1 row per STG_NS_LOCATION
  Schema  : static (explicit column list from Silver LLD)
  Cleaning: inline (TransformationRule expressions from Silver LLD)
  Source  : {{ source('ns', 'LOCATION') }}
  Watermark: LASTMODIFIEDDATE
#}

WITH source AS (

    SELECT *
    FROM {{ source('ns', 'LOCATION') }}
    {% if is_incremental() %}
    WHERE LASTMODIFIEDDATE > (SELECT MAX(SILVER_UPDATED_ON_TS_UTC) FROM {{ this }})
    {% endif %}

),

renamed AS (

    SELECT
        MD5(CAST(ID AS VARCHAR))                                                    AS SURROGATE_KEY,
        CAST(ID AS NUMBER(38,0))                                                    AS ID,
        CAST(AUTOASSIGNMENTREGIONSETTING AS NUMBER(38,0))                           AS AUTO_ASSIGNMENT_REGION_SETTING,
        NULLIF(TRIM(BUFFERSTOCK), '')                                               AS BUFFERSTOCK,
        CAST(CUSTRECORD1399 AS NUMBER(38,0))                                        AS CUSTRECORD1399,
        CAST(CUSTRECORD1400 AS NUMBER(38,0))                                        AS CUSTRECORD1400,
        NULLIF(TRIM(CUSTRECORD_NSPBCS_LOC_PLANNING_CAT), '')                        AS CUSTRECORD_NSPBCS_LOC_PLANNING_CAT,
        CAST(CUSTRECORDCUSTRECORD_STORE_SQ_FT AS NUMBER(38,0))                      AS CUSTRECORDCUSTRECORD_STORE_SQ_FT,
        NULLIF(TRIM(DEFAULTALLOCATIONPRIORITY), '')                                 AS DEFAULTALLOCATIONPRIORITY,
        NULLIF(TRIM(EXTERNALID), '')                                                AS EXTERNAL_ID,
        NULLIF(TRIM(FULLNAME), '')                                                  AS FULL_NAME,
        NULLIF(TRIM(GEOLOCATIONMETHOD), '')                                         AS GEOLOCATIONMETHOD,
        INCLUDEINSUPPLYPLANNING                                                     AS INCLUDEINSUPPLYPLANNING,
        NULLIF(TRIM(INVTTURNOVERVELOCITY), '')                                      AS INVTTURNOVERVELOCITY,
        ISINACTIVE                                                                  AS IS_INACTIVE,
        LASTMODIFIEDDATE                                                            AS LAST_MODIFIED_DATE,
        NULLIF(TRIM(LATITUDE), '')                                                  AS LATITUDE,
        CAST(LOCATIONTYPE AS NUMBER(38,0))                                          AS LOCATION_TYPE,
        NULLIF(TRIM(LONGITUDE), '')                                                 AS LONGITUDE,
        CAST(MAINADDRESS AS NUMBER(38,0))                                           AS MAIN_ADDRESS,
        MAKEINVENTORYAVAILABLE                                                      AS MAKE_INVENTORY_AVAILABLE,
        MAKEINVENTORYAVAILABLESTORE                                                 AS MAKE_INVENTORY_AVAILABLE_STORE,
        NULLIF(TRIM("NAME"), '')                                                    AS NAME,
        CAST(PARENT AS NUMBER(38,0))                                                AS PARENT,
        CAST(RETURNADDRESS AS NUMBER(38,0))                                         AS RETURN_ADDRESS,
        CAST(SUBSIDIARY AS NUMBER(38,0))                                            AS SUBSIDIARY,
        NULLIF(TRIM(TRANPREFIX), '')                                                AS TR_AN_PREFIX,
        USEBINS                                                                     AS USEBINS,
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
