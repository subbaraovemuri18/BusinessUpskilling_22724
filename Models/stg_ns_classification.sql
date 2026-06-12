# The output have been generated with the assistance of Claude at 2026-06-12T10:00:00Z UTC. The content has been verified by the designated engineer.

version: 2

sources:
  - name: ns
    description: "NetSuite ERP Bronze tables — raw Fivetran extracts from RAW_NETSUITE_22724. One table per NetSuite entity. No transformations applied at this layer."
    database: "BUI_SUBBARAO_VEMURI_DB"
    schema:   "RAW_NETSUITE_22724"
    freshness:
      warn_after:  {count: 25, period: hour}
      error_after: {count: 49, period: hour}
    loaded_at_field: _FIVETRAN_SYNCED

    tables:
      - name: ACCOUNT
        description: "Chart of Accounts — one row per GL account. ACCTTYPE drives financial statement routing."
      - name: ACCOUNTINGPERIOD
        description: "Fiscal calendar — one row per posting period (month/quarter/year)."
      - name: CLASSIFICATION
        description: "Classification segment — business line / product group dimension."
      - name: CONSOLIDATEDEXCHANGERATE
        description: "Period-level FX rates for multi-currency consolidation (AVERAGE, CURRENT, HISTORICAL)."
      - name: CURRENCY
        description: "Currency master — functional currencies for each subsidiary."
      - name: DEPARTMENT
        description: "Department hierarchy — cost centre dimension for P&L slicing."
      - name: EMPLOYEE
        description: "Employee master — referenced by expense and payroll transactions."
      - name: ENTITY
        description: "Unified counterparty master — customers, vendors, employees."
      - name: ITEM
        description: "Product and service item master — referenced on transaction lines."
      - name: LOCATION
        description: "Physical and operational location dimension."
      - name: SUBSIDIARY
        description: "Legal entity hierarchy — five portfolio companies plus PCP Holdings parent."
      - name: TRANSACTION
        description: "Transaction header — one row per financial document (journal, invoice, bill)."
      - name: TRANSACTIONLINE
        description: "Transaction line detail — one row per segment combination per transaction."
