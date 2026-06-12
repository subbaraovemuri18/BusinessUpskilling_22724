{#
  The output have been generated with the assistance of Claude at 2026-06-12T10:00:00Z UTC. The content has been verified by the designated engineer.

  Macro : generate_schema_name
  Purpose: Overrides dbt default schema name generation so that models always
           land in the schema declared in dbt_project.yml (+schema:) regardless
           of the target profile schema. Prevents dev/prod schema bleeding.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
