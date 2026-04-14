{% macro source_parquet(source_name, table_name, partition_key='ingest_dt') %}
  {#- Call source() to register the dependency in dbt's lineage graph -#}
  {%- set src = source(source_name, table_name) -%}
  
  {#- Return the actual DuckDB command to read the files -#}
  read_parquet('{{ var('bronze_base_path') }}/{{ table_name }}/{{ partition_key }}=*/part-*.parquet', union_by_name=true, hive_partitioning=true)
{%- endmacro %}

{% macro silver_parquet(table_name) %}
  {%- set base = var('silver_base_path') -%}
  read_parquet('{{ base }}/{{ table_name }}/**/*.parquet', union_by_name=true, hive_partitioning=true)
{%- endmacro %}

{% macro dims_parquet(table_name) %}
  {%- set base = var('dims_base_path') -%}
  read_parquet('{{ base }}/{{ table_name }}/**/*.parquet', union_by_name=true, hive_partitioning=true)
{%- endmacro %}
