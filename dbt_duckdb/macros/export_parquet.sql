{% macro export_parquet(model_relation, dest_path) %}
  {% if execute %}
    {% set sql %}copy (select * from {{ model_relation }}) to '{{ dest_path }}' (format parquet);{% endset %}
    {% do run_query(sql) %}
  {% endif %}
{% endmacro %}
