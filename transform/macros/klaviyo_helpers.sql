{#- True when models should read the SYNTHETIC example seeds instead of the
    real sources. Defaults to true on duckdb only. -#}
{% macro klaviyo_use_example_data() %}
  {{ return(var('klaviyo_use_example_data', target.type == 'duckdb')) }}
{% endmacro %}


{#- Pick the real source or the synthetic seed for one input. -#}
{% macro klaviyo_input(source_name, table_name, example_seed) %}
  {% if klaviyo_use_example_data() %}
    {{ return(ref(example_seed)) }}
  {% else %}
    {{ return(source(source_name, table_name)) }}
  {% endif %}
{% endmacro %}


{#- Metric map: the user-filled seed, or the synthetic one for local runs. -#}
{% macro klaviyo_metric_map_relation() %}
  {% if klaviyo_use_example_data() %}
    {{ return(ref('example_synthetic_metric_map')) }}
  {% else %}
    {{ return(ref('klaviyo_metric_map')) }}
  {% endif %}
{% endmacro %}


{#- Turn a metric name into a column prefix: "Opened Email" -> opened_email -#}
{% macro klaviyo_metric_slug(metric_name) %}
  {{ return(modules.re.sub('[^a-z0-9]+', '_', metric_name | lower).strip('_')) }}
{% endmacro %}


{#- Timestamp (with or without zone) to a UTC timestamp without zone. -#}
{% macro klaviyo_to_utc(column) %}
  {{ return(adapter.dispatch('klaviyo_to_utc', 'klaviyo_transform')(column)) }}
{% endmacro %}

{% macro snowflake__klaviyo_to_utc(column) %}
  convert_timezone('UTC', {{ column }})::timestamp_ntz
{% endmacro %}

{% macro default__klaviyo_to_utc(column) %}
  cast({{ column }} as {{ dbt.type_timestamp() }})
{% endmacro %}


{#- Reference point for activity flags, as a UTC timestamp without zone. -#}
{% macro klaviyo_now_utc() %}
  {% set as_of = var('klaviyo_as_of_timestamp', none) %}
  {% if as_of %}
    {{ return("cast('" ~ as_of ~ "' as " ~ dbt.type_timestamp() ~ ")") }}
  {% endif %}
  {{ return(adapter.dispatch('klaviyo_now_utc', 'klaviyo_transform')()) }}
{% endmacro %}

{% macro snowflake__klaviyo_now_utc() %}
  convert_timezone('UTC', current_timestamp())::timestamp_ntz
{% endmacro %}

{% macro duckdb__klaviyo_now_utc() %}
  cast(timezone('UTC', current_timestamp) as timestamp)
{% endmacro %}

{% macro default__klaviyo_now_utc() %}
  cast({{ dbt.current_timestamp() }} as {{ dbt.type_timestamp() }})
{% endmacro %}
