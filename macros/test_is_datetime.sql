{% test is_datetime(model, column_name) %}

  SELECT COUNT(*)
  FROM {{ model }}
  WHERE SAFE_CAST({{ column_name }} AS DATETIME) IS NULL
    AND {{ column_name }} IS NOT NULL

{% endtest %}
