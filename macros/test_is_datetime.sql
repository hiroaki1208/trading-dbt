{% test is_datetime(model, column_name) %}

  SELECT COUNT(*)
  FROM {{ model }}
  WHERE {{ column_name }} IS NOT NULL
    AND (
      SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', CAST({{ column_name }} AS STRING)) IS NULL
      AND SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST({{ column_name }} AS STRING)) IS NULL
    )

{% endtest %}