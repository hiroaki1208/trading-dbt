{% macro to_prev_weekday(date_expr) %}
(
    case
        -- 日曜なら金曜
        when extract(dayofweek from {{ date_expr }}) = 1
          then date_sub({{ date_expr }}, interval 2 day)
        -- 土曜なら金曜
        when extract(dayofweek from {{ date_expr }}) = 7
          then date_sub({{ date_expr }}, interval 1 day)
        -- 平日ならそのまま
        else {{ date_expr }}
    end
)
{% endmacro %}
