{% macro main(name, id_key, cursor_key, partition_key) %}
SELECT
    *
EXCEPT
    (_rn)
FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY {{ id_key }} ORDER BY {{ cursor_key }} DESC) AS _rn
        FROM
            {{ source('Caresoft', partition_name(name)) }}
        WHERE
            DATE({{ partition_key }}) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
    )
WHERE
    _rn = 1
{% endmacro %}
