{% test expression_is_true(model, expression, column_name=none, where_clause=none) %}
select *
from {{ model }}
where not ({{ expression }})
{% if where_clause %}and ({{ where_clause }}){% endif %}
{% endtest %}


{% test unique_combination_of_columns(model, combination_of_columns) %}
select {{ combination_of_columns | join(', ') }}, count(*) as occurrences
from {{ model }}
group by {{ combination_of_columns | join(', ') }}
having count(*) > 1
{% endtest %}


{% test within_range(model, column_name, min_value=none, max_value=none) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and (
      false
      {% if min_value is not none %} or {{ column_name }} < {{ min_value }}{% endif %}
      {% if max_value is not none %} or {{ column_name }} > {{ max_value }}{% endif %}
  )
{% endtest %}
