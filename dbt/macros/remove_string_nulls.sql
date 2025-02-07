{% macro remove_string_nulls(column_name) %}

    case 
    when lower({{ column_name }}) = 'null' then null
    else {{ column_name }}
    end
    
{% endmacro %}