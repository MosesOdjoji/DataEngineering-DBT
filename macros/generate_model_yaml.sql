{# 
Description : This macro generates a YAML for DBT source,stage and rest of the models.
Arguments   :
    1. type - It accepts source,stage and other as input denoting the type of model's YAML.
    2. table_name - Name of the table for which we are generating YAML.
    3. database_name - *Optional paramater* only needed for source and stage type YAMLs.
    4. schema_name - *Optional paramater* only needed for source and stage type YAMLs.
    5. source_system - *Optional paramater* Value of the source system or domain_name present in the stage table should be provided here. For example if we are Generating YML for "stg__ride__ann_glb_broker_dealer_dim" then source system value will be "ride".

1. Source:
    {{mc_generate_model_yaml(type = "source", table_name = "<my table>", database_name = "<my database>", schema_name = "<my schema>")}}
2. Stage:
    {{mc_generate_model_yaml(type = "stage", table_name = "<my table>", database_name = "<my database>", schema_name = "<my schema>", source_system="<my source>")}}
3. other:
    {{mc_generate_model_yaml(type = "other", table_name = "my int_table")}}
#}

{% macro mc_generate_model_yaml(type, table_name, source_system = none, database_name = none, schema_name = none) %}
    {% set model_lst = []%}
    {% do model_lst.append(table_name) %}
    {% if type == 'other'%}
        {{ log(codegen.generate_model_yaml( model_names = model_lst, upstream_descriptions = true), info = true) }}
        {% do return(codegen.generate_model_yaml( model_names = model_lst, upstream_descriptions = true)) %}
    {% endif %}
    {% if execute %}
        {% if type == 'source'%}
            {% if database_name == none or schema_name == none %} 
                {{ exceptions.raise_compiler_error("To generate source yml please provide database and schema name")}}
            {% else %}
                {% set sql %}
                    with "columns" as (
                    select '- name: ' || lower(column_name) || '\n            data_type: ' || lower(data_type) ||'\n            description: "'|| lower(COALESCE(REPLACE(REPLACE(comment,'"',''''),'\n',' '),'')) || '"'as column_statement, ordinal_position 
                    from {{ database_name }}.information_schema.columns
                    where table_schema = '{{ schema_name | upper }}' and table_name = '{{table_name | upper }}'
                    )select '   columns:' || listagg('\n          ' || column_statement) within group (order by ordinal_position asc)
                    from "columns"
                {% endset %}
                {%- call statement('generator', fetch_result = true) -%}
                {{ sql }}
                {%- endcall -%}
                {%- set states = load_result('generator') -%}
                {%- set states_data = states['data'] -%}
                {%- set states_status = states['response'] -%}
                {% set sources_yaml = [] %}
                {% do sources_yaml.append('version: 2') %}
                {% do sources_yaml.append('') %}
                {% do sources_yaml.append('sources:') %}
                {% do sources_yaml.append('  - name: ' ~ database_name | lower ~'__' ~ schema_name | lower ) %}
                {% do sources_yaml.append('    description: ""') %}
                {% do sources_yaml.append('    database: ' ~ database_name | lower) %}
                {% do sources_yaml.append('    schema: ' ~ schema_name | lower) %}
                {% do sources_yaml.append('    tables:') %}
                {% do sources_yaml.append('      - name: ' ~ table_name | lower ) %}
                {% do sources_yaml.append('        description: ""') %}
                {% do sources_yaml.append('     '~ states_data[0][0]) %}
                {% set joined = sources_yaml | join ('\n') %}
                {{ log(joined, info = true) }}
                {% do return(joined) %}
            {% endif %}
        {% elif type == 'stage'%}
            {% if database_name == none or schema_name == none %} 
                {{ exceptions.raise_compiler_error("To generate stage yml please provide  database and schema name")}}
            {% else %}
                {% set sql %}
                    with columns_disc as (
                        select '- name: ' || lower(column_name) || '\n        data_type: ' || lower(data_type) ||'\n        description: "'|| lower(COALESCE(REPLACE(REPLACE(comment,'"',''''),'\n',' '),'')) || '"'as column_statement, ordinal_position
                        from {{ database_name }}.information_schema.columns
                        where table_schema = '{{ schema_name | upper }}' and table_name = '{{table_name | upper }}'	
                    )select listagg('\n      ' || column_statement || '\n') within group (order by ordinal_position asc) as discription
                    from columns_disc
                {% endset %}
                {%- call statement('generator', fetch_result = true) -%}
                {{ sql }}
                {%- endcall -%}
                {%- set states = load_result('generator') -%}
                {%- set states_data = states['data'] -%}
                {%- set states_status = states['response'] -%}
                {% set stage_yaml = [] %}
                {% do stage_yaml.append('version: 2') %}
                {% do stage_yaml.append('') %}
                {% do stage_yaml.append('models:') %}
                {% do stage_yaml.append('  - name: stg__'~source_system~'__'~ table_name) %}
                {% do stage_yaml.append('    description: ""') %}
                {% do stage_yaml.append('    columns:' ~ states_data[0][0]) %}
                {% set joined = stage_yaml | join ('\n') %}
                {{ log(joined, info = true) }}
                {% do return(joined) %}
            {% endif %}
        {% endif %}
    {% endif %}                 
{% endmacro %}