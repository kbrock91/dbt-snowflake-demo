 {% macro list_tags(prefix) %}

    -- Loop through nodes in graph object
    {% for node in graph['nodes'] %}
        
        -- Check if node has any tags
        {% set resource = graph['nodes'][node] %}
        {% if resource.resource_type == 'model' and resource.config.tags | length > 0 %}
            {{ log(resource.config.tags, info=True) }}
            {{ log(resource.depends_on.nodes, info=True) }}
                    {% endif %}

            {% endfor %}
        
    
    
{% endmacro %}