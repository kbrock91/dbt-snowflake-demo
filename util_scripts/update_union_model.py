import os

# Define the path to the subdirectory and the file to be updated
subdirectory_path = 'models/example/input_models'
union_models_file_path = 'models/example/union_models.sql'

# List all SQL files in the subdirectory
sql_files = [f.replace('.sql', '') for f in os.listdir(subdirectory_path) if f.endswith('.sql')]

# Generate the new content for the model_names variable
model_names_content = ', '.join([f"ref('{model_name}')" for model_name in sql_files])

# Template for the new content of the union_models.sql file
new_content = f"""{{% set model_names = [{model_names_content}] -%}}

{{{{ dbt_utils.union_relations(relations=model_names) }}}}
"""

# Read the current content of the union_models.sql file (if it exists)
try:
    with open(union_models_file_path, 'r') as file:
        current_content = file.read()
except FileNotFoundError:
    current_content = ''

# Check if the new content is different from the current content
if new_content != current_content:
    # Write the new content to the union_models.sql file
    with open(union_models_file_path, 'w') as file:
        file.write(new_content)
    print("union_models.sql has been updated.")
else:
    print("No update needed for union_models.sql.")