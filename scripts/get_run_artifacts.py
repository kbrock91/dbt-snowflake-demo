from dbtc import dbtCloudClient

client = dbtCloudClient()

artifacts = client.cloud.get_run_artifact(64729, 88853895,'manifest.json')

raw_sql = artifacts['nodes']['model.my_snowflake_dbt_project.part_suppliers']['raw_sql']

print(raw_sql)