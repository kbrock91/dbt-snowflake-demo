from dbtc import dbtCloudClient


def get_raw_sql(account_id, run_id, model_path):

    client = dbtCloudClient()

    artifacts = client.cloud.get_run_artifact(account_id, run_id, 'manifest.json')

    raw_sql = artifacts['nodes'][model_path]['raw_sql']

    return raw_sql


account_id = 64729

run_id = 88853895

model_path = 'model.my_snowflake_dbt_project.part_suppliers'

raw_sql = get_raw_sql(account_id, run_id, model_path)

print(raw_sql)
