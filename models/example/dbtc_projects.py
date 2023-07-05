import requests
import pandas as pd

def model(dbt, session):
    dbt.config(
        materialized = "table",
        packages = ["requests", "pandas"]
    )

    dbt_cloud_api_key =  dbt.config.get("api_token")
    account_id = 64729

    
    projects_url = f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/projects/'
    headers = {
    'Authorization': f'Token {dbt_cloud_api_key}',
    'Content-Type': 'application/json'
    }
    response = requests.get(projects_url, headers=headers)
    projects = response.json()

    df = pd.DataFrame((projects['data']))

    # return final dataset (PySpark DataFrame)
    return df