import requests

session = requests.Session()

def get_joke():
  url = "https://official-joke-api.appspot.com/random_joke"
  response = session.get(url)
  return response.json()

def model(dbt, session):
    dbt.config(
        materialized = "table",
        packages = ["requests"], 
        external_access_integration = "joke_apis_access_integration"
    )

    my_first_df = dbt.ref("my_first_dbt_model")

    #df = orders_df.to_pandas_on_spark()  # Spark 3.2+
    df = my_first_df.toPandas() #in earlier versions

    # apply our function
    df["joke"] = get_joke()

    # convert back to PySpark
    #df = df.to_spark()               # Spark 3.2+
    df = session.createDataFrame(df) #in earlier versions

    # return final dataset (PySpark DataFrame)
    return df