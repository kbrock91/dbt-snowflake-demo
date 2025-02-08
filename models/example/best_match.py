
import pandas as pd
import Levenshtein

# Sample datasets


# Function to calculate distance
def find_best_match(word, candidates):
    distances = [(candidate, Levenshtein.distance(word, candidate)) for candidate in candidates]
    return min(distances, key=lambda x: x[1])  # Get closest match



def model(dbt, session):
    dbt.config(
        materialized = "table",
        packages = ["python-Levenshtein"]
    )

    df_2017 =  dbt.ref("inv_2017")
    df_2025 = dbt.ref("inv_2025")
    #df = orders_df.to_pandas_on_spark()  # Spark 3.2+
    df_2017 = df_2017.toPandas() #in earlier versions
    df_2025 = df_2025.toPandas()
    # Apply function across datasets
    df_2017['best_match'] = df_2017['WINE_DESCRIPTION'].apply(lambda x: find_best_match(x, df_2025['WINE_DESCRIPTION'].tolist())[0])
    df_2017['distance'] = df_2017['WINE_DESCRIPTION'].apply(lambda x: find_best_match(x, df_2025['WINE_DESCRIPTION'].tolist())[1])

    # apply our function
   # df["is_holiday"] = df["ORDER_DATE"].apply(is_holiday)

    # convert back to PySpark
    #df = df.to_spark()               # Spark 3.2+
    df_2017 = session.createDataFrame(df_2017) #in earlier versions

    # return final dataset (PySpark DataFrame)
    return df_2017