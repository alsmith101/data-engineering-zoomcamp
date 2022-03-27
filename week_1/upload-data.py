import pandas as pd


df = pd.read_csv("yellow_tripdata_2021-01.csv", nrows=1000)

print(df.head())

print(pd.io.sql.get_schema(df, name="yellow_trip_data"))