import pandas as pd
from sqlalchemy import create_engine

df = pd.read_csv("week_1/yellow_tripdata_2021-01.csv", nrows=1000)

print(df.head())

engine = create_engine("postgresql://root:root@localhost:5432/ny_taxi")

print(pd.io.sql.get_schema(df, name="yellow_trip_data", con=engine))