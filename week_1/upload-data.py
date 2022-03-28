import pandas as pd
from sqlalchemy import create_engine

df_100 = pd.read_csv("week_1/yellow_tripdata_2021-01.csv", nrows=100)

df_100.tpep_pickup_datetime = pd.to_datetime(df_100.tpep_pickup_datetime)
df_100.tpep_dropoff_datetime = pd.to_datetime(df_100.tpep_dropoff_datetime)

df_iter = pd.read_csv("week_1/yellow_tripdata_2021-01.csv", iterator=True, chunksize=100000)

engine = create_engine("postgresql://root:root@localhost:5432/ny_taxi")

df_100.head(0).to_sql(name="yellow_taxi_data", con=engine, if_exists="replace")

for chunk in df_iter:
    chunk.tpep_pickup_datetime = pd.to_datetime(chunk.tpep_pickup_datetime)
    chunk.tpep_dropoff_datetime = pd.to_datetime(chunk.tpep_dropoff_datetime)
    chunk.to_sql(name="yellow_taxi_data", con=engine, if_exists="append")
    print("inserted another chunk")


# print(pd.io.sql.get_schema(df_100, name="yellow_trip_data", con=engine))

# CREATE TABLE yellow_trip_data (
#         "VendorID" BIGINT,
#         tpep_pickup_datetime TEXT,
#         tpep_dropoff_datetime TEXT,
#         passenger_count BIGINT,
#         trip_distance FLOAT(53),
#         "RatecodeID" BIGINT,
#         store_and_fwd_flag TEXT,
#         "PULocationID" BIGINT,
#         "DOLocationID" BIGINT,
#         payment_type BIGINT,
#         fare_amount FLOAT(53),
#         extra FLOAT(53),
#         mta_tax FLOAT(53),
#         tip_amount FLOAT(53),
#         tolls_amount FLOAT(53),
#         improvement_surcharge FLOAT(53),
#         total_amount FLOAT(53),
#         congestion_surcharge FLOAT(53)
# )

