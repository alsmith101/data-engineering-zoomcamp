import pandas as pd
from sqlalchemy import create_engine
import argparse
import os


def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.db
    table_name = params.table_name
    url = params.url
    csv_output = "output.csv"

    print(f"downloading file to {csv_output}")
    os.system(f"wget {url} -O {csv_output}")
    print("download completed")

    df_100 = pd.read_csv(csv_output, nrows=100)

    df_100.tpep_pickup_datetime = pd.to_datetime(df_100.tpep_pickup_datetime)
    df_100.tpep_dropoff_datetime = pd.to_datetime(df_100.tpep_dropoff_datetime)

    df_iter = pd.read_csv(csv_output, iterator=True, chunksize=100000)

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

    df_100.head(0).to_sql(name=table_name, con=engine, if_exists="replace")

    for chunk in df_iter:
        chunk.tpep_pickup_datetime = pd.to_datetime(chunk.tpep_pickup_datetime)
        chunk.tpep_dropoff_datetime = pd.to_datetime(chunk.tpep_dropoff_datetime)
        chunk.to_sql(name=table_name, con=engine, if_exists="append")
        print("inserted another chunk")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Loading CSV data to Postgres")

    parser.add_argument('--user', help='user_name for postgres db')
    parser.add_argument('--password', help='password for postgres db')
    parser.add_argument('--host', help='hostname for postgres db')
    parser.add_argument('--port', help='port for postgres db')
    parser.add_argument('--db', help='database name for postgres db')
    parser.add_argument('--table_name', help='table name where file is written to')
    parser.add_argument('--url', help="path to csv file")

    args = parser.parse_args()

    main(args)


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

