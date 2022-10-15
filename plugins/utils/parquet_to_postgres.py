import pandas as pd
from datetime import datetime
import sqlalchemy
from sqlalchemy.engine import Engine
from sqlalchemy.types import Integer, String, Float, DateTime
from pandas import DataFrame

def fetch_and_load(**kwargs):
    base_path = kwargs['base_path']
    url = kwargs['url']
    engine = sqlalchemy.create_engine(url)

    execution_date: str = kwargs['execution_date']
    date_to_fetch = datetime.strptime(
        execution_date, "%Y-%m-%d").strftime("%Y-%m")
    print(f"Fetching NYC taxi trips data at: {date_to_fetch}")

    # Extract data source -- read data
    data_path_without_ext = f"{base_path}/data/{date_to_fetch}/yellow_tripdata_{date_to_fetch}"
    df = pd.read_parquet(
        f"{data_path_without_ext}.parquet")
    
    print(f"Initial dataframe schema: \n {df.info()}")

    # Convert to datetime so that it can be used in filtering
    start_date = datetime.strptime(kwargs['start_date'], "%Y-%m-%d %H:%M:%S")
    end_date = datetime.strptime(kwargs['end_date'], "%Y-%m-%d %H:%M:%S")
        
    # Rename the column
    df = df.rename(columns={
        "VendorID": "vendor_id",
        "RatecodeID": "rate_code_id",
        "Store_and_fwd_flag": "store_and_fwd_flag",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id",
    })

    # Filter based on start_date (YYYY-MM-DD HH:mm:ss) and end_date (YYYY-MM-DD HH:mm:ss)
    df_filter = df[(df['tpep_pickup_datetime'] >= start_date) & (df['tpep_pickup_datetime'] < end_date)]

    # Add column -- trip period
    df_filter['trip_period'] = date_to_fetch

    # Reorder column
    columns = [
        "vendor_id",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_period",
        "passenger_count",
        "trip_distance",
        "rate_code_id",
        "store_and_fwd_flag",
        "pu_location_id",
        "do_location_id",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee"
    ]
    df_filter = df_filter[columns]

    # Load to data target
    schema = 'staging'
    stg_table_name = 'raw_trip_data'
    print(f"Load the data to `{schema}.{stg_table_name}`...")
    load_to_postgres(df_filter, engine, schema, stg_table_name)

def load_to_postgres(df: DataFrame, engine: Engine, schema: str, table_name: str):
    with engine.connect().execution_options(autocommit=True) as conn:
        df.to_sql(table_name, conn, if_exists='replace', index=False,
                schema=schema, chunksize=500,
                dtype={
                    "vendor_id": Integer(),
                    "tpep_pickup_datetime": DateTime(),
                    "tpep_dropoff_datetime": DateTime(),
                    "trip_period": String(),
                    "passenger_count": Integer(),
                    "trip_distance": Float(),
                    "rate_code_id": Integer(),
                    "store_and_fwd_flag": String(),
                    "pu_location_id": Integer(),
                    "do_location_id": Integer(),
                    "payment_type": Integer(),
                    "fare_amount": Integer(),
                    "extra": Float(),
                    "mta_tax": Float(),
                    "tip_amount": Float(),
                    "tolls_amount": Float(),
                    "improvement_surcharge": Float(),
                    "total_amount": Float(),
                    "congestion_surcharge": Float(),
                    "airport_fee": Float()
                })