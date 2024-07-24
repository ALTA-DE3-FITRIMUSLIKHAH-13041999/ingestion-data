import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger, String, DateTime, Boolean, Float, Integer

pd.set_option('display.max_columns', None)

# create database from parquet
def read_parquet():
    df = pd.read_parquet("dataset/yellow_tripdata_2023-01.parquet", engine='fastparquet')

    return df

# 3) Clean the Yellow Trip dataset
def cleaning(df):
    df.dropna(inplace=True)

    #cast data type
    df['VendorID'] = df['VendorID'].astype('int8')
    df['passenger_count'] = df['passenger_count'].astype('int8')
    df['PULocationID'] = df['PULocationID'].astype('int8')
    df['DOLocationID'] = df['DOLocationID'].astype('int8')
    df['payment_type'] = df['payment_type'].astype('int8')

    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].replace(['N', 'Y'], [False, True])
    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].astype('boolean')

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    #Rename columns with snake_case format
    df = df.rename(columns={'VendorID': 'vendor_id', 'RatecodeID': 'rate_code_id', 'PULocationID': 'pu_location_id', 'DOLocationID': 'do_location_id'})
    
    return df

def connect_database():
    user = 'postgres'
    password = 'admin'
    host = 'localhost'
    database = 'mydb'
    port = 5433
    
    conn_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(conn_string)

    return engine