import fastparquet
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData

# Load the parquet file into a DataFrame
df = pd.read_parquet('dataset/yellow_tripdata_2023-01.parquet')
print(df)


# Drop rows with missing values
df = df.dropna()

# Remove duplicates
df = df.drop_duplicates()

# Filter out rows with invalid data, e.g., negative trip distance
df = df[df['trip_distance'] > 0]



from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData

metadata = MetaData()

yellow_tripdata_table = Table('yellow_tripdata', metadata,
                              Column('VendorID', Integer),
                              Column('tpep_pickup_datetime', String),
                              Column('tpep_dropoff_datetime', String),
                              Column('passenger_count', Integer),
                              Column('trip_distance', Float),
                              Column('RatecodeID', Integer),
                              Column('store_and_fwd_flag', String),
                              Column('PULocationID', Integer),
                              Column('DOLocationID', Integer),
                              Column('payment_type', Integer),
                              Column('fare_amount', Float),
                              Column('extra', Float),
                              Column('mta_tax', Float),
                              Column('tip_amount', Float),
                              Column('tolls_amount', Float),
                              Column('improvement_surcharge', Float),
                              Column('total_amount', Float))


from sqlalchemy import create_engine

# Define the connection string
db_url = 'postgresql://username:password@hostname:port/database'
engine = create_engine(db_url)

# Ingest the DataFrame into the PostgreSQL table
df.to_sql('yellow_tripdata', engine, if_exists='replace', index=False, dtype={
    'VendorID': Integer,
    'tpep_pickup_datetime': String,
    'tpep_dropoff_datetime': String,
    'passenger_count': Integer,
    'trip_distance': Float,
    'RatecodeID': Integer,
    'store_and_fwd_flag': String,
    'PULocationID': Integer,
    'DOLocationID': Integer,
    'payment_type': Integer,
    'fare_amount': Float,
    'extra': Float,
    'mta_tax': Float,
    'tip_amount': Float,
    'tolls_amount': Float,
    'improvement_surcharge': Float,
    'total_amount': Float
})


with engine.connect() as connection:
    result = connection.execute('SELECT COUNT(*) FROM yellow_tripdata')
    row_count = result.fetchone()[0]
    print(f'Number of rows ingested: {row_count}')
