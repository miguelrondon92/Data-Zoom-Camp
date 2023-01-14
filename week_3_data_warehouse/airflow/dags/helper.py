#import pyarrow.csv as pv
#import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd 
import pyarrow.parquet as pq 
from datetime import datetime

yellow_trip_2019_01_link= 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-10.parquet'
yellow_trip_2022_05 = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-05.parquet'

yt_2019_01_file= '~/downloads/yellow_tripdata_2019-01.parquet'
yt_2022_06_file= '~/downloads/yellow_tripdata_2022-06.parquet'
yt_2022_05_file= '~/downloads/yellow_tripdata_2022-05.parquet'
gr_2019_01_file= '~/downloads/green_tripdata_2019-01.parquet'
fhv_2019_01_file= '~/downloads/fhv_tripdata_2019-01.parquet'

yellow_taxi_schema= {
    'VendorID': pa.string(),
    'tpep_pickup_datetime': pa.timestamp('ns'),
    'tpep_dropoff_datetime': pa.timestamp('ns'),
    'passenger_count': pa.int64(),
    'trip_distance': pa.float64(),
    'RatecodeID': pa.string(),
    'store_and_fwd_flag': pa.string(),
    'PULocationID': pa.string(),
    'DOLocationID': pa.string(),
    'payment_type': pa.string(),
    'fare_amount': pa.float64(),
    'extra': pa.float64(),
    'mta_tax': pa.float64(),
    'tip_amount': pa.float64(),
    'tolls_amount': pa.float64(),
    'improvement_surcharge': pa.float64(),
    'total_amount': pa.float64(),
    'congestion_surcharge': pa.float64(),
    'airport_fee': pa.float64(),
    }

yellow_schema = [pa.field(x, y) for x, y in yellow_taxi_schema.items()]


green_taxi_schema = {
    'VendorID': pa.string(),
    'lpep_pickup_datetime': pa.timestamp('ns'),
    'lpep_dropoff_datetime': pa.timestamp('ns'),
    'store_and_fwd_flag': pa.string(),
    'RatecodeID': pa.string(),
    'PULocationID': pa.string(),				
    'DOLocationID': pa.string(),	
    'passenger_count': pa.int64(), 
    'trip_distance': pa.float64(),
    'fare_amount': pa.float64(),
    'extra': pa.float64(), 
    'mta_tax': pa.float64(),	
    'tip_amount': pa.float64(),		
    'tolls_amount': pa.float64(),		
    'ehail_fee': pa.float64(),				
    'improvement_surcharge': pa.float64(), 
    'total_amount': pa.float64(), 
    'payment_type': pa.string(), 
    'trip_type': pa.string(), 
    'congestion_surcharge': pa.float64()
}

green_schema = [pa.field(x, y) for x, y in green_taxi_schema.items()]

fhv_taxi_schema = {
    'dispatching_base_num': pa.string(),
    'pickup_datetime': pa.timestamp('ns'),
    'dropOff_datetime': pa.timestamp('ns'), 
    'PUlocationID': pa.string(),
    'DOlocationID': pa.string(),
    'SR_Flag': pa.string(),
    'Affiliated_base_number': pa.string(),
}

fhv_schema = [pa.field(x, y) for x, y in fhv_taxi_schema.items()]




