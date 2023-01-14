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


yellow_taxi_schema= {
    'VendorID': pa.string(),
    'tpep_pickup_datetime': pa.timestamp('ns'),
    'tpep_dropoff_datetime': pa.timestamp('ns'),
    'passenger_count': pa.int64(),
    'trip_distance': pa.float64(),
    'RatecodeID': pa.string(),
    'store_and_fwd_flag': pa.string(),
    'PULocationID': pa.int64(),
    'DOLocationID': pa.int64(),
    'payment_type': pa.int64(),
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


geen_taxi_schema = {

}


fhv_taxi_schema = {

}


def test_schema(file, taxi):
    table = pq.read_table(file) 
    match taxi:
        case "yellow":
            fields = [pa.field(x, y) for x, y in yellow_taxi_schema.items()]
        case "green":
            fields = [pa.field(x, y) for x, y in green_taxi_schema.items()]
        case "fhv": 
            fields = [pa.field(x, y) for x, y in fhv_taxi_schema.items()]
    new_schema = pa.schema(fields)
    table = table.cast(new_schema)
    print(table)
    #pa.parquet.write_table(table, '~/downloads/modified_file.parquet')


test_schema(yt_2019_01_file)
test_schema(yt_2022_06_file)
test_schema(yt_2022_05_file)

