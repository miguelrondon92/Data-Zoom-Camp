#import pyarrow.csv as pv
#import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd 
from datetime import datetime

yellow_trip_2019_01_link= 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-10.parquet'

yellow_trip_2022_05 = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-05.parquet'

def yellow_schema():
    df=pd.read_parquet(yellow_trip_2019_01_link)
    #df['VendorID'] = df['VendorID'].astype(str)
    #df['airport_fee'] = df['airport_fee'].astype(float)
    print(df.info())

yellow_schema()

# yellow_taxi_schema= {
# 'VendorID': pa.string(),
# 'tpep_pickup_datetime': pa.timestamp('ns'),
# 'tpep_dropoff_datetime': pa.timestamp('ns'),
# 'passenger_count': pa.int64(),
# 'trip_distance': pa.float64(),
# 'RatecodeID': pa.string(),
# 'store_and_fwd_flag': pa.string(),
# 'PULocationID': pa.int64(),
# 'DOLocationID': pa.int64(),
# 'payment_type': pa.int64(),
# 'fare_amount': pa.float64(),
# 'extra': pa.float64(),
# 'mta_tax': pa.float64(),
# 'tip_amount': pa.float64(),
# 'tolls_amount': pa.float64(),
# 'improvement_surcharge': pa.float64(),
# 'total_amount': pa.float64(),
# 'congestion_surcharge': pa.float64(),
# 'airport_fee': pa.float64(),
# }



# def get_yellow_schema(parquet_file): 
#     df = pd.read_parquet(parquet_file)
#     fields = [pa.field(x, y) for x, y in yellow_taxi_schema.items()]
#     new_schema = pa.schema(fields)
#     pa_table = pa.Table.from_pandas(
#         df = df,
#         schema = new_schema,
#         preserve_index= False
#     )
#     #print(pa_table.schema.get_all_field_indices('airport_fee'))
#     #return(pa_table.schema)
#     print(pa_table.schema)

# yellow_schema = get_yellow_schema(yellow_trip_2019_01_link)



# def transform_schema(parquet_file, schema):
#     """
#     give implicit schema to parquet files 
#     """
#     df= pd.read_parquet(parquet_file)
#     pq_file = pa.Table.from_pandas(
#         df = df,
#         schema = schema 
#     )
#     print("This is the file:")
#     print(pq_file)


# #transform_schema(yellow_trip_2022_05, yellow_schema)

# transform_schema(yellow_trip_2022_05, yellow_schema)
