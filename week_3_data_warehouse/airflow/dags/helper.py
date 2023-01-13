#import pyarrow.csv as pv
#import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd 
from datetime import datetime


yellow_trip_2019_01_link = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-01.parquet'

yellow_trip_2022_05 = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-05.parquet'
def get_schema(parquet_file): 
    df = pd.read_parquet(parquet_file)
    pa_table = pa.Table.from_pandas(
        df = df
    )
    return(pa_table.schema)
    # print(pa_table.schema)

yellow_schema = get_schema(yellow_trip_2019_01_link)



def transform_schema(parquet_file, schema):
    """
    give implicit schema to parquet files 
    """
    df= pd.read_parquet(parquet_file)
    pq_file = pa.Table.from_pandas(
        df = df,
        schema = schema 
    )
    print("This is the file:")
    print(pq_file)


transform_schema(yellow_trip_2022_05, yellow_schema)