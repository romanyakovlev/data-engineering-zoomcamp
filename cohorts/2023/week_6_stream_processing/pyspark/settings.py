import pyspark.sql.types as T

FHV_INPUT_DATA_PATH = '../../resources/fhv_tripdata_2019-01.csv.gz'
GREEN_INPUT_DATA_PATH = '../../resources/green_tripdata_2019-01.csv.gz'
BOOTSTRAP_SERVERS = 'localhost:9092'

TOPIC_WINDOWED_VENDOR_ID_COUNT = 'vendor_counts_windowed'

CONSUME_TOPIC_RIDES_CSV = 'rides_csv'
PRODUCE_TOPIC_FHV_RIDES_CSV = CONSUME_TOPIC_FHV_RIDES_CSV = 'fhv_rides_csv'
PRODUCE_TOPIC_GREEN_RIDES_CSV = CONSUME_TOPIC_GREEN_RIDES_CSV = 'green_rides_csv'

RIDE_SCHEMA = T.StructType(
    [T.StructField("vendor_id", T.IntegerType()),
     T.StructField('tpep_pickup_datetime', T.TimestampType()),
     T.StructField('tpep_dropoff_datetime', T.TimestampType()),
     T.StructField("passenger_count", T.IntegerType()),
     T.StructField("trip_distance", T.FloatType()),
     T.StructField("payment_type", T.IntegerType()),
     T.StructField("total_amount", T.FloatType()),
     ])

FHV_RIDE_SCHEMA = T.StructType(
    [T.StructField("PUlocationID", T.IntegerType()),
     T.StructField("DOlocationID", T.IntegerType()),
     T.StructField('pickup_datetime', T.TimestampType()),
     T.StructField('dropOff_datetime', T.TimestampType()),
     ])

GREEN_RIDE_SCHEMA = T.StructType(
    [T.StructField("PUlocationID", T.IntegerType()),
     T.StructField("DOlocationID", T.IntegerType()),
     T.StructField('pickup_datetime', T.TimestampType()),
     T.StructField('dropOff_datetime', T.TimestampType()),
     ])
