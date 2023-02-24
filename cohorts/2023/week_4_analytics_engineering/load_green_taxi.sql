
-- Create external table with data
CREATE OR REPLACE EXTERNAL TABLE sacred-alloy-375819.trips_data_all.green_tripdata
OPTIONS (
  format = 'parquet',
  uris = ['gs://europe-datazoom-bucket/data/green/green_tripdata_2019-*.parquet.gz', 'gs://europe-datazoom-bucket/data/green/green_tripdata_2020-*.parquet.gz']
);


-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE sacred-alloy-375819.trips_data_all.green_tripdata_non_partitoned AS
SELECT * FROM sacred-alloy-375819.trips_data_all.green_tripdata;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE sacred-alloy-375819.trips_data_all.green_tripdata_partitoned
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS (
  SELECT * FROM sacred-alloy-375819.trips_data_all.green_tripdata
);

