
-- Create external table with data
CREATE OR REPLACE EXTERNAL TABLE sacred-alloy-375819.nytaxi.fhv_tripdata
OPTIONS (
  format = 'CSV',
  uris = ['gs://prefect-de-zoomcamp-ry/data/fhv/fhv_tripdata_2019-*.csv.gz']
)

-- Get rows count for fhv vehicle records
SELECT count(*) FROM sacred-alloy-375819.nytaxi.fhv_tripdata;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE sacred-alloy-375819.nytaxi.fhv_tripdata_non_partitoned AS
SELECT * FROM sacred-alloy-375819.nytaxi.fhv_tripdata;

-- count the distinct number of affiliated_base_number for external table
SELECT COUNT(DISTINCT(affiliated_base_number)) FROM sacred-alloy-375819.nytaxi.fhv_tripdata;

-- count the distinct number of affiliated_base_number for table
SELECT COUNT(DISTINCT(affiliated_base_number)) FROM sacred-alloy-375819.nytaxi.fhv_tripdata_non_partitoned;

-- count how many records have both a blank (null) PUlocationID and DOlocationID
SELECT COUNT(*) FROM sacred-alloy-375819.nytaxi.fhv_tripdata_non_partitoned WHERE PUlocationID IS NULL and DOlocationID IS NULL;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE sacred-alloy-375819.nytaxi.fhv_tripdata_partitoned
PARTITION BY DATE(pickup_datetime) AS
SELECT * FROM sacred-alloy-375819.nytaxi.fhv_tripdata;

-- This query will process 23.05 MB when run
SELECT COUNT(DISTINCT(affiliated_base_number)) FROM sacred-alloy-375819.nytaxi.fhv_tripdata_partitoned
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';

-- This query will process 647.87 MB when run
SELECT COUNT(DISTINCT(affiliated_base_number)) FROM sacred-alloy-375819.nytaxi.fhv_tripdata_non_partitoned
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';