# create docker network

docker network create pg-network

# build images

docker-compose build
docker build -t trips_ingest:v001 -f trips.Dockerfile .
docker build -t zones_ingest:v001 -f zones.Dockerfile .

# run infrastructure

docker-compose up -d

# run data pipeline for trips

URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"

docker run -it \
  --network=pg-network \
  trips_ingest:v001 \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=taxi_trips \
    --url=${URL}

# run data pipeline for zones

URL="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

docker run -it \
  --network=pg-network \
  zones_ingest:v001 \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_zones \
    --url=${URL}
