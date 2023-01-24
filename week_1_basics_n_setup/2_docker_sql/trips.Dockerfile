FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_trips_data.py ingest_trips_data.py

ENTRYPOINT [ "python", "ingest_trips_data.py" ]