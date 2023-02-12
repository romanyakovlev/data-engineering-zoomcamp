FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_zones_data.py ingest_zones_data.py

ENTRYPOINT [ "python", "ingest_zones_data.py" ]