# How to run

```sh
python3 producer.py
python3 consumer.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.kafka:kafka-clients:2.8.1 ./streaming.py
```
