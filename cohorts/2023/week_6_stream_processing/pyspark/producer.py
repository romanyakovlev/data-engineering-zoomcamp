import csv
import gzip
from time import sleep
from typing import Dict
from kafka import KafkaProducer

from settings import (
    BOOTSTRAP_SERVERS,
    FHV_INPUT_DATA_PATH,
    GREEN_INPUT_DATA_PATH,
    PRODUCE_TOPIC_FHV_RIDES_CSV,
    PRODUCE_TOPIC_GREEN_RIDES_CSV,
)


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


class RideCSVProducer:
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)
        # self.producer = Producer(producer_props)

    @staticmethod
    def read_records(resource_path: str, indexes: list):
        records, ride_keys = [], []
        i = 0
        with gzip.open(resource_path, mode='rt') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            for row in reader:
                # vendor_id, passenger_count, trip_distance, payment_type, total_amount
                if row[indexes[0]] and row[indexes[1]]:
                    records.append(", ".join([row[i] for i in indexes]))
                    ride_keys.append(str(row[indexes[0]]))
                    i += 1
                    if i == 5:
                        break
        return zip(ride_keys, records)

    def publish(self, topic: str, records: [str, str]):
        for key_value in records:
            key, value = key_value
            try:
                self.producer.send(topic=topic, key=key, value=value)
                print(f"Producing record for <key: {key}, value:{value}>")
                break
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")

        self.producer.flush()
        sleep(1)


if __name__ == "__main__":
    config = {
        'bootstrap_servers': [BOOTSTRAP_SERVERS],
        'key_serializer': lambda x: x.encode('utf-8'),
        'value_serializer': lambda x: x.encode('utf-8')
    }
    producer = RideCSVProducer(props=config)
    # fhv data
    fhv_ride_records = producer.read_records(resource_path=FHV_INPUT_DATA_PATH, indexes=[3, 4, 1, 2])
    producer.publish(topic=PRODUCE_TOPIC_FHV_RIDES_CSV, records=fhv_ride_records)
    # green data
    green_ride_records = producer.read_records(resource_path=GREEN_INPUT_DATA_PATH, indexes=[5, 6, 1, 2])
    producer.publish(topic=PRODUCE_TOPIC_GREEN_RIDES_CSV, records=green_ride_records)
