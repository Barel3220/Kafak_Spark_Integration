from kafka import KafkaProducer
import json
import pandas as pd
import time


def produce_data():
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    df = pd.read_csv('sample_campaign_data_2023.csv')
    for idx, row in df.iterrows():
        index = int(idx)
        producer.send('topic_url', row.to_dict())
        if (index + 1) % 1000 == 0:
            print(f"Produced {index + 1} messages. Sleeping for 1 minute.")
            time.sleep(60)  # Sleep for 1 minute
    producer.flush()
    print("All messages produced.")


if __name__ == "__main__":
    produce_data()
