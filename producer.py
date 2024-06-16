import json
import time
import pandas as pd
from kafka import KafkaProducer


def produce_data():
    """
    Function to produce data to a Kafka topic. This function reads a CSV file, converts each row to a dictionary,
    and sends it to the Kafka topic 'topic_url'. It pauses for 1 minute after every 1000 messages to simulate batch
    processing.
    """
    # Initialize Kafka producer with specified bootstrap server and value serializer
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    df = pd.read_csv('filtered_file_w_domain_10000.csv')

    # Iterate over DataFrame rows and send each row as a message to the Kafka topic
    for idx, row in df.iterrows():
        index = int(idx)
        producer.send('topic_url', row.to_dict())  # Convert row to dictionary and send to Kafka topic

        # Pause after every 1000 messages
        if (index + 1) % 1000 == 0:
            print(f"Produced {index + 1} messages. Sleeping for 1 minute.")
            time.sleep(60)  # Sleep for 1 minute

    # Ensure all buffered messages are sent to Kafka
    producer.flush()
    print("All messages produced.")


if __name__ == "__main__":
    produce_data()
