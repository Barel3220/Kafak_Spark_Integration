from confluent_kafka import Consumer, KafkaException, KafkaError
import time
import json


def create_consumer(group_id, topics_):
    consumer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }
    consumer_ = Consumer(consumer_conf)
    consumer_.subscribe(topics_)
    return consumer_


def load_counts(output_file):
    try:
        with open(output_file, 'r') as f:
            data = json.load(f)
        return data['counts'], data['clusters']
    except FileNotFoundError:
        return [0] * 50, [str(i) for i in range(1, 51)]


def save_counts(counts, clusters, output_file):
    with open(output_file, 'w') as f:
        json.dump({'counts': counts, 'clusters': clusters}, f)


def consume_and_count(consumer_, topics_, interval=1, output_file='message_counts.json'):
    counts, clusters = load_counts(output_file)
    start_time = time.time()
    try:
        while True:
            msg = consumer_.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            topic_index = topics_.index(msg.topic())
            counts[topic_index] += 1
            current_time = time.time()
            if current_time - start_time >= interval:
                save_counts(counts, clusters, output_file)
                start_time = current_time
    except KeyboardInterrupt:
        pass
    finally:
        consumer_.close()


if __name__ == "__main__":
    topics = [f"cluster_{i}" for i in range(50)]
    consumer = create_consumer('count_group', topics)
    consume_and_count(consumer, topics)
