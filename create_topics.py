from confluent_kafka.admin import AdminClient, NewTopic


def create_topics(broker_, topics_):
    """Creates topics in a Kafka broker.

    Args:
        broker_ (str): The Kafka broker address.
        topics_ (list of str): List of topic names to create.
    """
    # Initialize the AdminClient with the broker address
    admin_client = AdminClient({'bootstrap.servers': broker_})

    # Define new topics with one partition and one replication factor
    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics_]

    fs = admin_client.create_topics(new_topics)

    # Wait for each topic creation operation to complete
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic {topic} created")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")


if __name__ == "__main__":
    # Kafka broker address
    broker = 'localhost:9092'

    topics = [f"cluster_{i}" for i in range(50)]

    create_topics(broker, topics)
