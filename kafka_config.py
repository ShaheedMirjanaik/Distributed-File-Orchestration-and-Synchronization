from kafka import KafkaProducer, KafkaConsumer
from json import dumps, loads

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
FILE_TRANSFER_TOPIC = 'file_transfers'
AUTH_TOPIC = 'authentication'

def create_producer():
    """
    Create a Kafka producer instance with specified configurations.

    Returns:
        KafkaProducer: A configured Kafka producer instance.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,  # List of Kafka bootstrap servers
        value_serializer=lambda x: dumps(x).encode('utf-8')  # Serialize data to JSON and then encode to UTF-8
    )

def create_consumer(group_id):
    """
    Create a Kafka consumer instance with specified configurations.

    Args:
        group_id (str): The consumer group ID.

    Returns:
        KafkaConsumer: A configured Kafka consumer instance.
    """
    return KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,  # List of Kafka bootstrap servers
        auto_offset_reset='earliest',  # Fetch from the beginning of the topic
        enable_auto_commit=True,  # Automatically commit the consumed messages
        group_id=group_id,  # Consumer group ID
        value_deserializer=lambda x: loads(x.decode('utf-8'))  # Deserialize JSON data from UTF-8 encoded bytes
    )
