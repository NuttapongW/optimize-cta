"""Producer base-class providing common utilities and functionality"""
import logging
import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

from utils.url import KAFKA_URL, SCHEMA_REGISTRY_URL

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "bootstrap.servers": KAFKA_URL
        }

        self.client: AdminClient = AdminClient(
            self.broker_properties
        )

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        schema_registry = CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL)
        self.producer = AvroProducer(
            self.broker_properties,
            schema_registry=schema_registry
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        existing_topics = self.client.list_topics().topics
        if self.topic_name in existing_topics:
            logger.info(f"The specified topic ({self.topic_name}) is already existed")
        else:
            self.client.create_topics([NewTopic(self.topic_name, self.num_partitions)])
            logger.info(f"creating a new topic, {self.topic_name}")
            Producer.existing_topics = {*Producer.existing_topics, self.topic_name}
            logger.info(f"add topic, {self.topic_name} to existing_topics")

    @staticmethod
    def time_millis():
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()
