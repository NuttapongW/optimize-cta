"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests

from utils import url

logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = f"{url.KAFKA_CONNECT_URL}/connectors"
CONNECTOR_NAME = "stations"

JDBC_USER = "cta_admin"
JDBC_PASS = "chicago"


def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    resp = requests.post(
       KAFKA_CONNECT_URL,
       headers={"Content-Type": "application/json"},
       data=json.dumps({
           "name": CONNECTOR_NAME,
           "config": {
               "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
               "key.converter": "org.apache.kafka.connect.json.JsonConverter",
               "key.converter.schemas.enable": "false",
               "value.converter": "org.apache.kafka.connect.json.JsonConverter",
               "value.converter.schemas.enable": "false",
               "batch.max.rows": "500",
               "connection.url": url.POSTGRESQL_URL,
               "connection.user": JDBC_USER,
               "connection.password": JDBC_PASS,
               "table.whitelist": "stations",
               "mode": "incrementing",
               "incrementing.column.name": "stop_id",
               "topic.prefix": "jdbc_",
               "poll.interval.ms": "1000",
           }
       }),
    )

    # Ensure a healthy response was given
    resp.raise_for_status()
    logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()
