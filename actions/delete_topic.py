from typing import Dict, Optional
from kafka.protocol.admin import Response
from lib.base_admin_action import BaseAdminAction
from kafka.errors import UnknownTopicOrPartitionError

class CreateTopicAction(BaseAdminAction):
    def run(
        self,
        kafka_broker,
        kafka_broker_port,
        topic,
        client_options=None,  # to support misc auth methods
    ):
        client = self.admin_client(kafka_broker, kafka_broker_port, client_options)
        try:
            response = client.delete_topics([topic])  # type: Response
            return "Topic %s deleted" % topic
        except UnknownTopicOrPartitionError:
            return "Unknown Topic %s" % topic


if __name__ == "__main__":
    import os

    KAFKA_BROKER = os.environ.get("KAFKA_BROKER")
    KAFKA_BROKER_PORT = os.environ.get("KAFKA_BROKER_PORT")
    KAFKA_USERNAME = os.environ.get("KAFKA_USERNAME")
    KAFKA_PASSWORD = os.environ.get("KAFKA_PASSWORD")
    TOPIC = os.environ.get("TOPIC", "test")

    CLIENT_OPTIONS = {
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "SCRAM-SHA-256",
        "sasl_plain_username": KAFKA_USERNAME,
        "sasl_plain_password": KAFKA_PASSWORD,
    }

    action = CreateTopicAction()

    res = action.run(
        kafka_broker=KAFKA_BROKER,
        kafka_broker_port=KAFKA_BROKER_PORT,
        topic=TOPIC,
        client_options=CLIENT_OPTIONS,
    )
    import pprint

    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(res)
