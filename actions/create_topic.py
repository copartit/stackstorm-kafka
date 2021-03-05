from typing import Dict, Optional

from kafka.admin import NewTopic
from kafka.protocol.admin import Response

from lib.base_admin_action import BaseAdminAction


class CreateTopicAction(BaseAdminAction):
    def run(
        self,
        kafka_broker,
        kafka_broker_port,
        topic,
        partitions,
        replication_factor,
        replica_assignment=None,  # type: Optional[Dict[str, int]]
        topic_config=None,
        client_options=None,  # to support misc auth methods
    ):
        if replica_assignment:
            replica_assignment = {int(k): int(v) for k, v in replica_assignment.items()}
        client = self.admin_client(kafka_broker, kafka_broker_port, client_options)

        new_topic = NewTopic(
            topic,
            num_partitions=partitions,
            replication_factor=replication_factor,
            replica_assignments=replica_assignment,
            topic_configs=topic_config,
        )

        response = client.create_topics([new_topic])  # type: Response
        return response.to_object()


if __name__ == "__main__":
    import os

    KAFKA_BROKER = os.environ.get("KAFKA_BROKER")
    KAFKA_BROKER_PORT = os.environ.get("KAFKA_BROKER_PORT", 9092)
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
        partitions=1,
        replication_factor=1,
        replica_assignment=None,
        topic_config=None,
        client_options=CLIENT_OPTIONS,
    )
    import pprint

    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(res)
