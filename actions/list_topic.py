from kafka.protocol.admin import Response

from lib.base_admin_action import BaseAdminAction


class CreateTopicAction(BaseAdminAction):
    def run(
        self,
        kafka_broker,
        kafka_broker_port,
        client_options=None,  # to support misc auth methods
    ):
        client = self.admin_client(kafka_broker, kafka_broker_port, client_options)
        response = client.list_topics()  # type: Response
        return response


if __name__ == "__main__":
    import os
    KAFKA_BROKER = os.environ.get("KAFKA_BROKER")
    KAFKA_BROKER_PORT = os.environ.get("KAFKA_BROKER_PORT")
    KAFKA_USERNAME = os.environ.get("KAFKA_USERNAME")
    KAFKA_PASSWORD = os.environ.get("KAFKA_PASSWORD")

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
        client_options=CLIENT_OPTIONS,
    )
    import pprint

    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(res)
