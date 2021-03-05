import abc
import six

from st2common.runners.base_action import Action
from kafka.admin import KafkaAdminClient


@six.add_metaclass(abc.ABCMeta)
class BaseAdminAction(Action):
    DEFAULT_CLIENT_ID = "st2-kafka-admin"

    def admin_client(
            self,
            kafka_broker,
            kafka_broker_port,
            client_options=None,  # to support misc auth methods
    ):
        if not client_options:
            client_options = {}
        client = KafkaAdminClient(
            bootstrap_servers=["%s:%s" % (kafka_broker, kafka_broker_port)],
            client_id=self.config.get("client_id") or self.DEFAULT_CLIENT_ID,
            **client_options
        )
        return client


