import six

from st2common.runners.base_action import Action
from kafka import KafkaProducer
from kafka.producer.future import RecordMetadata


# copied from kafka-python <1.0
def kafka_bytestring(s):
    """
    Takes a string or bytes instance
    Returns bytes, encoding strings in utf-8 as necessary
    """
    if isinstance(s, six.binary_type):
        return s
    if isinstance(s, six.string_types):
        return s.encode('utf-8')
    raise TypeError(s)


class ProduceMessageAction(Action):
    """
    Action to send messages to Apache Kafka system.
    """
    DEFAULT_CLIENT_ID = 'st2-kafka-producer'

    def run(self, topic, message, hosts=None):
        """
        Simple round-robin synchronous producer to send one message to one topic.

        :param hosts: Kafka hostname(s) to connect in host:port format.
                      Comma-separated for several hosts.
        :type hosts: ``str``
        :param topic: Kafka Topic to publish the message on.
        :type topic: ``str``
        :param message: The message to publish.
        :type message: ``str``

        :returns: Response data: `topic`, target `partition` where message was sent,
                  `offset` number and `error` code (hopefully 0).
        :rtype: ``dict``
        """

        if hosts:
            _hosts = hosts
        elif self.config.get('hosts', None):
            _hosts = self.config['hosts']
        else:
            raise ValueError("Need to define 'hosts' in either action or in config")

        # set default for empty value
        _client_id = self.config.get('client_id') or self.DEFAULT_CLIENT_ID

        producer = KafkaProducer(
            bootstrap_servers=_hosts.split(','),
            client_id=_client_id,
            # TODO: Support security_protocol + sasl_mechanism + sasl_*
        )
        future = producer.send(topic=topic, value=kafka_bytestring(message))
        producer.flush()
        if not future.failed():
            result = future.get()  # type: RecordMetadata
            return result._asdict()
