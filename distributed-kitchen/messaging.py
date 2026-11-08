import json
import os

import pika

RETRY_DELAY_MS = 2000
WORK_QUEUES = ('pizza.prepare', 'pizza.bake', 'pizza.pack')
QUEUES = (*WORK_QUEUES, 'pizza.events', 'pizza.dead', 'pizza.retry.prepare', 'pizza.retry.bake', 'pizza.retry.pack')


def connect():
    parameters = pika.URLParameters(os.getenv('AMQP_URL', 'amqp://kitchen:kitchen@localhost:5672/%2F'))
    parameters.heartbeat = 30
    parameters.socket_timeout = 3
    parameters.stack_timeout = 5
    parameters.blocked_connection_timeout = 5
    parameters.connection_attempts = 1
    return pika.BlockingConnection(parameters)


def setup(connection):
    channel = connection.channel()
    for queue in QUEUES:
        arguments = None
        if queue.startswith('pizza.retry.'):
            arguments = {'x-message-ttl': RETRY_DELAY_MS, 'x-dead-letter-exchange': '',
                         'x-dead-letter-routing-key': 'pizza.' + queue.rsplit('.', 1)[1]}
        channel.queue_declare(queue=queue, durable=True, arguments=arguments)
    channel.basic_qos(prefetch_count=1)
    channel.confirm_delivery()
    return channel


def publish(channel, queue, payload):
    # With confirms enabled, an unroutable return or broker nack raises.
    channel.basic_publish(
        exchange='', routing_key=queue, body=json.dumps(payload).encode(),
        mandatory=True,
        properties=pika.BasicProperties(content_type='application/json', delivery_mode=2),
    )
