import argparse
import json
import os
from pathlib import Path
import sys
import uuid

import pika

QUEUE = 'image.resize'
DEFAULT_URL = 'amqp://resizer:resizer@localhost:5673/%2F'


def positive_width(value):
    try:
        width = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError('width must be a positive integer') from None
    if width <= 0:
        raise argparse.ArgumentTypeError('width must be a positive integer')
    return width


def main():
    parser = argparse.ArgumentParser(description='Publish one confirmed resize job per local image.')
    parser.add_argument('images', nargs='+', type=Path)
    parser.add_argument('--width', required=True, type=positive_width)
    args = parser.parse_args()
    sources = [path.expanduser().resolve() for path in args.images]
    for source in sources:
        if not source.is_file():
            parser.error(f'not a source file: {source}')

    connection = None
    confirmed = 0
    job = None
    try:
        parameters = pika.URLParameters(os.getenv('AMQP_URL', DEFAULT_URL))
        parameters.socket_timeout = 5
        parameters.stack_timeout = 10
        parameters.blocked_connection_timeout = 10
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE, durable=True)
        channel.confirm_delivery()
        for source in sources:
            job_id = str(uuid.uuid4())
            job = {'job_id': job_id, 'source': str(source), 'width': args.width}
            channel.basic_publish(
                exchange='', routing_key=QUEUE, body=json.dumps(job).encode(), mandatory=True,
                properties=pika.BasicProperties(content_type='application/json', delivery_mode=2),
            )
            confirmed += 1
            job = None
            print(f'SUBMITTED job={job_id} source={source} width={args.width}', flush=True)
    except Exception as error:
        # Confirmation loss is ambiguous: a failed call may still reach RabbitMQ.
        print(f'Publication interrupted ({type(error).__name__}); confirmed={confirmed}/{len(sources)}. '
              'Do not automatically resubmit the batch.', file=sys.stderr)
        if job:
            print(f'UNCONFIRMED job={job["job_id"]}; it may have reached the broker.', file=sys.stderr)
        return 1
    finally:
        if connection and connection.is_open:
            try:
                connection.close()
            except Exception as error:
                print(f'Connection close failed ({type(error).__name__}); '
                      'this does not undo confirmed publications.', file=sys.stderr)
    print(f'Confirmed {confirmed} job(s).', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
