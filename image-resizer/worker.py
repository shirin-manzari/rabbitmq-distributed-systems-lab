import argparse
from contextlib import closing
import json
import math
import os
from pathlib import Path
import sys
import tempfile
import uuid

import pika
from PIL import Image

QUEUE = 'image.resize'
DEFAULT_URL = 'amqp://resizer:resizer@localhost:5673/%2F'


class InvalidJob(ValueError):
    pass


def parse_job(body):
    try:
        job = json.loads(body)
        if not isinstance(job, dict) or set(job) != {'job_id', 'source', 'width'}:
            raise ValueError('expected job_id, source and width')
        if not isinstance(job['job_id'], str) or str(uuid.UUID(job['job_id'])) != job['job_id']:
            raise ValueError('job_id must be a canonical UUID')
        if not isinstance(job['source'], str) or not Path(job['source']).is_absolute():
            raise ValueError('source must be an absolute path')
        if type(job['width']) is not int or job['width'] <= 0:
            raise ValueError('width must be a positive integer')
        return job
    except (ValueError, TypeError) as error:
        raise InvalidJob(str(error)) from error


def resize(job, output):
    try:
        with Image.open(job['source']) as source:
            source.load()
            height = max(1, round(source.height * job['width'] / source.width))
            # Convert palette/CMYK/etc. so all supported inputs can become PNGs.
            mode = 'RGBA' if 'A' in source.getbands() or 'transparency' in source.info else 'RGB'
            result = source.convert(mode).resize((job['width'], height), Image.Resampling.LANCZOS)
    except (OSError, ValueError, Image.DecompressionBombError) as error:
        raise InvalidJob(f'source cannot be read/resized: {error}') from error

    final = output / f'{job["job_id"]}.png'
    with closing(result):
        with tempfile.NamedTemporaryFile(dir=output, prefix=f'.{job["job_id"]}-', suffix='.png', delete=False) as file:
            temporary = Path(file.name)
        try:
            result.save(temporary, format='PNG')
            os.replace(temporary, final)
        finally:
            temporary.unlink(missing_ok=True)
    return final, height


def handle(connection, channel, delivery, body, name, delay, output):
    try:
        job = parse_job(body)
    except InvalidJob as error:
        print(f'REJECT worker={name} reason={error}', flush=True)
        channel.basic_reject(delivery_tag=delivery.delivery_tag, requeue=False)
        return
    print(f'START worker={name} job={job["job_id"]} short={job["job_id"][:8]} '
          f'redelivered={delivery.redelivered}', flush=True)
    if delay:
        # Let Pika service heartbeats throughout the artificial delay.
        connection.sleep(delay)
    try:
        final, height = resize(job, output)
    except InvalidJob as error:
        print(f'REJECT worker={name} job={job["job_id"]} reason={error}', flush=True)
        channel.basic_reject(delivery_tag=delivery.delivery_tag, requeue=False)
        return
    # Unexpected output/connection errors propagate: closing requeues the delivery.
    channel.basic_ack(delivery_tag=delivery.delivery_tag)
    print(f'DONE worker={name} job={job["job_id"]} size={job["width"]}x{height} '
          f'output={final} ack=sent', flush=True)


def nonnegative_delay(value):
    try:
        delay = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError('delay must be a finite nonnegative number') from None
    if not math.isfinite(delay) or delay < 0:
        raise argparse.ArgumentTypeError('delay must be a finite nonnegative number')
    return delay


def main():
    parser = argparse.ArgumentParser(description='Resize local images from a shared RabbitMQ work queue.')
    parser.add_argument('--name', default=f'worker-{os.getpid()}')
    parser.add_argument('--delay', type=nonnegative_delay, default=0)
    parser.add_argument('--output', type=Path, default=Path('output'))
    args = parser.parse_args()
    output = args.output.expanduser().resolve()
    connection = None
    try:
        output.mkdir(parents=True, exist_ok=True)
        parameters = pika.URLParameters(os.getenv('AMQP_URL', DEFAULT_URL))
        parameters.heartbeat = 30
        parameters.socket_timeout = 5
        parameters.stack_timeout = 10
        parameters.blocked_connection_timeout = 10
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE, durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=QUEUE, auto_ack=False,
            on_message_callback=lambda ch, method, properties, body:
                handle(connection, ch, method, body, args.name, args.delay, output))
        print(f'WAITING worker={args.name} pid={os.getpid()} queue={QUEUE} delay={args.delay:g}s', flush=True)
        channel.start_consuming()
    except KeyboardInterrupt:
        print(f'STOP worker={args.name}; unfinished delivery will be requeued on close.', flush=True)
    except Exception as error:
        print(f'FAILED worker={args.name}: {type(error).__name__}: {error}', file=sys.stderr, flush=True)
        return 1
    finally:
        if connection and connection.is_open:
            try:
                connection.close()
            except Exception as error:
                print(f'Connection close failed ({type(error).__name__}).', file=sys.stderr)
    return 0


if __name__ == '__main__':
    sys.exit(main())
