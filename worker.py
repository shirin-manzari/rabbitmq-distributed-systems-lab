import json
import logging
import os
import time
import urllib.request
from urllib.parse import urljoin
import uuid

from contracts import STAGES, ControlResponse, DisplayEvent, Task
from messaging import RETRY_DELAY_MS, connect, setup, publish
from store import Store

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger('kitchen.worker')


class WorkerControl:
    def __init__(self, worker_id):
        url = os.getenv('CONTROL_URL', 'http://localhost:8000/api/control')
        self.url = urljoin(url, f'workers/{worker_id}/heartbeat')
        self.worker_url = urljoin(url, f'workers/{worker_id}/')
        self.instance_id = str(uuid.uuid4())
        self.sequence = 0
        self.observed_session_id = None
        self.observed_revision = -1
        self.active_order_id = None
        self.attempt_id = None
        self.broker_connected = False
        self.config = None

    def post(self, url, payload):
        request = urllib.request.Request(url, data=json.dumps(payload).encode(), headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=0.8) as response:
            return json.load(response)

    def observe(self):
        self.sequence += 1
        payload = dict(instance_id=self.instance_id, sequence=self.sequence,
            observed_session_id=self.observed_session_id, observed_revision=self.observed_revision,
            active_order_id=self.active_order_id, attempt_id=self.attempt_id,
            broker_connected=self.broker_connected)
        try:
            self.config = ControlResponse.model_validate(self.post(self.url, payload))
            self.observed_session_id = self.config.session_id
            self.observed_revision = self.config.revision
            return not self.config.paused and not self.config.retiring
        except (OSError, ValueError):
            return False

    def crash(self, point, emit):
        if not self.config or self.config.crash_point != point or not self.config.crash_token:
            return
        try:
            claim = self.post(self.worker_url + 'crash/claim', {'token': self.config.crash_token})
        except (OSError, ValueError):
            return
        if claim.get('fire'):
            emit('crash_triggered', f'One-shot crash: {point}; trigger consumed before process exit')
            os._exit(86)  # Real process/connection failure, never a visual simulation.


def process(connection, channel, method, body, stage, worker_id, duration, control=None, store=None):
    try:
        task = Task.model_validate_json(body)
    except ValueError:
        log.error('%s rejecting malformed wire payload', worker_id)
        channel.basic_reject(method.delivery_tag, requeue=False)
        return
    if control and control.config and task.session_id != control.config.session_id:
        channel.basic_ack(method.delivery_tag)  # Old-session work must not enter a reset demo.
        log.warning('Discarded stale-session task %s', task.order_id)
        return
    attempt_id = str(uuid.uuid4())
    attempt = store.next_attempt(task.order_id, stage) if store else 1
    idempotency = bool(control and control.config and control.config.idempotency)

    def emit(status, detail='', retry_count=None):
        event = DisplayEvent(event_id=str(uuid.uuid4()), session_id=task.session_id,
            order_id=task.order_id, stage=stage, status=status, worker_id=worker_id,
            attempt_id=attempt_id, timestamp=time.time(), duration_s=duration,
            redelivered=bool(method.redelivered), delivery_attempt=attempt,
            retry_count=task.retry_count if retry_count is None else retry_count, detail=detail)
        publish(channel, 'pizza.events', event.model_dump())

    def ack():
        channel.basic_ack(method.delivery_tag)
        # Consumer ACKs have no broker ACK confirmation. Do not label this confirmed.
        try:
            emit('ack_sent', 'Worker sent consumer ACK; RabbitMQ does not confirm consumer ACKs')
        except Exception:
            log.warning('ACK was sent but its observational report could not be published')

    if control:
        control.active_order_id, control.attempt_id = task.order_id, attempt_id
    try:
        if control:
            control.observe()
        emit('delivered', 'RabbitMQ basic.get delivery received')
        if method.redelivered:
            emit('redelivered', 'Actual RabbitMQ delivery metadata: redelivered=True')
        if task.force_redelivery and not method.redelivered:
            emit('requeue_sent', 'Instructor requested one real broker requeue before processing')
            channel.basic_nack(method.delivery_tag, requeue=True)
            connection.sleep(0.5)
            return
        emit('started')
        log.info('%s started %s attempt=%s order_id=%s redelivered=%s', worker_id, task.label, attempt, task.order_id, method.redelivered)
        if control:
            control.crash('during', emit)
        invalid = task.failure_kind == 'invalid' or (task.failure_kind == 'transient' and stage == 'prepare' and task.retry_count == 0)
        if invalid:
            if task.retry_count < 2:
                retried = task.model_copy(update={'retry_count': task.retry_count + 1, 'force_redelivery': False})
                publish(channel, f'pizza.retry.{stage}', retried.model_dump())
                emit('retry_scheduled', f'Retry {retried.retry_count}/2 confirmed into a {RETRY_DELAY_MS/1000:g}s RabbitMQ TTL queue', retried.retry_count)
                ack()
            else:
                emit('failed', 'Retry limit reached; rejecting to pizza.dead')
                channel.basic_reject(method.delivery_tag, requeue=False)
                emit('reject_sent', 'Worker sent reject(requeue=False); configured RabbitMQ DLX routes to pizza.dead')
            return
        if control:
            finish_at = time.monotonic() + duration
            while (remaining := finish_at - time.monotonic()) > 0:
                connection.sleep(min(0.5, remaining))
                control.observe()  # Active work finishes even if admission is paused/unavailable.
                control.crash('during', emit)
        else:
            connection.sleep(duration)
        emit('processing_completed', 'Stage computation finished; effect/handoff/ACK remain separate operations')
        if store:
            applied, effect_id = store.effect(task, stage, attempt_id, idempotency)
            emit('effect_recorded' if applied else 'duplicate_skipped',
                 f'Durable stage effect {effect_id}' if applied else f'Shared (order_id, stage) completion exists: effect {effect_id}; no repeated effect')
        if control:
            control.crash('after_effect', emit)
        if stage != 'pack':
            next_stage = STAGES[STAGES.index(stage) + 1]
            next_task = task.model_copy(update={'retry_count': 0, 'force_redelivery': False})
            publish(channel, f'pizza.{next_stage}', next_task.model_dump())
            emit('handoff_confirmed', f'Publisher confirmation received for pizza.{next_stage}; current delivery still unacknowledged')
            emit('stage_completed')
        else:
            emit('ready', 'Packaging finished; ready event published before current delivery ACK')
        ack()
    finally:
        if control:
            control.active_order_id = control.attempt_id = None


def main():
    stage, worker_id = os.environ['STAGE'], os.environ['WORKER_ID']
    if stage not in STAGES:
        raise ValueError('Unknown stage')
    store = Store()
    control = WorkerControl(worker_id)
    while True:
        connection = None
        try:
            control.observe()
            connection = connect()
            channel = setup(connection)
            control.broker_connected = True
            while True:
                allowed = control.observe()
                if control.config and control.config.retiring:
                    return  # Graceful removal after any current delivery finished.
                if not allowed:
                    connection.sleep(0.5)
                    continue
                method, properties, body = channel.basic_get(f'pizza.{stage}', auto_ack=False)
                if method is None:
                    connection.sleep(0.25)
                    continue
                duration = control.config.duration_s
                process(connection, channel, method, body, stage, worker_id, duration, control, store)
        except KeyboardInterrupt:
            return
        except Exception as error:
            log.warning('%s handoff/connection failed: %s; backing off', worker_id, type(error).__name__)
        finally:
            if connection and connection.is_open:
                try:
                    connection.close()
                except Exception:
                    log.warning('%s connection close failed', worker_id, exc_info=True)
            control.broker_connected = False
            control.observe()
        time.sleep(2)


if __name__ == '__main__':
    main()
