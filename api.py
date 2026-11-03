import asyncio
from contextlib import asynccontextmanager, contextmanager
import logging
import math
from pathlib import Path
import threading
import time
from typing import Literal
import uuid
import urllib.error

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, StrictBool, StrictInt, field_validator

from contracts import WORKERS, Stage, Task, ControlRequest, DisplayEvent, Heartbeat
from messaging import QUEUES, connect, setup, publish
from state import KitchenState
from store import Store
import runtime

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('kitchen.api')
state = KitchenState(Store())
stop = threading.Event()
monitor_stop = threading.Event()
operations = threading.RLock()
consumer_thread = None
metrics = {'connected': False, 'queues': [], 'sampled_at': None}
host_connected = False


@contextmanager
def mutation():
    session = state.session_id
    if state.resetting:
        raise HTTPException(409, 'Reset in progress; controls and order admission are stopped')
    with operations:
        if state.resetting or state.session_id != session:
            raise HTTPException(409, 'Demo session changed while the request was waiting')
        yield


def consume_events():
    while not stop.is_set():
        connection = None
        try:
            connection = connect()
            channel = setup(connection)

            def on_event(ch, method, properties, body):
                try:
                    event = DisplayEvent.model_validate_json(body)
                except ValueError:
                    log.warning('Discarding malformed display event')
                    ch.basic_reject(method.delivery_tag, requeue=False)
                    return
                if event.session_id == state.session_id:
                    with state.lock:
                        if state.store:
                            state.store.save_event(event)
                        state.apply(event)
                ch.basic_ack(method.delivery_tag)

            channel.basic_consume('pizza.events', on_event, auto_ack=False)
            with state.lock:
                state.events_connected = True
            while not stop.is_set():
                connection.process_data_events(time_limit=0.5)
        except Exception as error:
            log.warning('Display consumer disconnected: %s', type(error).__name__)
        finally:
            with state.lock:
                state.events_connected = False
            if connection and connection.is_open:
                try:
                    connection.close()
                except Exception:
                    log.warning('Display connection close failed; reconnecting after backoff')
        stop.wait(2)


def monitor_runtime():
    global metrics, host_connected
    while not monitor_stop.is_set():
        try:
            workers = runtime.supervisor('/workers')
            with state.lock:
                for worker in workers:
                    current = state.workers.get(worker['worker_id'])
                    if current:
                        current['process_running'] = worker['running']
                        if not worker['running']:
                            state.mark_interrupted(worker['worker_id'])
                        if current['retiring'] and not worker['running']:
                            current['removed'] = True
                if not state.resetting:
                    state.persist_workers()
            host_connected = True
        except (OSError, ValueError):
            host_connected = False
        try:
            sampled_session = state.session_id
            queues = runtime.broker('/api/queues/%2F')
            if sampled_session == state.session_id and not state.resetting:
                metrics = {'connected': True, 'sampled_at': time.time(),
                    'queues': [{ 'name': q['name'], 'ready_messages': q.get('messages_ready', 0),
                                'unacknowledged': q.get('messages_unacknowledged', 0),
                                'consumers': q.get('consumers', 0)} for q in queues if q['name'] in QUEUES]}
        except (OSError, ValueError):
            metrics = {**metrics, 'connected': False}
        monitor_stop.wait(1)


def start_consumer():
    global consumer_thread
    stop.clear()
    consumer_thread = threading.Thread(target=consume_events, name='display-events', daemon=True)
    consumer_thread.start()


@asynccontextmanager
async def lifespan(app):
    runtime.install_dead_letters()
    start_consumer()
    monitor_stop.clear()
    monitor = threading.Thread(target=monitor_runtime, name='runtime-observer', daemon=True)
    monitor.start()
    try:
        yield
    finally:
        stop.set()
        monitor_stop.set()
        await asyncio.to_thread(consumer_thread.join, 10)
        await asyncio.to_thread(monitor.join, 10)


app = FastAPI(title='Distributed Kitchen', lifespan=lifespan)


class OrderRequest(BaseModel):
    count: StrictInt
    failure_kind: Literal['none', 'invalid', 'transient'] = 'none'

    @field_validator('count')
    @classmethod
    def allowed_count(cls, count):
        if count not in (1, 20):
            raise ValueError('count must be exactly 1 or 20')
        return count


@app.get('/api/health')
def health():
    with state.lock:
        connected = state.events_connected
    return JSONResponse({'events_connected': connected}, status_code=200 if connected else 503)


@app.get('/api/state')
def snapshot():
    snapshot = state.snapshot()
    snapshot['broker_metrics'] = metrics
    snapshot['supervisor_connected'] = host_connected
    snapshot['effects'] = state.store.effects() if state.store else []
    return snapshot


@app.get('/api/control')
def control():
    return state.control()


@app.post('/api/control')
def set_control(request: ControlRequest):
    with mutation():
        return state.set_paused(request.paused)


@app.post('/api/workers/{worker_id}/heartbeat')
def heartbeat(worker_id: str, report: Heartbeat):
    if worker_id not in state.workers:
        raise HTTPException(status_code=404, detail='Unknown worker')
    try:
        result = state.heartbeat(worker_id, report)
        worker = state.workers[worker_id]
        return {**result, 'paused': result['paused'] or state.resetting,
                'duration_s': state.durations[worker['stage']], 'retiring': worker['retiring'] or worker['removed'],
                'crash_point': worker['crash_point'], 'crash_token': worker['crash_token']}
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@app.post('/api/orders')
def add_orders(request: OrderRequest):
    with mutation():
        accepted = []
        unconfirmed = []
        connection = None
        try:
            connection = connect()
            channel = setup(connection)
            for _ in range(request.count):
                task = state.reserve(failure_kind=request.failure_kind)
                try:
                    publish(channel, 'pizza.prepare', task.model_dump())
                except Exception:
                    state.discard_unconfirmed(task)
                    # Loss of confirmation can be ambiguous: the broker might have
                    # received this ID. Never report it as accepted or auto-retry it.
                    unconfirmed.append(task.order_id)
                    raise
                state.accept(task)
                accepted.append(task.model_dump())
                if state.store:
                    state.store.accept(task)
        except Exception as error:
            log.warning('Order publication failed: %s', type(error).__name__)
            return JSONResponse({
                'accepted': accepted, 'requested': request.count,
                'unconfirmed_order_ids': unconfirmed,
                'not_attempted': request.count - len(accepted) - len(unconfirmed),
                'error': 'Publication interrupted. Only the listed accepted orders were confirmed. '
                         'Unconfirmed orders may have reached the broker; do not retry automatically.',
            }, status_code=503)
        finally:
            if connection and connection.is_open:
                try:
                    connection.close()
                except Exception:
                    log.warning('Connection close failed after publication')
        return {'accepted': accepted, 'requested': request.count,
                'unconfirmed_order_ids': [], 'not_attempted': 0}


class SettingsRequest(BaseModel):
    durations: dict[Stage, float] | None = None
    idempotency: StrictBool | None = None

    @field_validator('durations')
    @classmethod
    def duration_bounds(cls, value):
        if value is not None and any(not math.isfinite(seconds) or not 0.2 <= seconds <= 20 for seconds in value.values()):
            raise ValueError('Durations must be between 0.2 and 20 seconds')
        return value


@app.post('/api/settings')
def settings(request: SettingsRequest):
    with mutation(), state.lock:
        if request.durations:
            state.durations.update(request.durations)
            state.store.set('durations', state.durations)
        if request.idempotency is not None:
            state.idempotency = request.idempotency
            state.store.set('idempotency', state.idempotency)
        return state.control()


class NewWorker(BaseModel):
    stage: Stage


@app.post('/api/workers')
def add_worker(request: NewWorker):
    with mutation(), state.lock:
        if sum(not w['removed'] and w['stage'] == request.stage for w in state.workers.values()) >= 4:
            raise HTTPException(409, 'Classroom limit: four workers per stage')
        prefix = {'prepare': 'prep', 'bake': 'baker', 'pack': 'pack'}[request.stage]
        number = 2 if request.stage in ('prepare', 'pack') else 1
        while f'{prefix}-{number}' in state.workers:
            number += 1
        worker_id = f'{prefix}-{number}'
        state.register_worker(worker_id, request.stage)
        try:
            runtime.supervisor('/workers', {'worker_id': worker_id, 'stage': request.stage})
            state.workers[worker_id]['process_running'] = True
        except OSError as error:
            raise HTTPException(503, 'Worker host unavailable; worker remains offline') from error
        return {'worker_id': worker_id}


class WorkerAction(BaseModel):
    action: Literal['start', 'stop', 'remove', 'crash_during', 'crash_after_effect', 'cancel_crash']


@app.post('/api/workers/{worker_id}')
def worker_action(worker_id: str, request: WorkerAction):
    with mutation(), state.lock:
        worker = state.workers.get(worker_id)
        if not worker:
            raise HTTPException(404, 'Unknown worker')
        try:
            if request.action == 'start':
                worker.update(retiring=False, removed=False)
                runtime.supervisor('/workers', {'worker_id': worker_id, 'stage': worker['stage']})
                worker['process_running'] = True
            elif request.action == 'stop':
                runtime.supervisor(f'/workers/{worker_id}/stop', {})
                worker.update(process_running=False, crash_point=None, crash_token=None)
                state.mark_interrupted(worker_id)
            elif request.action == 'remove':
                worker.update(retiring=True, crash_point=None, crash_token=None)
                if worker['process_running'] is False:
                    worker['removed'] = True
            elif request.action == 'cancel_crash':
                worker.update(crash_point=None, crash_token=None)
            else:
                if worker['process_running'] is False or worker['removed']:
                    raise HTTPException(409, 'Start this worker before arming a crash')
                worker.update(crash_point='during' if request.action == 'crash_during' else 'after_effect', crash_token=str(uuid.uuid4()))
        except urllib.error.HTTPError as error:
            raise HTTPException(error.code, 'Worker host rejected the action') from error
        except OSError as error:
            raise HTTPException(503, 'Worker host unreachable') from error
        state.persist_workers()
        return {'worker_id': worker_id, 'action': request.action}


class CrashClaim(BaseModel):
    token: str


@app.post('/api/workers/{worker_id}/crash/claim')
def claim_crash(worker_id: str, request: CrashClaim):
    with state.lock:
        worker = state.workers.get(worker_id)
        if not worker or worker['crash_token'] != request.token:
            return {'fire': False}
        worker.update(crash_point=None, crash_token=None)
        return {'fire': True}


@app.get('/api/orders/{order_id}/history')
def order_history(order_id: str):
    with state.lock:
        if order_id not in state.orders or not state.orders[order_id]['accepted']:
            raise HTTPException(404, 'Unknown accepted order')
        return {'order': state.orders[order_id], 'events': state.timelines.get(order_id, []),
                'effects': state.store.effects(order_id)}


class ReplayRequest(BaseModel):
    stage: Stage


@app.post('/api/orders/{order_id}/redeliver')
def redeliver(order_id: str, request: ReplayRequest):
    with mutation(), state.lock:
        order = state.orders.get(order_id)
        if not order or not order['accepted'] or order['category'] != 'ready':
            raise HTTPException(409, 'Choose a completed order to compare repeated stage effects')
        if order.get('history_unavailable'):
            raise HTTPException(409, 'This imported v0 order predates durable effect history; submit a new order for replay experiments')
        task = Task(**{key: order[key] for key in ('order_id', 'session_id', 'label', 'pizza_type')}, force_redelivery=True)
        connection = None
        try:
            connection = connect()
            channel = setup(connection)
            publish(channel, f'pizza.{request.stage}', task.model_dump())
        except Exception as error:
            raise HTTPException(503, 'Duplicate request was not confirmed; do not retry automatically') from error
        finally:
            if connection and connection.is_open:
                try:
                    connection.close()
                except Exception:
                    # Closing cannot undo publisher confirmation or mask its failure.
                    log.warning('Connection close failed after replay publication', exc_info=True)
        return {'confirmed': True, 'order_id': order_id, 'stage': request.stage,
                'detail': 'Same identity published; worker performs a real broker requeue before the repeated attempt'}


class ResetRequest(BaseModel):
    session_id: str
    confirmation: Literal['RESET']


@app.post('/api/reset')
def reset_demo(request: ResetRequest):
    global state, metrics
    with operations:
        if request.session_id != state.session_id:
            raise HTTPException(409, 'Session changed; review and confirm the current demo')
        if stop.is_set() and consumer_thread is not None:
            consumer_thread.join(6)
            if not consumer_thread.is_alive():
                start_consumer()  # Recovery after an earlier partial reset stopped observation.
        state.resetting = True
        state.set_paused(True)
        deadline = time.monotonic() + 30
        while state.control()['status'] != 'paused':
            if time.monotonic() > deadline:
                state.resetting = False
                raise HTTPException(409, 'Could not drain within 30s; reset aborted and kitchen remains paused')
            time.sleep(0.1)
        try:
            runtime.supervisor('/stop-all', {})  # Waits for every child process to exit.
            stop.set()
            consumer_thread.join(10)
            if consumer_thread.is_alive():
                raise RuntimeError('Display consumer did not stop')
            connection = connect()
            try:
                channel = connection.channel()
                # Explicitly scoped demo queues only; no vhost-wide purge.
                for queue in QUEUES:
                    channel.queue_delete(queue=queue)
                setup(connection)
            finally:
                connection.close()
            store = state.store
            store.reset()
            state = KitchenState(store)
            metrics = {'connected': False, 'queues': [], 'sampled_at': None}
            start_consumer()
            baseline = runtime.supervisor('/baseline', {})
            with state.lock:
                for worker in baseline:
                    state.workers[worker['worker_id']]['process_running'] = worker['running']
            return {'session_id': state.session_id, 'reset': True, 'baseline': WORKERS}
        except Exception as error:
            # Do not restart admission after an incomplete destructive reset.
            state.resetting = True
            state.set_paused(True)
            raise HTTPException(503, 'Reset incomplete; admission remains blocked. Retry confirmed reset after restoring services.') from error


app.mount('/', StaticFiles(directory=Path(__file__).parent / 'web', html=True), name='web')
