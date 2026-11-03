from copy import deepcopy
from threading import RLock
import time
import uuid

from contracts import DURATIONS, STAGES, WORKERS, DisplayEvent, Heartbeat, Task

WORKER_TIMEOUT = 4.0


class KitchenState:
    def __init__(self, store=None):
        self.lock = RLock()
        self.store = store
        self.session_id = store.get('session_id') if store else str(uuid.uuid4())
        self.resetting = False
        self.durations = store.get('durations', DURATIONS.copy()) if store else DURATIONS.copy()
        self.idempotency = store.get('idempotency', False) if store else False
        self.started_at = time.monotonic()
        self.paused = False
        self.control_revision = 0
        self.orders = {}
        self.workers = {}
        self.seen_events = set()
        self.sequence = 0
        self.events_connected = False
        self.last_event_at = None
        self.retired_instances = {}
        self.timelines = {}
        for worker_id, stage in WORKERS.items():
            self.register_worker(worker_id, stage, persist=False)
        if store:
            for key, config in store.get('workers', {}).items():
                self.register_worker(key, config['stage'], persist=False)
                self.workers[key].update(retiring=config.get('retiring', False), removed=config.get('removed', False))
            orders, events = store.history()
            legacy = store.get('legacy_states', {})
            for payload in orders:
                task = Task.model_validate(payload)
                if task.session_id == self.session_id:
                    self.reserve(task=task)
                    self.accept(task)
                    if task.order_id in legacy:
                        self.orders[task.order_id].update(legacy[task.order_id])
            for payload in events:
                self.apply(DisplayEvent.model_validate(payload))

    def register_worker(self, worker_id, stage, persist=True):
        with self.lock:
            if worker_id not in self.workers:
                self.workers[worker_id] = {
                    'worker_id': worker_id, 'stage': stage,
                    'order_id': None, 'attempt_id': None, 'timestamp': 0,
                    'status': 'unobserved', 'instance_id': None, 'sequence': 0,
                    'last_seen': None, 'active_order_id': None, 'active_attempt_id': None,
                    'observed_session_id': None, 'observed_revision': -1,
                    'broker_connected': False, 'retiring': False, 'removed': False,
                    'process_running': None, 'crash_point': None, 'crash_token': None,
                }
                self.retired_instances[worker_id] = set()
            if persist:
                self.persist_workers()

    def persist_workers(self):
        if self.store:
            self.store.set('workers', {key: {k: w[k] for k in ('stage', 'retiring', 'removed')} for key, w in self.workers.items()})

    def mark_interrupted(self, worker_id):
        for order in self.orders.values():
            if order['category'] == 'cooking' and order['worker_id'] == worker_id:
                order['orphaned_attempt'] = True

    def _worker_views(self):
        now = time.monotonic()
        workers = deepcopy(list(self.workers.values()))
        for worker in workers:
            seen = worker.pop('last_seen')
            worker['last_seen_age_s'] = now - seen if seen is not None else None
            if seen is None:
                worker['availability'] = 'starting' if now - self.started_at < WORKER_TIMEOUT else 'unavailable'
            else:
                worker['availability'] = 'online' if now - seen < WORKER_TIMEOUT else 'unavailable'
            if worker['process_running'] is False:
                worker['availability'] = 'unavailable'
            if seen is not None:
                worker['order_id'] = worker['active_order_id']
                worker['attempt_id'] = worker['active_attempt_id']
                worker['status'] = 'busy' if worker['active_order_id'] else (
                    'paused' if self.paused and worker['observed_session_id'] == self.session_id
                    and worker['observed_revision'] == self.control_revision else 'idle')
            if worker['availability'] == 'unavailable':
                worker['display_state'] = 'offline'
            else:
                worker['display_state'] = 'processing' if worker['active_order_id'] else 'idle'
        return workers

    def _control_view(self, workers):
        workers = [worker for worker in workers if not worker['removed']]
        live = [worker for worker in workers if worker['availability'] == 'online']
        live_ids = {worker['worker_id'] for worker in live}
        waiting = any(worker['availability'] == 'starting' for worker in workers) or any(
            worker['observed_session_id'] != self.session_id
            or worker['observed_revision'] != self.control_revision
            or worker['active_order_id'] is not None for worker in live)
        # Let completed display events catch up before claiming a drained scene.
        waiting |= any(order['category'] == 'cooking' and not order.get('orphaned_attempt') and order['worker_id'] in live_ids
                       and self.workers[order['worker_id']]['broker_connected']
                       for order in self.orders.values())
        return {'session_id': self.session_id, 'paused': self.paused,
                'revision': self.control_revision,
                'durations': self.durations.copy(), 'idempotency': self.idempotency, 'resetting': self.resetting,
                'status': ('pausing' if waiting or not self.events_connected else 'paused') if self.paused else 'running',
                'unavailable_workers': [w['worker_id'] for w in workers if
                                        w['availability'] != 'online' or not w['broker_connected']]}

    def control(self):
        with self.lock:
            return self._control_view(self._worker_views())

    def set_paused(self, paused):
        with self.lock:
            if self.paused != paused:
                self.paused = paused
                self.control_revision += 1
            return self._control_view(self._worker_views())

    def heartbeat(self, worker_id, report: Heartbeat):
        with self.lock:
            worker = self.workers[worker_id]
            if report.instance_id in self.retired_instances[worker_id]:
                raise ValueError('Heartbeat from a superseded worker instance')
            if worker['instance_id'] != report.instance_id:
                if worker['instance_id'] is not None or report.active_order_id is None:
                    self.mark_interrupted(worker_id)
                if worker['instance_id'] is not None:
                    self.retired_instances[worker_id].add(worker['instance_id'])
                worker.update(instance_id=report.instance_id, sequence=0)
            if report.sequence > worker['sequence']:
                if worker['process_running'] is None:
                    worker['process_running'] = True  # A valid heartbeat proves this process exists.
                worker.update(sequence=report.sequence, last_seen=time.monotonic(),
                              active_order_id=report.active_order_id,
                              active_attempt_id=report.attempt_id,
                              observed_session_id=report.observed_session_id,
                              observed_revision=report.observed_revision,
                              broker_connected=report.broker_connected)
            return self._control_view(self._worker_views())

    def reserve(self, task=None, failure_kind='none'):
        with self.lock:
            if task is None:
                self.sequence += 1
                task = Task(order_id=str(uuid.uuid4()), session_id=self.session_id,
                            label=f'#{self.sequence:04d}', failure_kind=failure_kind)
            else:
                self.sequence = max(self.sequence, int(task.label.lstrip('#')))
            self.orders[task.order_id] = {
                **task.model_dump(), 'accepted': False, 'category': 'queued',
                'stage': 'prepare', 'worker_id': None, 'rank': 0, 'started_at': None,
                'duration_s': None, 'redelivered': False, 'event_timestamp': 0,
                'retry_count': 0, 'delivery_attempt': 0,
                'orphaned_attempt': False,
            }
            return task

    def accept(self, task):
        with self.lock:
            self.orders[task.order_id]['accepted'] = True

    def discard_unconfirmed(self, task):
        with self.lock:
            self.orders.pop(task.order_id, None)

    def apply(self, event: DisplayEvent):
        with self.lock:
            if (event.session_id != self.session_id or event.order_id not in self.orders
                    or event.event_id in self.seen_events
                    or self.workers.get(event.worker_id, {}).get('stage') != event.stage
                    or (event.status == 'ready' and event.stage != 'pack')
                    or (event.status == 'stage_completed' and event.stage == 'pack')):
                return False
            self.seen_events.add(event.event_id)
            self.timelines.setdefault(event.order_id, []).append(event.model_dump())
            self.last_event_at = time.time()
            worker = self.workers[event.worker_id]
            # Occupancy describes physical attempts, even if duplicate business work
            # runs after an order's display has already advanced.
            if event.timestamp >= worker['timestamp']:
                if event.status == 'started':
                    worker.update(order_id=event.order_id, attempt_id=event.attempt_id,
                                  timestamp=event.timestamp, status='busy')
                elif event.status in ('stage_completed', 'ready', 'failed', 'retry_scheduled', 'ack_sent') and worker['attempt_id'] in (None, event.attempt_id):
                    worker.update(order_id=None, attempt_id=None,
                                  timestamp=event.timestamp, status='idle')
            order = self.orders[event.order_id]
            order['redelivered'] = order['redelivered'] or event.redelivered
            order['delivery_attempt'] = max(order['delivery_attempt'], event.delivery_attempt)
            order['retry_count'] = max(order['retry_count'], event.retry_count)
            if event.status == 'failed':
                if order['category'] != 'ready':
                    order.update(category='failed', stage=event.stage, worker_id=None, rank=7,
                                 started_at=None, duration_s=None)
                return True
            if event.status == 'retry_scheduled':
                if order['rank'] <= STAGES.index(event.stage) * 2 + 1:
                    order.update(category='queued', worker_id=None, started_at=None, duration_s=None)
                return True
            if event.status not in ('started', 'stage_completed', 'ready'):
                return True
            rank = STAGES.index(event.stage) * 2 + (1 if event.status == 'started' else 2)
            if rank < order['rank'] or (rank == order['rank'] and (
                    event.status != 'started' or event.timestamp <= order['event_timestamp'])):
                return False
            order.update(rank=rank, event_timestamp=event.timestamp)
            if event.status == 'started':
                order.update(category='cooking', stage=event.stage, worker_id=event.worker_id,
                             started_at=event.timestamp, duration_s=event.duration_s, orphaned_attempt=False)
            elif event.status == 'ready':
                order.update(category='ready', stage='pack', worker_id=None,
                             started_at=None, duration_s=None)
            else:
                order.update(category='queued', stage=STAGES[STAGES.index(event.stage) + 1],
                             worker_id=None, started_at=None, duration_s=None)
            return True

    def snapshot(self):
        with self.lock:
            workers = self._worker_views()
            worker_by_id = {worker['worker_id']: worker for worker in workers}
            orders = [deepcopy(order) for order in self.orders.values() if order['accepted']]
            for order in orders:
                for private in ('rank', 'accepted', 'event_timestamp'):
                    del order[private]
                worker = worker_by_id.get(order['worker_id'])
                order['activity_uncertain'] = bool(order['category'] == 'cooking' and worker
                    and (order.get('orphaned_attempt') or worker['availability'] != 'online' or not worker['broker_connected']))
            return {
                'session_id': self.session_id, 'server_time': time.time(),
                'orders': orders, 'workers': workers, 'control': self._control_view(workers),
                'counts': {category: sum(o['category'] == category for o in orders)
                           for category in ('queued', 'cooking', 'ready', 'failed')},
                'accepted_count': len(orders), 'events_connected': self.events_connected,
                'last_event_at': self.last_event_at,
            }
