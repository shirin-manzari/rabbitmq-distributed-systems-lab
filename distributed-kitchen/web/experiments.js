const stages = { prepare: 'Preparation', bake: 'Baking', pack: 'Packaging' };
const eventLabels = {
  delivered: 'Delivery received', redelivered: 'Real redelivery', started: 'Processing started',
  processing_completed: 'Processing completed', effect_recorded: 'Effect committed',
  duplicate_skipped: 'Duplicate skipped', handoff_confirmed: 'Next publication confirmed',
  stage_completed: 'Stage completed', ready: 'Ready pizza', ack_sent: 'ACK sent (unconfirmed)',
  retry_scheduled: 'Retry scheduled', failed: 'Permanently failed', reject_sent: 'Dead-letter reject sent',
  crash_triggered: 'One-shot crash fired', requeue_sent: 'Requeue requested',
};
const stageNodes = new Map();
const workerNodes = new Map();
const metricNodes = new Map();
const historyNodes = new Map();
const pending = new Set();
let snapshot = null;
let enabled = false;
let apiAvailable = false;
let historyOrder = null;
let historyBusy = false;
const byId = id => document.getElementById(id);

function node(tag, className = '', text = '') {
  const element = document.createElement(tag);
  element.className = className;
  element.textContent = text;
  return element;
}

function notify(text, error = false) {
  byId('request-notice').textContent = text;
  byId('request-notice').classList.toggle('error', error);
}

async function call(path, body) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), path === '/api/reset' ? 45000 : 10000);
  try {
    const response = await fetch(path, {
      method: body === undefined ? 'GET' : 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: controller.signal,
      cache: 'no-store',
    });
    const result = await response.json();
    if (!response.ok) {
      const detail = result.detail || result.error;
      const message = Array.isArray(detail) ? detail.map(item => item.msg).join('; ') : detail;
      const error = new Error(message || `Request failed (${response.status})`);
      error.status = response.status;
      throw error;
    }
    return result;
  } finally {
    clearTimeout(timer);
  }
}

async function mutate(button, path, body, message) {
  if (pending.has(button)) return;
  pending.add(button);
  button.disabled = true;
  try {
    const result = await call(path, body);
    if (path === '/api/settings' && result.session_id === snapshot?.session_id) {
      snapshot = { ...snapshot, control: result };
    }
    notify(message);
    return result;
  } catch (error) {
    const uncertainty = error.status ? '' : ' The outcome may be uncertain; inspect live state before retrying.';
    notify(`${error.message}.${uncertainty}`, true);
  } finally {
    pending.delete(button);
    if (snapshot) renderExperiments(snapshot);
  }
}

function makeStage(stage) {
  const root = node('section', 'stage-control');
  root.dataset.stage = stage;
  const heading = node('div', 'stage-heading');
  heading.append(node('h3', '', stages[stage]));
  const add = node('button', '', '+ Worker');
  add.dataset.action = 'add-worker';
  add.setAttribute('aria-label', `Add ${stages[stage].toLowerCase()} worker`);
  add.addEventListener('click', () => mutate(add, '/api/workers', { stage }, 'Parallel worker requested.'));
  heading.append(add);
  const label = node('label', 'time-control', 'Time ');
  label.title = 'Applies to newly started tasks';
  const output = node('output');
  const slider = node('input');
  slider.type = 'range';
  slider.min = '.2';
  slider.max = '20';
  slider.step = '.2';
  slider.value = stage === 'bake' ? '8' : '2';
  slider.dataset.stage = stage;
  slider.setAttribute('aria-label', `${stages[stage]} processing seconds`);
  slider.addEventListener('input', () => { output.textContent = `${Number(slider.value).toFixed(1)}s`; });
  slider.addEventListener('change', () => mutate(slider, '/api/settings', {
    durations: { [stage]: Number(slider.value) },
  }, 'Time updated.'));
  label.append(output, slider);
  const workers = node('div', 'worker-controls');
  root.append(heading, label, workers);
  byId('stage-controls').append(root);
  return { slider, output, add, workers };
}

function makeWorker(worker) {
  const id = worker.worker_id;
  const root = node('div', 'worker-control');
  root.dataset.workerId = id;
  const heading = node('div', 'worker-heading');
  const state = node('span', 'worker-state');
  const power = node('button');
  power.dataset.action = 'power';
  power.addEventListener('click', () => {
    const current = snapshot.workers.find(item => item.worker_id === id);
    const action = current.process_running ? 'stop' : 'start';
    mutate(power, `/api/workers/${id}`, { action }, action === 'stop' ? 'Worker stopped.' : 'Worker start requested.');
  });
  heading.append(node('strong', '', id), state, power);
  const remove = node('button', '', 'Retire');
  remove.dataset.action = 'retire';
  remove.title = 'Finish the current task, then gracefully remove this worker';
  remove.setAttribute('aria-label', `Retire ${id}`);
  remove.addEventListener('click', () => mutate(remove, `/api/workers/${id}`, { action: 'remove' }, 'Worker retiring after current task.'));
  const point = node('select');
  point.setAttribute('aria-label', `${id} one-shot crash point`);
  for (const [value, text] of [['crash_during', 'During processing'], ['crash_after_effect', 'After effect, before ACK']]) {
    const option = node('option', '', text);
    option.value = value;
    point.append(option);
  }
  const arm = node('button', '', 'Arm once');
  arm.dataset.action = 'arm';
  arm.setAttribute('aria-label', `Arm one-shot crash for ${id}`);
  arm.addEventListener('click', () => mutate(arm, `/api/workers/${id}`, { action: point.value }, 'One-shot crash armed; the trigger clears before the process exits.'));
  const cancel = node('button', '', 'Cancel crash');
  cancel.setAttribute('aria-label', `Cancel crash for ${id}`);
  cancel.addEventListener('click', () => mutate(cancel, `/api/workers/${id}`, { action: 'cancel_crash' }, 'Crash trigger canceled.'));
  const armed = node('span', 'armed-state');
  const buttons = node('div', 'worker-actions');
  buttons.append(remove);
  const crash = node('div', 'crash-controls');
  crash.append(point, arm, cancel, armed);
  const options = node('details', 'worker-options');
  const summary = node('summary', '', 'More options');
  summary.setAttribute('aria-label', `More options for ${id}`);
  options.append(summary, buttons, crash);
  root.append(heading, options);
  return { root, state, power, remove, point, arm, cancel, armed, options };
}

export function setExperimentConnection(value, reachable = value) {
  if (enabled === value && apiAvailable === reachable) return;
  enabled = value;
  apiAvailable = reachable;
  if (snapshot) renderExperiments(snapshot);
}

export function renderExperiments(value) {
  if (snapshot && snapshot.session_id !== value.session_id) {
    metricNodes.forEach(row => row.remove());
    metricNodes.clear();
    historyOrder = null;
    historyNodes.clear();
    byId('order-kind').value = 'none';
    byId('order-history').close();
  }
  snapshot = value;
  for (const stage of Object.keys(stages)) {
    let group = stageNodes.get(stage);
    if (!group) {
      group = makeStage(stage);
      stageNodes.set(stage, group);
    }
    const seconds = value.control?.durations?.[stage] ?? (stage === 'bake' ? 8 : 2);
    if (document.activeElement !== group.slider && !pending.has(group.slider)) group.slider.value = seconds;
    group.output.textContent = `${Number(group.slider.value).toFixed(1)}s`;
    group.slider.disabled = !enabled || pending.has(group.slider);
    group.add.disabled = !enabled || !value.supervisor_connected || pending.has(group.add)
      || value.workers.filter(worker => worker.stage === stage && !worker.removed).length >= 4;
  }
  const current = new Set();
  for (const worker of value.workers.filter(item => !item.removed)) {
    current.add(worker.worker_id);
    let row = workerNodes.get(worker.worker_id);
    if (!row) {
      row = makeWorker(worker);
      workerNodes.set(worker.worker_id, row);
      stageNodes.get(worker.stage).workers.append(row.root);
    }
    const active = value.orders.find(order => order.order_id === worker.order_id);
    row.state.textContent = (worker.display_state || 'offline') + (worker.retiring ? ' · retiring' : active ? ` · ${active.label}` : '');
    row.state.dataset.state = worker.display_state;
    row.power.textContent = worker.process_running ? 'Stop' : 'Start';
    row.power.setAttribute('aria-label', `${row.power.textContent} ${worker.worker_id}`);
    row.power.title = worker.process_running
      ? 'Immediately kills this Python process; unacknowledged work is eligible for real redelivery'
      : 'Start a real worker process';
    for (const button of [row.power, row.remove, row.arm, row.cancel]) {
      button.disabled = !enabled || !value.supervisor_connected || pending.has(button);
    }
    row.remove.disabled ||= worker.retiring;
    row.arm.disabled ||= !worker.process_running || Boolean(worker.crash_token) || worker.retiring;
    row.point.disabled = !enabled || Boolean(worker.crash_token) || worker.retiring;
    row.cancel.hidden = !worker.crash_token;
    if (worker.crash_token && worker.crash_token !== row.lastCrashToken) row.options.open = true;
    row.lastCrashToken = worker.crash_token;
    row.armed.hidden = !worker.crash_token;
    row.armed.textContent = worker.crash_token
      ? `Armed: ${worker.crash_point === 'during' ? 'during processing' : 'after effect, before ACK'}` : '';
  }
  for (const [id, row] of workerNodes) {
    if (!current.has(id)) {
      row.root.remove();
      workerNodes.delete(id);
    }
  }
  const idempotency = byId('idempotency-mode');
  idempotency.disabled = !enabled || pending.has(idempotency);
  if (!pending.has(idempotency)) idempotency.checked = Boolean(value.control?.idempotency);
  const reset = byId('reset-demo');
  reset.disabled = !apiAvailable || !value.supervisor_connected || pending.has(reset);
  reset.textContent = pending.has(reset) ? 'Resetting…' : 'Reset demo';
  byId('effect-totals').textContent = 'Stage effects · ' + Object.entries(stages)
    .map(([key, label]) => `${label} ${value.effects?.find(effect => effect.stage === key)?.count || 0}`).join(' · ');
  const metrics = value.broker_metrics;
  byId('metrics-status').textContent = metrics?.connected
    ? `Updated ${new Date(metrics.sampled_at * 1000).toLocaleTimeString()}` : 'Disconnected / stale broker metrics';
  if (metrics?.connected) {
    const names = new Set(metrics.queues.map(queue => queue.name));
    for (const [name, row] of metricNodes) {
      if (!names.has(name)) {
        row.remove();
        metricNodes.delete(name);
      }
    }
  }
  for (const queue of metrics?.queues || []) {
    let row = metricNodes.get(queue.name);
    if (!row) {
      row = node('tr');
      for (let index = 0; index < 4; index++) row.append(node('td'));
      metricNodes.set(queue.name, row);
      byId('metrics-rows').append(row);
    }
    [queue.name, queue.ready_messages, queue.unacknowledged, queue.consumers]
      .forEach((text, index) => { row.children[index].textContent = text; });
  }
  if (!enabled) byId('repeat-order').disabled = true;
  if (historyOrder && byId('order-history').open) refreshHistory();
}

async function refreshHistory() {
  if (historyBusy || !enabled) return;
  historyBusy = true;
  const requested = historyOrder;
  const session = snapshot.session_id;
  const isCurrent = () => requested === historyOrder && session === snapshot.session_id && byId('order-history').open;
  try {
    const data = await call(`/api/orders/${requested}/history`);
    if (!isCurrent()) return;
    byId('history-title').textContent = `Order ${data.order.label} · timeline`;
    byId('history-effects').textContent = 'Recorded effects: ' + Object.entries(stages)
      .map(([stage, label]) => `${label} ${data.effects.filter(effect => effect.stage === stage).length}`).join(' · ')
      + ` · idempotency ${snapshot.control.idempotency ? 'ON' : 'OFF'}`;
    if (data.order.history_unavailable) byId('history-effects').textContent = 'Imported order: earlier history unavailable. Submit a new order to replay.';
    byId('repeat-order').disabled = data.order.category !== 'ready' || data.order.history_unavailable || !enabled || pending.has(byId('repeat-order'));
    for (const event of data.events) {
      if (historyNodes.has(event.event_id)) continue;
      const row = node('tr');
      row.dataset.eventId = event.event_id;
      for (const text of [
        `${event.stage} #${event.delivery_attempt}\n${event.attempt_id.slice(0, 8)}`,
        new Date(event.timestamp * 1000).toLocaleTimeString(),
        `${stages[event.stage]} / ${event.worker_id}`, eventLabels[event.status] || event.status, event.detail,
      ]) row.append(node('td', '', text));
      historyNodes.set(event.event_id, row);
      byId('history-rows').append(row);
    }
  } catch (error) {
    if (isCurrent()) {
      byId('history-effects').textContent = `Timeline unavailable: ${error.message}`;
      byId('repeat-order').disabled = true;
    }
  } finally {
    historyBusy = false;
  }
}

export function bindExperiments() {
  byId('idempotency-mode').addEventListener('change', event => mutate(
    event.target, '/api/settings', { idempotency: event.target.checked }, 'Idempotency updated.',
  ));
  document.addEventListener('click', event => {
    const item = event.target.closest('[data-order-id]');
    if (!item || !enabled) return;
    historyOrder = item.dataset.orderId;
    historyNodes.clear();
    byId('history-rows').replaceChildren();
    byId('history-title').textContent = 'Loading order timeline…';
    byId('history-effects').textContent = '';
    byId('repeat-order').disabled = true;
    if (!byId('order-history').open) byId('order-history').showModal();
    refreshHistory();
  });
  byId('close-history').addEventListener('click', () => byId('order-history').close());
  byId('order-history').addEventListener('close', () => {
    historyOrder = null;
    byId('repeat-order').disabled = true;
  });
  byId('repeat-order').addEventListener('click', () => mutate(
    byId('repeat-order'), `/api/orders/${historyOrder}/redeliver`, { stage: byId('repeat-stage').value },
    'Repeated identity confirmed into RabbitMQ. Watch for actual redelivery and stage effects in the timeline.',
  ));
  byId('reset-demo').addEventListener('click', async () => {
    const button = byId('reset-demo');
    if (!snapshot || button.disabled || pending.has(button)) return;
    const sessionId = snapshot.session_id;
    button.textContent = 'Resetting…';
    notify('Resetting… Finishing active work.');
    const result = await mutate(button, '/api/reset', { session_id: sessionId, confirmation: 'RESET' }, 'Demo reset.');
    if (result) {
      byId('order-history').close();
      historyOrder = null;
      historyNodes.clear();
      metricNodes.forEach(row => row.remove());
      metricNodes.clear();
    }
  });
}
