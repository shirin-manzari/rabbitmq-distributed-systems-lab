import { render, renderEstimates } from './render.js';
import { bindExperiments, renderExperiments, setExperimentConnection } from './experiments.js';

// Live is the default. Fixtures are an explicit, separate development adapter.
const mode = new URLSearchParams(location.search).get('mode') || 'live';
const buttons = [...document.querySelectorAll('.controls button')];
const connectionNotice = document.getElementById('connection-notice');
const notice = document.getElementById('request-notice');
let connected = false;
let serverOffset = 0;
let lastSnapshot = null;
let controlState = null;
const pending = new Set();

function updateButtons() {
  for (const button of buttons.slice(0, 2)) {
    button.disabled = mode !== 'live' || !connected || controlState?.resetting || pending.has(button);
    button.title = connected ? '' : 'Requires a connected API and RabbitMQ display consumer';
  }
  buttons[2].disabled = mode !== 'live' || !connected || !controlState || controlState.resetting || pending.has(buttons[2]);
  buttons[2].title = connected ? (controlState?.paused?'Allow workers to admit queued tasks':'Stop admitting new work; active tasks finish normally') : 'Requires a connected API and RabbitMQ';
  buttons[2].textContent = controlState?.status === 'pausing' ? 'Pausing…' : controlState?.paused ? '▶ Resume' : 'Ⅱ Pause';
}

function applyControl(control) {
  if (!control || (controlState?.session_id === control.session_id && control.revision < controlState.revision)) return;
  controlState = control;
  document.body.dataset.controlStatus = control.status;

  updateButtons();
}

async function request(url, options = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), options.method ? 20000 : 2500);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal, cache: 'no-store' });
    // Include body reads in the timeout, not only arrival of HTTP headers.
    return { ok: response.ok, status: response.status, body: await response.json() };
  } finally {
    clearTimeout(timer);
  }
}

function setConnection(healthy, message, apiAvailable=false) {
  connected = healthy;
  document.title = healthy ? 'Distributed Kitchen · RabbitMQ Lab' : 'Disconnected · Distributed Kitchen';
  setExperimentConnection(mode==='live'&&healthy&&!controlState?.resetting,mode==='live'&&apiAvailable);
  const visibleMessage = healthy ? (controlState?.status === 'paused' ? 'Paused' : '') : message;
  if (connectionNotice.textContent !== visibleMessage) connectionNotice.textContent = visibleMessage;
  connectionNotice.hidden = !visibleMessage;
  document.querySelectorAll('.tray, .order-action').forEach(button => { button.disabled = !healthy || controlState?.resetting; });
  document.body.classList.toggle('disconnected', !healthy);

  updateButtons();
}

async function poll() {
  const started = performance.now();
  try {
    const response = await request('/api/state');
    if (!response.ok) throw new Error(`API returned ${response.status}`);
    const snapshot = response.body;
    applyControl(snapshot.control);
    render(snapshot);
    renderExperiments(snapshot);
    lastSnapshot = snapshot;
    serverOffset = snapshot.server_time * 1000 - Date.now();
    setConnection(snapshot.events_connected, 'RabbitMQ display consumer disconnected · state may be stale', true);
  } catch (error) {
    console.warn('Snapshot update failed', error);
    setConnection(false, lastSnapshot ? 'API disconnected · showing the last snapshot' : 'API unavailable · no live snapshot');
  } finally {
    // Schedule only after completion: one request at a time, no retry chain.
    setTimeout(poll, Math.max(500 - (performance.now() - started), 100));
  }
}

async function submit(button, count) {
  pending.add(button);
  updateButtons();
  notice.textContent = `Publishing ${count === 1 ? 'one order' : '20 distinct orders'}…`;
  notice.classList.remove('error');
  try {
    const response = await request('/api/orders', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ count, failure_kind:document.getElementById('order-kind').value }),
    });
    const result = response.body;
    const accepted = result.accepted?.length || 0;
    if (!response.ok || accepted !== count) {
      notice.classList.add('error');
      notice.textContent = `${accepted}/${count} confirmed. ${result.error || 'Publication failed.'} ${result.unconfirmed_order_ids?.length || 0} unconfirmed; ${result.not_attempted ?? count - accepted} not attempted.`;
    } else {
      notice.textContent = `${accepted} ${accepted === 1 ? 'order' : 'orders'} accepted.`;
    }
  } catch {
    notice.classList.add('error');
    notice.textContent = 'No submission result received. Some orders may have reached RabbitMQ. Do not automatically retry; inspect the order list.';
  } finally {
    pending.delete(button);
    updateButtons();
  }
}

async function togglePause() {
  const button = buttons[2];
  const paused = !controlState.paused;
  pending.add(button);
  updateButtons();
  notice.classList.remove('error');
  try {
    const response = await request('/api/control', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ paused }),
    });
    if (!response.ok) throw new Error('Control request rejected');
    applyControl(response.body);
    notice.textContent = paused ? 'Pause requested. Active pizzas finish.' : 'Running.';
  } catch {
    notice.classList.add('error');
    notice.textContent = 'No control result received. The request may have applied; inspect the polled kitchen status before retrying.';
  } finally {
    pending.delete(button);
    updateButtons();
  }
}

if (mode === 'fixture') {
  document.title = 'Development fixture · Distributed Kitchen';
  const { fixtureOrders } = await import('./fixtures.js');
  const stages = { Prep: 'prepare', Bake: 'bake', Pack: 'pack', Ready: 'pack' };
  const orders = fixtureOrders.map(order => ({ ...order, stage: stages[order.stage], worker_id: order.worker }));
  render({ orders, workers: orders.filter(o => o.worker_id).map(o => ({ worker_id: o.worker_id, order_id: o.order_id, status: 'busy' })) });
  connectionNotice.textContent = 'Layout preview · sample orders';
} else if (mode === 'shell') {
  document.title = 'Empty shell · Distributed Kitchen';
  render({ orders: [], workers: [] });
  connectionNotice.textContent = 'Layout preview · empty kitchen';
} else if (mode === 'live') {
  buttons[0].addEventListener('click', () => submit(buttons[0], 1));
  buttons[1].addEventListener('click', () => submit(buttons[1], 20));
  buttons[2].addEventListener('click', togglePause);
  bindExperiments();
  poll();
  // Update estimates only; backend snapshots alone advance orders.
  setInterval(() => {
    if (connected && lastSnapshot) renderEstimates(lastSnapshot.orders, Date.now() + serverOffset);
  }, 100);
} else {
  connectionNotice.textContent = 'Unknown mode · choose live, fixture, or shell';
}
updateButtons();
