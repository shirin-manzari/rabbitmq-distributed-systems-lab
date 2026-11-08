const rows = new Map();
const trays = new Map();
const badges = new Map();
const categories = ['queued', 'cooking', 'ready', 'failed'];
const workerLabels = { prep: 'Prep 1', 'baker-1': 'Baker 1', 'baker-2': 'Baker 2', pack: 'Pack 1' };
const stageLabels = { prepare: 'Prep', bake: 'Bake', pack: 'Pack' };
const stations = { prep: '.prep-station', 'baker-1': '.oven-one', 'baker-2': '.oven-two', pack: '.pack-station' };
const baselineWorkers = Object.keys(stations);
const positions = { prep: [332, 180], 'baker-1': [645, 105], 'baker-2': [645, 243], pack: [976, 180] };

function createRow(order) {
  const row = document.createElement('li');
  row.id = `order-${order.order_id}`;
  row.dataset.orderId = order.order_id;
  const action = document.createElement('button');
  action.type = 'button';
  action.className = 'order-action';
  action.disabled = true;
  action.setAttribute('aria-label', `Open timeline for ${order.label}`);
  const name = document.createElement('span');
  name.className = 'order-name';
  const dot = document.createElement('span');
  dot.className = 'list-dot';
  dot.setAttribute('aria-hidden', 'true');
  const label = document.createElement('span');
  label.className = 'order-label';
  name.append(dot, label);
  const stage = document.createElement('span');
  stage.className = 'order-stage';
  action.append(name, stage);
  row.append(action);
  return row;
}

function highlightHandoff(points, duration) {
  const scene = document.querySelector('.scene');
  for (let index = 1; index < points.length; index++) {
    const [ax, ay] = points[index - 1];
    const [bx, by] = points[index];
    const length = Math.hypot(bx - ax, by - ay);
    if (length < 1) continue;
    const trace = document.createElement('div');
    trace.className = 'handoff-trace';
    trace.setAttribute('aria-hidden', 'true');
    trace.style.left = `${ax + 20}px`;
    trace.style.top = `${ay + 16}px`;
    trace.style.width = `${length}px`;
    trace.style.transform = `rotate(${Math.atan2(by - ay, bx - ax)}rad)`;
    scene.append(trace);
    trace.animate([{opacity:0},{opacity:.8,offset:.15},{opacity:.8,offset:.65},{opacity:0}], {duration})
      .finished.catch(() => {}).finally(() => trace.remove());
  }
}

function moveTray(tray, x, y, handoff) {
  const oldX = parseFloat(tray.style.left);
  const oldY = parseFloat(tray.style.top);
  if (oldX === x && oldY === y) return;
  const transform = new DOMMatrixReadOnly(getComputedStyle(tray).transform);
  const startX = oldX + transform.m41;
  const startY = oldY + transform.m42;
  tray.getAnimations().forEach(animation => animation.cancel());
  tray.style.left = `${x}px`;
  tray.style.top = `${y}px`;
  if (!Number.isFinite(startX) || matchMedia('(prefers-reduced-motion: reduce)').matches) return;
  const points = [[startX, startY]];
  // Follow the shared fork and merge when changing oven lanes.
  if (Math.abs(startY - y) > 8) {
    const junctionX = x < 400 ? 209 : startX < 400 ? 401
      : x < 790 ? 559 : startX < 790 ? 799 : x < 1040 ? 853 : 1045;
    points.push([junctionX, startY], [junctionX, y]);
  }
  points.push([x, y]);
  const lengths = points.slice(1).map((point, index) => Math.hypot(point[0] - points[index][0], point[1] - points[index][1]));
  const distance = lengths.reduce((total, length) => total + length, 0);
  let covered = 0;
  const frames = points.map(([px, py], index) => {
    if (index) covered += lengths[index - 1];
    return { transform: `translate(${px - x}px, ${py - y}px)`, offset: distance ? covered / distance : 1 };
  });
  const duration = Math.min(1400, Math.max(450, distance * 2.5));
  if (handoff) highlightHandoff(points, duration);
  tray.animate(frames, { duration, easing: 'linear' });
}

function placeTray(order, x, y, active = false) {
  let tray = trays.get(order.order_id);
  if (!tray) {
    tray = document.createElement('button');
    tray.type = 'button';
    tray.disabled = true;
    tray.id = `tray-${order.order_id}`;
    tray.dataset.orderId = order.order_id;
    const emoji = document.createElement('span');
    emoji.setAttribute('aria-hidden', 'true');
    const label = document.createElement('span');
    label.className = 'tray-label';
    tray.append(emoji, label);
    document.getElementById('trays').append(tray);
    trays.set(order.order_id, tray);
  }
  const boxed = order.category === 'ready';
  tray.className = `tray${boxed ? ' box' : ''}${active ? ' active-tray' : ''}`;
  const routeKey = `${order.category}:${order.stage}:${order.worker_id || ''}`;
  const handoff = Boolean(tray.dataset.routeKey && tray.dataset.routeKey !== routeKey);
  tray.dataset.routeKey = routeKey;
  moveTray(tray, x, y, handoff);
  const awaitingPrep = order.category === 'queued' && order.stage === 'prepare';
  tray.firstElementChild.textContent = boxed ? '📦' : awaitingPrep ? '🫓' : '🍕';
  tray.lastElementChild.textContent = order.label;
  tray.setAttribute('aria-label', `Open timeline for ${order.label}`);
  tray.title = `${order.label} · ${order.category === 'queued' ? 'Waiting for ' : ''}${stageLabels[order.stage]}${order.redelivered ? ' · Redelivered by RabbitMQ' : ''}`;
}

export function render(snapshot) {
  const workers = snapshot.workers || [];
  reconcileStations(workers);
  if(snapshot.control){
    const durations=snapshot.control.durations;
    if(durations){
      document.querySelectorAll('.worker-label span').forEach(span=>span.textContent=`${durations.bake}s / pizza`);
      document.querySelector('.prep-caption span').textContent=`${durations.prepare}s / pizza`;
      document.querySelector('.pack-caption span').textContent=`${durations.pack}s / pizza`;
    }
  }
  const orders = snapshot.orders;
  for (const stage of ['prepare', 'bake', 'pack']) {
    const count = orders.filter(order => order.category === 'queued' && order.stage === stage).length;
    const badge = document.getElementById(`waiting-${stage}`);
    badge.textContent = `Waiting: ${count}`;
    badge.dataset.tickets = Math.min(3, count);
    badge.title = `${count} application orders waiting for ${stageLabels[stage]}. Ticket stack shows up to three; the count includes all orders.`;
  }
  const counts = Object.fromEntries(categories.map(category => [category, orders.filter(o => o.category === category).length]));
  if (new Set(orders.map(o => o.order_id)).size !== orders.length || categories.some(c => snapshot.counts && counts[c] !== snapshot.counts[c]) || Object.values(counts).reduce((a,b) => a+b,0) !== orders.length) {
    throw new Error('Snapshot contains inconsistent order categories or IDs');
  }
  const visibleIds = new Set(orders.map(o => o.order_id));
  for (const [id, row] of rows) {
    if (!visibleIds.has(id)) { row.remove(); rows.delete(id); }
  }
  for (const order of orders) {
    let row = rows.get(order.order_id);
    if (!row) { row = createRow(order); rows.set(order.order_id, row); }
    row.querySelector('.order-label').textContent = order.label + (order.redelivered ? ' ↺' : '') + (order.retry_count ? ` · retry ${order.retry_count}/2` : '');
    row.querySelector('.order-stage').textContent = order.activity_uncertain ? 'Activity unknown' : order.category === 'failed' ? 'Dead letter' : order.category === 'ready' ? '✓ Boxed' : (workerLabels[order.worker_id] || order.worker_id || `→ ${stageLabels[order.stage]}`);
    row.classList.toggle('uncertain', Boolean(order.activity_uncertain));
    row.title = order.activity_uncertain ? 'Last-known active stage. Worker is unavailable; waiting for a real event or redelivery.' : order.redelivered ? 'Actual RabbitMQ redelivery observed' : order.pizza_type || 'Margherita';
    const list = document.getElementById(`${order.category}-list`);
    if (row.parentElement !== list) {
      const focused = row.contains(document.activeElement) ? document.activeElement : null;
      list.append(row);
      focused?.focus({ preventScroll: true });
    }
  }
  for (const category of categories) {
    document.getElementById(`${category}-count`).textContent = counts[category];
    const list = document.getElementById(`${category}-list`);
    const empty = list.querySelector('.empty');
    if (counts[category]) empty?.remove();
    else if (!empty) {
      const placeholder = document.createElement('li');
      placeholder.className = 'empty';
      placeholder.textContent = 'No orders';
      list.append(placeholder);
    }
  }

  for (const [workerId, selector] of Object.entries(stations)) {
    const worker = workers.find(w => w.worker_id === workerId);
    const order = orders.find(o => o.category === 'cooking' && o.order_id === worker?.order_id);
    const unavailable = worker?.availability === 'unavailable' || worker?.broker_connected === false;
    const station = document.querySelector(selector);
    const action = order ? { prepare: 'Preparing', bake: 'Baking', pack: 'Packing' }[order.stage] : '';
    document.getElementById(`${workerId}-order`).textContent = unavailable ? 'Offline' : order ? `${action} ${order.label}` : worker?.status === 'paused' ? 'Paused' : worker?.status === 'idle' ? 'Idle' : worker?.status === 'busy' ? 'Working…' : 'Starting…';
    station.classList.toggle('busy', Boolean(order) && !unavailable);
    station.classList.toggle('unavailable', unavailable);
    station.title = unavailable ? `${workerLabels[workerId]}: heartbeat missing or broker disconnected. Activity is uncertain.` : order ? `${workerLabels[workerId]} · ${order.label}${order.redelivered ? ' · Redelivered' : ''}` : `${workerLabels[workerId]} · ${worker?.status || 'No heartbeat yet'}`;
  }
  const unavailable = workers.filter(w => !w.removed && (w.availability === 'unavailable' || w.broker_connected === false));
  const uncertain = orders.filter(o => o.activity_uncertain).length;
  document.getElementById('worker-notice').textContent = unavailable.length
    ? `Unavailable: ${unavailable.map(w => workerLabels[w.worker_id]).join(', ')}. ${uncertain ? `${uncertain} last-known active ${uncertain === 1 ? 'order has' : 'orders have'} uncertain activity; waiting for worker events.` : 'Available workers continue when running.'}` : '';

  const shown = new Set();
  const shownBadges = new Set();
  const groups = [
    { key: 'prepare', items: orders.filter(o => o.category === 'queued' && o.stage === 'prepare'), x: 51, gap: 57, cap: 2 },
    { key: 'bake', items: orders.filter(o => o.category === 'queued' && o.stage === 'bake'), x: 425, gap: 51, cap: 2 },
    { key: 'pack', items: orders.filter(o => o.category === 'queued' && o.stage === 'pack'), x: 837, gap: 0, cap: 1 },
    { key: 'ready', items: orders.filter(o => o.category === 'ready'), x: 1060, gap: 0, cap: 1 },
  ];
  for (const group of groups) {
    group.items.slice(0, group.cap).forEach((order, index) => {
      shown.add(order.order_id);
      placeTray(order, group.x + index * group.gap, 183);
    });
    const overflow = group.items.length - group.cap;
    if (overflow > 0) {
      let badge = badges.get(group.key);
      if (!badge) {
        badge = document.createElement('span');
        badge.id = `overflow-${group.key}`;
        badge.className = 'overflow';
        badges.set(group.key, badge);
        document.getElementById('trays').append(badge);
      }
      badge.style.left = `${group.x + (group.cap - 1) * group.gap + 37}px`;
      badge.textContent = `+${overflow}`;
      badge.title = `${overflow} more orders; every order remains in the list below`;
      shownBadges.add(group.key);
    }
  }
  // Show at most one active tray per physical station.
  const usedWorkers = new Set();
  for (const order of orders.filter(o => o.category === 'cooking')) {
    const position = positions[order.worker_id];
    if (position && !usedWorkers.has(order.worker_id)) {
      shown.add(order.order_id);
      usedWorkers.add(order.worker_id);
      placeTray(order, ...position, true);
      trays.get(order.order_id).classList.toggle('uncertain', Boolean(order.activity_uncertain));
    }
  }
  for (const [id, tray] of trays) {
    if (!shown.has(id)) { tray.remove(); trays.delete(id); }
  }
  for (const [key, badge] of badges) {
    if (!shownBadges.has(key)) { badge.remove(); badges.delete(key); }
  }
  renderEstimates(orders, snapshot.server_time ? snapshot.server_time * 1000 : 0);
}

const extras = new Map();
function reconcileStations(workers) {
  const scene = document.querySelector('.scene');
  const current = new Set();
  let height = 358;
  for (const stage of ['prepare', 'bake', 'pack']) {
    const added = workers.filter(worker => worker.stage === stage && !worker.removed
      && !baselineWorkers.includes(worker.worker_id));
    for (const [offset, worker] of added.entries()) {
      current.add(worker.worker_id);
      const baking = stage === 'bake';
      const left = stage === 'prepare' ? 231 : baking ? 626 : 875;
      const top = baking ? 84 + (2 + offset) * 138 : 145 + (1 + offset) * 155;
      let extra = extras.get(worker.worker_id);
      if (!extra) {
        const source = document.querySelector(stage === 'prepare' ? '.prep-station' : baking ? '.oven-one' : '.pack-station');
        const station = source.cloneNode(true);
        station.classList.remove('prep-station', 'pack-station', 'oven-one', 'oven-two', 'retired-station');
        if (baking) station.querySelector('.oven-controls > span').textContent = worker.worker_id.split('-').at(-1).padStart(2, '0');
        else station.classList.add('extra-mint');
        station.id = `station-${worker.worker_id}`;
        station.querySelector('[id$="-order"]').id = `${worker.worker_id}-order`;
        if (!baking) station.querySelector('.station-top > span').textContent = worker.worker_id.toUpperCase();
        const label = document.createElement('div');
        label.className = 'extra-label';
        label.textContent = worker.worker_id.toUpperCase();
        const belt = document.createElement('div');
        belt.className = 'belt extra-lane';
        scene.append(belt, station, label);
        extra = { station, label, belt };
        extras.set(worker.worker_id, extra);
      }
      extra.station.style.left = `${left}px`;
      extra.station.style.top = `${top}px`;
      extra.label.style.left = `${left}px`;
      extra.label.style.top = `${top - 18}px`;
      extra.belt.style.left = `${baking ? 561 : left - 22}px`;
      extra.belt.style.top = `${top + (baking ? 36 : 45)}px`;
      extra.belt.style.width = baking ? '276px' : '200px';
      stations[worker.worker_id] = `#station-${worker.worker_id}`;
      positions[worker.worker_id] = [left + (baking ? 19 : 101), top + (baking ? 21 : 35)];
      workerLabels[worker.worker_id] = worker.worker_id.toUpperCase();
      height = Math.max(height, top + (baking ? 150 : 185));
    }
    if (stage === 'bake') {
      document.querySelectorAll('.split-belt,.merge-belt').forEach(belt => {
        belt.style.height = `${37 + (1 + added.length) * 138}px`;
      });
    } else {
      document.querySelector(stage === 'prepare' ? '.prep-caption' : '.pack-caption').style.top = `${294 + added.length * 155}px`;
      for (const side of ['left', 'right']) {
        const id = `parallel-${stage}-${side}`;
        let connector = document.getElementById(id);
        if (!connector) {
          connector = document.createElement('div');
          connector.id = id;
          connector.className = 'belt parallel-connector';
          scene.append(connector);
        }
        connector.hidden = added.length === 0;
        connector.style.left = `${(stage === 'prepare' ? 231 : 875) + (side === 'left' ? -22 : 170)}px`;
        connector.style.height = `${added.length * 155 + 35}px`;
      }
    }
  }
  for (const [id, extra] of extras) {
    if (!current.has(id)) {
      extra.station.remove();
      extra.label.remove();
      extra.belt.remove();
      extras.delete(id);
      delete stations[id];
      delete positions[id];
      delete workerLabels[id];
    }
  }
  for (const id of baselineWorkers) {
    const removed = Boolean(workers.find(worker => worker.worker_id === id)?.removed);
    document.querySelector(stations[id]).classList.toggle('retired-station', removed);
    if (id === 'baker-1' || id === 'baker-2') {
      document.querySelector(id === 'baker-1' ? '.baker-one-label' : '.baker-two-label').style.visibility = removed ? 'hidden' : '';
    }
  }
  scene.style.height = `${height}px`;
}

export function renderEstimates(orders, now) {
  for (const [workerId, selector] of Object.entries(stations)) {
    const bar = document.querySelector(`${selector} .estimate > span`);
    if (!bar) continue;
    const order = orders.find(o => o.category === 'cooking' && o.worker_id === workerId);
    if (order?.activity_uncertain) continue; // Freeze the last estimate, do not imply activity.
    const estimate = order?.started_at && order?.duration_s ? Math.min(95, Math.max(0, (now / 1000 - order.started_at) / order.duration_s * 100)) : 0;
    bar.style.width = `${estimate}%`;
    bar.parentElement.title = 'Time estimate only. Waiting for backend confirmation to complete.';
  }
}
