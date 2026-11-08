// Development-only, immutable layout samples. Never feed these into live state.
const order = (number, category, stage, worker = null) => Object.freeze({
  order_id: `fixture-${number}`, label: `#${number}`, category, stage, worker,
});
export const fixtureOrders = Object.freeze([
  order(1041, 'queued', 'Prep'), order(1042, 'queued', 'Prep'),
  order(1043, 'queued', 'Prep'), order(1036, 'queued', 'Bake'),
  order(1037, 'queued', 'Bake'), order(1038, 'queued', 'Bake'),
  order(1030, 'queued', 'Pack'), order(1031, 'queued', 'Pack'),
  order(1040, 'cooking', 'Prep', 'prep'),
  order(1034, 'cooking', 'Bake', 'baker-1'),
  order(1035, 'cooking', 'Bake', 'baker-2'),
  order(1029, 'cooking', 'Pack', 'pack'),
  ...[1024, 1025, 1026, 1027, 1028].map(id => order(id, 'ready', 'Ready')),
]);
