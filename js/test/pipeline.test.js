import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Pipeline, PipelineError } from '../src/pipeline.js';
import { scenarios } from './scenarios.js';

const cfg = scenarios.pipeline;
const inputs = Array.from({ length: cfg.items }, (_, i) => String(i + 1));
const expected = Array.from({ length: cfg.items }, (_, i) => `${i + 1}^2=${(i + 1) ** 2}`);

let formatted = 0;

/** @param {boolean} ordered @param {number} [failAt] */
function pipeline(ordered, failAt = -1) {
  formatted = 0;
  return new Pipeline(
    [
      { name: 'parse', workers: cfg.workers.parse, fn: (/** @type {string} */ s) => Number.parseInt(s, 10) },
      {
        name: 'square',
        workers: cfg.workers.square,
        fn: async (/** @type {number} */ n) => {
          await Promise.resolve();
          if (n === failAt) throw new Error(`cannot square ${n}`);
          return [n, n * n];
        },
      },
      {
        name: 'format',
        workers: cfg.workers.format,
        fn: (/** @type {number[]} */ p) => {
          formatted++;
          return `${p[0]}^2=${p[1]}`;
        },
      },
    ],
    cfg.queue_capacity,
    { ordered },
  );
}

test('ordered output equals the sequential result', async () => {
  const p = pipeline(true);
  assert.deepEqual(await p.run(inputs), expected);
  assert.equal(p.lastRunTerminated, true);
  for (const max of p.lastMaxOccupancy) assert.ok(max <= cfg.queue_capacity, `buffer over capacity: ${max}`);
});

test('unordered output has the same items', async () => {
  const out = /** @type {string[]} */ (await pipeline(false).run(inputs));
  assert.deepEqual([...out].sort(), [...expected].sort());
});

test('one failure stops every stage and names the item', async () => {
  const p = pipeline(true, cfg.fail_at);
  await assert.rejects(p.run(inputs), (/** @type {PipelineError} */ e) => {
    assert.ok(e instanceof PipelineError);
    assert.equal(e.stage, 'square');
    assert.equal(e.index, cfg.fail_at - 1);
    assert.equal(e.message, `stage 'square' failed on item ${cfg.fail_at - 1}: cannot square ${cfg.fail_at}`);
    return true;
  });
  assert.equal(p.lastRunTerminated, true, 'every worker promise settled');
});

test('a failure early in a long input stops the remaining work', async () => {
  const items = cfg.early_stop_items;
  const p = pipeline(true, cfg.fail_at);
  await assert.rejects(p.run(Array.from({ length: items }, (_, i) => String(i + 1))), PipelineError);
  assert.ok(formatted < items / 2, `${formatted} of ${items} items were still formatted`);
});

test('a slow stage fills the buffers without overflowing them', async () => {
  const p = new Pipeline(
    [
      { name: 'fast', workers: 4, fn: (/** @type {number} */ x) => x + 1 },
      { name: 'slow', workers: 1, fn: async (/** @type {number} */ x) => new Promise((r) => setTimeout(() => r(x), 1)) },
    ],
    4,
    { ordered: true },
  );
  await p.run(Array.from({ length: 100 }, (_, i) => i));
  assert.equal(p.lastMaxOccupancy[1], 4, 'the fast stage is held back by the full buffer');
  for (const m of p.lastMaxOccupancy) assert.ok(m <= 4);
});

test('bad configurations are rejected', async () => {
  assert.throws(() => new Pipeline([], 2, { ordered: true }), RangeError);
  assert.throws(() => new Pipeline([{ name: 'x', workers: 0, fn: (x) => x }], 2, { ordered: true }), RangeError);
  assert.deepEqual(await new Pipeline([{ name: 'x', workers: 1, fn: (x) => x }], 2, { ordered: true }).run([]), []);
});
