import assert from 'node:assert/strict';
import { test } from 'node:test';
import { AsyncMutex, Barrier, dine } from '../src/philosophers.js';
import { scenarios } from './scenarios.js';

const cfg = scenarios.philosophers;

test('naive strategy deadlocks on one thread, and it is detected', async () => {
  const o = await dine(cfg.count, cfg.meals, 'naive', { forceCycle: true, timeoutMs: cfg.timeout_ms });
  assert.equal(o.cycleFormed, true, 'every philosopher held the left fork at once');
  assert.equal(o.deadlocked, true, 'then nobody got the right fork: detected, not hung');
  assert.equal(o.violations, 0);
});

for (const strategy of /** @type {const} */ (['ordered', 'seats'])) {
  test(`${strategy} strategy cannot form the cycle`, async () => {
    const o = await dine(cfg.count, cfg.meals, strategy, { forceCycle: true, timeoutMs: cfg.timeout_ms });
    assert.equal(o.cycleFormed, false);
    assert.equal(o.deadlocked, false);
    assert.deepEqual(o.meals, new Array(cfg.count).fill(cfg.meals));
    assert.equal(o.violations, 0);
  });

  test(`${strategy} strategy survives repeated contention`, async () => {
    for (let round = 0; round < 5; round++) {
      const o = await dine(cfg.count, cfg.meals, strategy, { forceCycle: false, timeoutMs: cfg.timeout_ms });
      assert.equal(o.deadlocked, false, `round ${round}`);
      assert.deepEqual(o.meals, new Array(cfg.count).fill(cfg.meals));
    }
  });
}

test('mutex hands over in order and times out cleanly', async () => {
  const m = new AsyncMutex();
  assert.equal(await m.acquire(), true);
  /** @type {Array<[string, boolean]>} */
  const order = [];
  const second = m.acquire().then((ok) => order.push(['second', ok]));
  const timedOut = await m.acquire(10);
  assert.equal(timedOut, false);
  m.release();
  await second;
  assert.deepEqual(order, [['second', true]]);
  assert.equal(m.waiters.length, 0, 'the timed-out waiter was removed');
});

test('barrier trips when all arrive and breaks on timeout', async () => {
  const b = new Barrier(2);
  assert.deepEqual(await Promise.all([b.wait(1000), b.wait(1000)]), [true, true]);
  const lonely = new Barrier(2);
  assert.equal(await lonely.wait(10), false);
  assert.equal(await lonely.wait(10), false, 'a broken barrier stays broken');
});
