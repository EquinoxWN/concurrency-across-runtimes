import assert from 'node:assert/strict';
import { test } from 'node:test';
import { SingleFlightCache } from '../src/cache.js';
import { scenarios } from './scenarios.js';

const sleep = (/** @type {number} */ ms) => new Promise((r) => setTimeout(r, ms));

test('concurrent misses share one load', async () => {
  const cfg = scenarios.cache;
  /** @type {SingleFlightCache<string, object>} */
  const cache = new SingleFlightCache(cfg.capacity, async () => {
    await sleep(cfg.load_ms);
    return {};
  });
  const values = await Promise.all(Array.from({ length: cfg.concurrent_requests }, () => cache.get('report')));
  assert.equal(cache.loads, 1);
  assert.equal(new Set(values).size, 1, 'every caller got the same object');
});

test('different keys load concurrently', async () => {
  /** @type {() => void} */
  let bStarted = () => {};
  const started = new Promise((r) => {
    bStarted = () => r(undefined);
  });
  /** @type {SingleFlightCache<string, string>} */
  const cache = new SingleFlightCache(3, async (key) => {
    if (key === 'a') {
      const ok = await Promise.race([started.then(() => true), sleep(5000).then(() => false)]);
      if (!ok) throw new Error('b never started while a was loading');
    } else {
      bStarted();
    }
    return key.toUpperCase();
  });
  assert.deepEqual(await Promise.all([cache.get('a'), cache.get('b')]), ['A', 'B']);
});

test('a failed load is not cached and every waiter sees the error', async () => {
  let attempts = 0;
  /** @type {SingleFlightCache<string, string>} */
  const cache = new SingleFlightCache(3, async () => {
    attempts++;
    await sleep(10);
    if (attempts === 1) throw new Error('database down');
    return 'ok';
  });
  const first = await Promise.allSettled(Array.from({ length: 8 }, () => cache.get('k')));
  assert.equal(cache.loads, 1);
  assert.ok(first.every((r) => r.status === 'rejected'));
  assert.equal(await cache.get('k'), 'ok', 'the next request retries');
});

test('the least recently used key is evicted', async () => {
  const capacity = scenarios.cache.capacity;
  /** @type {SingleFlightCache<string, string>} */
  const cache = new SingleFlightCache(capacity, async (k) => `${k}!`);
  for (const k of ['a', 'b', 'c', 'a', 'd']) await cache.get(k);
  assert.deepEqual(cache.keys(), ['c', 'a', 'd']);
  assert.equal(cache.loads, 4);
  await cache.get('b');
  assert.equal(cache.loads, 5);
  assert.equal(cache.keys().length, capacity);
});

test('missing values and bad capacity are rejected', async () => {
  await assert.rejects(new SingleFlightCache(1, async () => undefined).get('x'), TypeError);
  assert.throws(() => new SingleFlightCache(0, async (k) => k), RangeError);
});
