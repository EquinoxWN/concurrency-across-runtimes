import assert from 'node:assert/strict';
import { once } from 'node:events';
import { test } from 'node:test';
import { Worker } from 'node:worker_threads';
import { SharedMutex, SharedRingBuffer } from '../src/shared-ring.js';
import { checkDelivery, scenarios } from './scenarios.js';

const workerUrl = new URL('../src/shared-ring-worker.js', import.meta.url);

/** Run producer and consumer threads over one shared ring; returns what each consumer received. */
async function runThreads(/** @type {{capacity: number, producers: number, consumers: number, items_per_producer: number}} */ cfg) {
  const sab = SharedRingBuffer.allocate(cfg.capacity);
  const ring = new SharedRingBuffer(sab);
  const consumers = Array.from(
    { length: cfg.consumers },
    (_, id) => new Worker(workerUrl, { workerData: { sab, role: 'consumer', id, count: 0 } }),
  );
  // Listen for 'exit' now: a fast worker can exit before a later once() would be attached.
  const consumerExits = consumers.map((w) => once(w, 'exit'));
  const received = consumers.map((w) => once(w, 'message').then(([m]) => /** @type {number[]} */ (m.received)));
  const producers = Array.from(
    { length: cfg.producers },
    (_, id) => new Worker(workerUrl, { workerData: { sab, role: 'producer', id, count: cfg.items_per_producer } }),
  );
  await Promise.all(producers.map((w) => once(w, 'exit')));
  ring.close();
  const lists = await Promise.all(received);
  await Promise.all(consumerExits);
  return { lists, ring };
}

test('producer and consumer threads share one ring buffer through Atomics', async () => {
  const cfg = scenarios.bounded_buffer;
  const { lists, ring } = await runThreads(cfg);
  checkDelivery(lists, cfg.producers, cfg.items_per_producer, 100_000);
  assert.ok(ring.maxObserved <= cfg.capacity, `occupancy ${ring.maxObserved} exceeded capacity`);
  assert.equal(ring.maxObserved, cfg.capacity, 'producers outpace consumers, so the ring fills');
});

test('stress: 8 threads move 200,000 items through a 64-slot ring without loss', async () => {
  const cfg = { capacity: 64, producers: 4, consumers: 4, items_per_producer: 50_000 };
  const { lists } = await runThreads(cfg);
  checkDelivery(lists, cfg.producers, cfg.items_per_producer, 100_000);
});

test('SharedMutex: 4 threads doing non-atomic increments under the lock lose none', async () => {
  const sab = new SharedArrayBuffer(8);
  const iterations = 100_000;
  const url = new URL('../src/mutex-worker.js', import.meta.url);
  const workers = Array.from({ length: 4 }, () => new Worker(url, { workerData: { sab, iterations } }));
  await Promise.all(workers.map((w) => once(w, 'exit')));
  assert.equal(new Int32Array(sab)[1], 4 * iterations);
  const m = new SharedMutex(new Int32Array(sab), 0);
  m.lock();
  m.unlock();
  assert.equal(new Int32Array(sab)[0], 0, 'unlocked after use');
});

test('a single thread sees FIFO order, close drains, then null', () => {
  const ring = new SharedRingBuffer(SharedRingBuffer.allocate(3));
  ring.put(1);
  ring.put(2);
  ring.close();
  assert.throws(() => ring.put(3), /closed/);
  assert.deepEqual([ring.take(), ring.take(), ring.take()], [1, 2, null]);
});

test('invalid capacity is rejected', () => {
  assert.throws(() => SharedRingBuffer.allocate(0), RangeError);
});
