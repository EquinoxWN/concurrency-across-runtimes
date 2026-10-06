import assert from 'node:assert/strict';
import { test } from 'node:test';
import { AsyncBoundedBuffer, ClosedError } from '../src/buffer.js';
import { checkDelivery, scenarios } from './scenarios.js';

test('every item is consumed exactly once and capacity is never exceeded', async () => {
  const cfg = scenarios.bounded_buffer;
  /** @type {AsyncBoundedBuffer<number>} */
  const buf = new AsyncBoundedBuffer(cfg.capacity);
  /** @type {number[][]} */
  const received = Array.from({ length: cfg.consumers }, () => []);
  const consumers = received.map(async (mine) => {
    for (let x = await buf.take(); x !== undefined; x = await buf.take()) mine.push(x);
  });
  await Promise.all(
    Array.from({ length: cfg.producers }, async (_, p) => {
      for (let s = 0; s < cfg.items_per_producer; s++) await buf.put(p * 1_000_000 + s);
    }),
  );
  buf.close();
  await Promise.all(consumers);
  checkDelivery(received, cfg.producers, cfg.items_per_producer);
  assert.equal(buf.maxObserved, cfg.capacity, 'producers outpace consumers, so the buffer fills');
});

test('close lets consumers drain, then reports empty', async () => {
  /** @type {AsyncBoundedBuffer<string>} */
  const buf = new AsyncBoundedBuffer(3);
  await buf.put('a');
  await buf.put('b');
  buf.close();
  await assert.rejects(buf.put('c'), ClosedError);
  assert.deepEqual([await buf.take(), await buf.take(), await buf.take()], ['a', 'b', undefined]);
});

test('a full buffer suspends the producer until space frees', async () => {
  /** @type {AsyncBoundedBuffer<number>} */
  const buf = new AsyncBoundedBuffer(1);
  await buf.put(1);
  let done = false;
  const pending = buf.put(2).then(() => {
    done = true;
  });
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(done, false, 'put waits while the buffer is full');
  assert.equal(await buf.take(), 1);
  await pending;
  assert.equal(done, true);
  assert.equal(await buf.take(), 2);
});

test('abort wakes waiting producers and consumers and drops items', async () => {
  /** @type {AsyncBoundedBuffer<number>} */
  const full = new AsyncBoundedBuffer(1);
  await full.put(1);
  const blockedPut = full.put(2);
  /** @type {AsyncBoundedBuffer<number>} */
  const empty = new AsyncBoundedBuffer(1);
  const blockedTake = empty.take();
  full.abort();
  empty.abort();
  await assert.rejects(blockedPut, ClosedError);
  assert.equal(await blockedTake, undefined);
  assert.equal(full.size, 0);
});

test('invalid use is rejected', async () => {
  assert.throws(() => new AsyncBoundedBuffer(0), RangeError);
  await assert.rejects(new AsyncBoundedBuffer(1).put(undefined), TypeError);
});
