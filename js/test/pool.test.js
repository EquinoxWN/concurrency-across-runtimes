import assert from 'node:assert/strict';
import { test } from 'node:test';
import { WorkerPool } from '../src/pool.js';
import { scenarios } from './scenarios.js';

test('worker threads return every result with bounded concurrency', async () => {
  const cfg = scenarios.worker_pool;
  const pool = new WorkerPool(cfg.workers, cfg.queue_capacity);
  const jobs = [];
  for (let i = 0; i < cfg.tasks; i++) jobs.push(await pool.submit('square', { n: i, ms: cfg.task_ms }));
  const results = await Promise.all(jobs.map((j) => j.result));
  assert.deepEqual(results, Array.from({ length: cfg.tasks }, (_, i) => i * i));
  assert.equal(pool.maxActive, cfg.workers, 'every worker busy, never more');
  await pool.shutdown();
  assert.equal(pool.terminated, true);
});

test('tasks really run on different OS threads', async () => {
  const pool = new WorkerPool(3, 8);
  const jobs = [];
  for (let i = 0; i < 12; i++) jobs.push(await pool.submit('square', { n: 1, ms: 5 }));
  await Promise.all(jobs.map((j) => j.result));
  const who = [];
  for (let i = 0; i < 30; i++) who.push(await pool.submit('whoAmI', null));
  const ids = new Set(await Promise.all(who.map((j) => j.result)));
  await pool.shutdown();
  assert.ok(ids.size >= 2, `expected several thread ids, got ${[...ids]}`);
  assert.ok(![...ids].includes(0), 'thread 0 is the main thread; tasks never run there');
});

test('a failing task rejects only its own result', async () => {
  const pool = new WorkerPool(2, 4);
  const bad = await pool.submit('fail', { message: 'boom' });
  const good = await pool.submit('square', { n: 6 });
  await assert.rejects(bad.result, /boom/);
  assert.equal(await good.result, 36);
  assert.equal(await (await pool.submit('square', { n: 7 })).result, 49, 'the pool keeps working');
  await pool.shutdown();
});

test('shutdown finishes queued tasks, then rejects new ones', async () => {
  const pool = new WorkerPool(2, 32);
  const jobs = [];
  for (let i = 0; i < 20; i++) jobs.push(await pool.submit('square', { n: i, ms: 2 }));
  const shut = pool.shutdown();
  const results = await Promise.all(jobs.map((j) => j.result));
  await shut;
  assert.equal(results.length, 20);
  assert.equal(pool.terminated, true);
  await assert.rejects(pool.submit('square', { n: 1 }), /shut down/);
});

test('submit waits while the queue is full', async () => {
  const pool = new WorkerPool(1, 1);
  await pool.submit('square', { n: 1, ms: 200 });
  await new Promise((r) => setTimeout(r, 20));
  await pool.submit('square', { n: 2 });
  let third = false;
  const pending = pool.submit('square', { n: 3 }).then(() => {
    third = true;
  });
  await new Promise((r) => setTimeout(r, 50));
  assert.equal(third, false, 'submit waits for queue space');
  await pending;
  assert.equal(third, true);
  await pool.shutdown();
});

test('invalid pool size is rejected', () => {
  assert.throws(() => new WorkerPool(0, 1), RangeError);
});
