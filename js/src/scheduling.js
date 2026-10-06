// Prints how Node.js schedules blocking and CPU-bound work on the event loop and worker threads.
import { availableParallelism } from 'node:os';
import { setTimeout as sleep } from 'node:timers/promises';
import { WorkerPool } from './pool.js';
import { tasks } from './tasks.js';

const spinN = 120_000_000;
const parallel = Math.min(4, availableParallelism());

console.log(`| Experiment (Node.js ${process.versions.node}, ${availableParallelism()} cores) | Wall time | OS threads used |`);
console.log('|---|---|---|');

let start = performance.now();
await Promise.all(Array.from({ length: 10_000 }, () => sleep(100)));
console.log(`| 10,000 promises sleeping 100 ms on the event loop | ${(performance.now() - start).toFixed(0)} ms | 1 |`);

start = performance.now();
for (let i = 0; i < parallel; i++) tasks.spin?.({ n: spinN });
const sequential = performance.now() - start;

const pool = new WorkerPool(parallel, parallel);
start = performance.now();
const jobs = [];
for (let i = 0; i < parallel; i++) jobs.push(await pool.submit('spin', { n: spinN }));
await Promise.all(jobs.map((j) => j.result));
const threaded = performance.now() - start;
const ids = new Set();
const who = [];
for (let i = 0; i < parallel * 4; i++) who.push(await pool.submit('whoAmI', null));
for (const j of who) ids.add(await j.result);
await pool.shutdown();
console.log(
  `| CPU-bound: ${parallel} busy tasks on ${parallel} worker threads vs one after another on the event loop | ` +
    `${threaded.toFixed(0)} ms vs ${sequential.toFixed(0)} ms (${(sequential / threaded).toFixed(1)}x) | ${ids.size} |`,
);
