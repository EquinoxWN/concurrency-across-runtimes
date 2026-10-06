// Runs inside each worker thread: receives {id, task, arg}, replies {id, ok, value | error}.
import { parentPort } from 'node:worker_threads';
import { tasks } from './tasks.js';

if (!parentPort) throw new Error('pool-worker.js must run in a worker thread');
const port = parentPort;

port.on('message', async (/** @type {{id: number, task: string, arg: unknown}} */ msg) => {
  try {
    const fn = tasks[msg.task];
    if (!fn) throw new Error(`unknown task ${msg.task}`);
    port.postMessage({ id: msg.id, ok: true, value: await fn(msg.arg) });
  } catch (e) {
    port.postMessage({ id: msg.id, ok: false, error: e instanceof Error ? e.message : String(e) });
  }
});
