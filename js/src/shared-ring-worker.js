// Producer or consumer thread for the SharedRingBuffer test and demo.
import { parentPort, workerData } from 'node:worker_threads';
import { SharedRingBuffer } from './shared-ring.js';

/** @type {{sab: SharedArrayBuffer, role: 'producer' | 'consumer', id: number, count: number}} */
const { sab, role, id, count } = workerData;
const ring = new SharedRingBuffer(sab);
if (role === 'producer') {
  for (let seq = 0; seq < count; seq++) ring.put(id * 100_000 + seq);
  parentPort?.postMessage({ done: true });
} else {
  const received = [];
  for (let x = ring.take(); x !== null; x = ring.take()) received.push(x);
  parentPort?.postMessage({ received });
}
