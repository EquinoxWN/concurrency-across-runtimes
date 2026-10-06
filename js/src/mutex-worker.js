// Increments a plain (non-atomic) shared counter under SharedMutex, for the mutual-exclusion test.
import { parentPort, workerData } from 'node:worker_threads';
import { SharedMutex } from './shared-ring.js';

/** @type {{sab: SharedArrayBuffer, iterations: number}} */
const { sab, iterations } = workerData;
const cells = new Int32Array(sab);
const mutex = new SharedMutex(cells, 0);
for (let i = 0; i < iterations; i++) {
  mutex.lock();
  cells[1] = /** @type {number} */ (cells[1]) + 1; // read-modify-write that races without the lock
  mutex.unlock();
}
parentPort?.postMessage({ done: true });
