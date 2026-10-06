// Tasks the worker pool can run; workers look them up by name (functions cannot cross threads).
import { threadId } from 'node:worker_threads';

const sleeper = new Int32Array(new SharedArrayBuffer(4));

/** Block this thread for ms milliseconds (allowed in worker threads). @param {number} ms */
function sleep(ms) {
  Atomics.wait(sleeper, 0, 0, ms);
}

/** @type {Record<string, (arg: any) => unknown>} */
export const tasks = {
  /** n squared after an optional blocking pause. */
  square: ({ n, ms = 0 }) => {
    if (ms > 0) sleep(ms);
    return n * n;
  },
  /** Always fails with the given message. */
  fail: ({ message }) => {
    throw new Error(message);
  },
  /** Busy CPU work; returns a checksum so it cannot be optimised away. */
  spin: ({ n }) => {
    let x = 0;
    for (let i = 0; i < n; i++) x = (x + (i ^ (x >>> 3))) | 0;
    return x;
  },
  /** Id of the OS thread running the task. */
  whoAmI: () => threadId,
};
