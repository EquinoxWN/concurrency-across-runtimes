import { Worker } from 'node:worker_threads';
import { AsyncBoundedBuffer, ClosedError } from './buffer.js';

/** @typedef {{id: number, task: string, arg: unknown, resolve: (v: unknown) => void, reject: (e: Error) => void}} Job */

/**
 * A fixed number of worker threads (real parallelism, unlike the event loop) fed from a
 * bounded queue, so submit waits when the queue is full instead of growing without limit.
 */
export class WorkerPool {
  /** @param {number} size @param {number} queueCapacity */
  constructor(size, queueCapacity) {
    if (!Number.isInteger(size) || size < 1) throw new RangeError('need at least one worker');
    /** @type {AsyncBoundedBuffer<Job>} */
    this.queue = new AsyncBoundedBuffer(queueCapacity);
    this.active = 0;
    this.maxActive = 0;
    this.nextId = 0;
    this.workers = Array.from({ length: size }, () => new Worker(new URL('./pool-worker.js', import.meta.url)));
    /** @type {Set<Worker>} */
    this.exited = new Set();
    for (const w of this.workers) w.once('exit', () => this.exited.add(w));
    this.dispatchers = this.workers.map((w) => this.#dispatch(w));
  }

  /**
   * Queue a named task (waiting while the queue is full); the returned job's result settles
   * when a worker finishes it.
   * @param {string} task @param {unknown} arg
   * @returns {Promise<{result: Promise<unknown>}>}
   */
  async submit(task, arg) {
    /** @type {(v: unknown) => void} */
    let resolve = () => {};
    /** @type {(e: Error) => void} */
    let reject = () => {};
    const result = new Promise((res, rej) => {
      resolve = res;
      reject = rej;
    });
    try {
      await this.queue.put({ id: this.nextId++, task, arg, resolve, reject });
    } catch (e) {
      if (e instanceof ClosedError) throw new Error('pool is shut down');
      throw e;
    }
    return { result };
  }

  /** @param {Worker} worker */
  async #dispatch(worker) {
    for (let job = await this.queue.take(); job !== undefined; job = await this.queue.take()) {
      this.active++;
      this.maxActive = Math.max(this.maxActive, this.active);
      const current = job;
      try {
        const reply = await new Promise((resolve, reject) => {
          worker.once('message', resolve);
          worker.once('error', reject);
          worker.postMessage({ id: current.id, task: current.task, arg: current.arg });
        });
        worker.removeAllListeners('error');
        if (reply.ok) current.resolve(reply.value);
        else current.reject(new Error(reply.error));
      } catch (e) {
        current.reject(e instanceof Error ? e : new Error(String(e)));
      } finally {
        this.active--;
      }
    }
  }

  /** Stop accepting tasks, finish the queued ones, then stop every worker thread. */
  async shutdown() {
    this.queue.close();
    await Promise.all(this.dispatchers);
    await Promise.all(this.workers.map((w) => w.terminate()));
  }

  /** True once every worker thread has exited. */
  get terminated() {
    return this.exited.size === this.workers.length;
  }
}
