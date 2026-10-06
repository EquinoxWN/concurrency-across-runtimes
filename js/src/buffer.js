/** Raised by put after the buffer was closed or aborted. */
export class ClosedError extends Error {
  constructor() {
    super('buffer is closed');
    this.name = 'ClosedError';
  }
}

/**
 * Bounded FIFO for one event loop. Waiting suspends a promise, not a thread, and nothing runs
 * between two awaits, so the state needs no lock.
 * @template T
 */
export class AsyncBoundedBuffer {
  /** @param {number} capacity */
  constructor(capacity) {
    if (!Number.isInteger(capacity) || capacity < 1) throw new RangeError('capacity must be at least 1');
    this.capacity = capacity;
    /** @type {T[]} */
    this.items = [];
    /** @type {Array<() => void>} */
    this.putWaiters = [];
    /** @type {Array<() => void>} */
    this.takeWaiters = [];
    this.closed = false;
    this.maxObserved = 0;
  }

  /** Wait for space and add; rejects with ClosedError once closed. @param {T} item */
  async put(item) {
    if (item === undefined) throw new TypeError('undefined cannot be queued; it means "closed and drained"');
    while (this.items.length >= this.capacity && !this.closed) {
      await new Promise((resolve) => this.putWaiters.push(() => resolve(undefined)));
    }
    if (this.closed) throw new ClosedError();
    this.items.push(item);
    this.maxObserved = Math.max(this.maxObserved, this.items.length);
    this.takeWaiters.shift()?.();
  }

  /** Next item; undefined when closed and drained. @returns {Promise<T | undefined>} */
  async take() {
    while (this.items.length === 0 && !this.closed) {
      await new Promise((resolve) => this.takeWaiters.push(() => resolve(undefined)));
    }
    if (this.items.length === 0) return undefined;
    const item = this.items.shift();
    this.putWaiters.shift()?.();
    return item;
  }

  /** Refuse new items; consumers drain the rest, then get undefined. */
  close() {
    this.closed = true;
    this.#wakeAll();
  }

  /** Close and drop everything queued, waking every waiter. */
  abort() {
    this.closed = true;
    this.items = [];
    this.#wakeAll();
  }

  #wakeAll() {
    for (const wake of this.putWaiters.splice(0)) wake();
    for (const wake of this.takeWaiters.splice(0)) wake();
  }

  get size() {
    return this.items.length;
  }
}
