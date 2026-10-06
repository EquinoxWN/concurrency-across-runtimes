// Header slots in the shared Int32Array; items follow the header.
const LOCK = 0;
const HEAD = 1;
const TAIL = 2;
const COUNT = 3;
const CLOSED = 4;
const MAX_OBSERVED = 5;
const NOT_EMPTY = 6; // bumped when an item arrives; consumers sleep on it
const NOT_FULL = 7; // bumped when a slot frees; producers sleep on it
const HEADER = 8;

/**
 * Mutex in one cell of shared memory, usable from any thread (Drepper's futex mutex):
 * 0 unlocked, 1 locked, 2 locked with waiters, so unlock only calls notify when needed.
 */
export class SharedMutex {
  /** @param {Int32Array} cells @param {number} index */
  constructor(cells, index) {
    this.cells = cells;
    this.index = index;
  }

  /** Block until this thread holds the lock. */
  lock() {
    let c = Atomics.compareExchange(this.cells, this.index, 0, 1);
    if (c === 0) return;
    if (c !== 2) c = Atomics.exchange(this.cells, this.index, 2);
    while (c !== 0) {
      Atomics.wait(this.cells, this.index, 2);
      c = Atomics.exchange(this.cells, this.index, 2);
    }
  }

  /** Release the lock, waking one waiter if there is one. */
  unlock() {
    if (Atomics.sub(this.cells, this.index, 1) !== 1) {
      Atomics.store(this.cells, this.index, 0);
      Atomics.notify(this.cells, this.index, 1);
    }
  }
}

/**
 * Bounded FIFO of 32-bit integers in a SharedArrayBuffer, usable from several worker threads
 * at once. Mutual exclusion is a futex-style mutex built from Atomics.compareExchange,
 * Atomics.wait and Atomics.notify. Producers and consumers sleep on two separate sequence
 * counters, and each change wakes exactly one thread (like a condition variable's signal)
 * instead of every waiting thread; close wakes everyone.
 */
export class SharedRingBuffer {
  /** Allocate shared memory for a buffer of the given capacity. @param {number} capacity */
  static allocate(capacity) {
    if (!Number.isInteger(capacity) || capacity < 1) throw new RangeError('capacity must be at least 1');
    return new SharedArrayBuffer((HEADER + capacity) * Int32Array.BYTES_PER_ELEMENT);
  }

  /** Attach to shared memory created by allocate (in any thread). @param {SharedArrayBuffer} sab */
  constructor(sab) {
    this.cells = new Int32Array(sab);
    this.capacity = this.cells.length - HEADER;
    this.mutex = new SharedMutex(this.cells, LOCK);
  }

  #lock() {
    this.mutex.lock();
  }

  #unlock() {
    this.mutex.unlock();
  }

  /** Release the lock, sleep until the counter moves, then take the lock again. @param {number} counter */
  #await(counter) {
    const seen = Atomics.load(this.cells, counter);
    this.#unlock();
    Atomics.wait(this.cells, counter, seen);
    this.#lock();
  }

  /** Bump a counter and wake waiters on it (one, or all on close). @param {number} counter @param {number} [count] */
  #signal(counter, count = 1) {
    Atomics.add(this.cells, counter, 1);
    Atomics.notify(this.cells, counter, count);
  }

  /** Block until there is space, then add; throws once closed. @param {number} value */
  put(value) {
    this.#lock();
    try {
      while (this.cells[COUNT] === this.capacity && this.cells[CLOSED] === 0) this.#await(NOT_FULL);
      if (this.cells[CLOSED] !== 0) throw new Error('buffer is closed');
      const tail = /** @type {number} */ (this.cells[TAIL]);
      this.cells[HEADER + tail] = value;
      this.cells[TAIL] = (tail + 1) % this.capacity;
      const count = /** @type {number} */ (this.cells[COUNT]) + 1;
      this.cells[COUNT] = count;
      if (count > /** @type {number} */ (this.cells[MAX_OBSERVED])) this.cells[MAX_OBSERVED] = count;
      this.#signal(NOT_EMPTY);
    } finally {
      this.#unlock();
    }
  }

  /** Block until an item arrives; null once closed and drained. @returns {number | null} */
  take() {
    this.#lock();
    try {
      while (this.cells[COUNT] === 0 && this.cells[CLOSED] === 0) this.#await(NOT_EMPTY);
      if (this.cells[COUNT] === 0) return null;
      const head = /** @type {number} */ (this.cells[HEAD]);
      const value = /** @type {number} */ (this.cells[HEADER + head]);
      this.cells[HEAD] = (head + 1) % this.capacity;
      this.cells[COUNT] = /** @type {number} */ (this.cells[COUNT]) - 1;
      this.#signal(NOT_FULL);
      return value;
    } finally {
      this.#unlock();
    }
  }

  /** Refuse new items; consumers drain what is left, then get null. */
  close() {
    this.#lock();
    try {
      this.cells[CLOSED] = 1;
      this.#signal(NOT_EMPTY, Infinity);
      this.#signal(NOT_FULL, Infinity);
    } finally {
      this.#unlock();
    }
  }

  /** Highest number of items ever queued at once. */
  get maxObserved() {
    return Atomics.load(this.cells, MAX_OBSERVED);
  }
}
