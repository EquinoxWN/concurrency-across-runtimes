/** Mutex for async code; acquire can give up after a timeout. */
export class AsyncMutex {
  locked = false;
  /** @type {Array<() => void>} */
  waiters = [];

  /** Resolve true once held, or false after timeoutMs. @param {number} [timeoutMs] */
  acquire(timeoutMs) {
    if (!this.locked) {
      this.locked = true;
      return Promise.resolve(true);
    }
    return new Promise((resolve) => {
      /** @type {ReturnType<typeof setTimeout> | undefined} */
      let timer;
      const grant = () => {
        if (timer) clearTimeout(timer);
        resolve(true);
      };
      this.waiters.push(grant);
      if (timeoutMs !== undefined) {
        timer = setTimeout(() => {
          const i = this.waiters.indexOf(grant);
          if (i >= 0) this.waiters.splice(i, 1);
          resolve(false);
        }, timeoutMs);
      }
    });
  }

  /** Hand the lock to the next waiter, or unlock. */
  release() {
    const next = this.waiters.shift();
    if (next) next();
    else this.locked = false;
  }
}

/** Counting semaphore for async code. */
class Semaphore {
  /** @param {number} permits */
  constructor(permits) {
    this.permits = permits;
    /** @type {Array<() => void>} */
    this.waiters = [];
  }

  async acquire() {
    if (this.permits > 0) {
      this.permits--;
      return;
    }
    await new Promise((resolve) => this.waiters.push(() => resolve(undefined)));
  }

  release() {
    const next = this.waiters.shift();
    if (next) next();
    else this.permits++;
  }
}

/** Waits until n parties arrive; a timeout breaks it for everyone. */
export class Barrier {
  /** @param {number} parties */
  constructor(parties) {
    this.parties = parties;
    /** @type {Array<(tripped: boolean) => void>} */
    this.waiting = [];
    this.broken = false;
  }

  /** Resolve true when all parties arrived, false when broken or timed out. @param {number} timeoutMs */
  wait(timeoutMs) {
    if (this.broken) return Promise.resolve(false);
    return new Promise((resolve) => {
      this.waiting.push(resolve);
      if (this.waiting.length === this.parties) {
        for (const r of this.waiting.splice(0)) r(true);
        return;
      }
      setTimeout(() => {
        if (this.waiting.includes(resolve)) this.#break();
      }, timeoutMs);
    });
  }

  #break() {
    this.broken = true;
    for (const r of this.waiting.splice(0)) r(false);
  }
}

/**
 * Dining philosophers on the event loop. There is only one thread, yet the circular wait
 * still forms because every await is a point where another philosopher can run.
 * @param {number} n @param {number} meals @param {'naive' | 'ordered' | 'seats'} strategy
 * @param {{forceCycle: boolean, timeoutMs: number}} options
 */
export async function dine(n, meals, strategy, { forceCycle, timeoutMs }) {
  const forks = Array.from({ length: n }, () => new AsyncMutex());
  const owner = new Array(n).fill(-1);
  const seats = new Semaphore(strategy === 'seats' ? n - 1 : n);
  const holding = new Barrier(n);
  let deadlocked = false;
  let cycleFormed = false;
  let violations = 0;
  const meals_ = new Array(n).fill(0);

  const take = (/** @type {number} */ fork, /** @type {number} */ who) => {
    if (owner[fork] !== -1) violations++;
    owner[fork] = who;
  };

  const philosopher = async (/** @type {number} */ who) => {
    const left = who;
    const right = (who + 1) % n;
    const first = strategy === 'ordered' ? Math.min(left, right) : left;
    const second = first === left ? right : left;
    const firstFork = /** @type {AsyncMutex} */ (forks[first]);
    const secondFork = /** @type {AsyncMutex} */ (forks[second]);
    for (let meal = 0; meal < meals && !deadlocked; meal++) {
      await seats.acquire();
      await firstFork.acquire();
      try {
        take(first, who);
        await Promise.resolve(); // yield, as real work between the two forks would
        if (forceCycle && meal === 0 && (await holding.wait(timeoutMs))) cycleFormed = true;
        if (!(await secondFork.acquire(timeoutMs))) {
          owner[first] = -1;
          deadlocked = true;
          return;
        }
        take(second, who);
        await Promise.resolve();
        owner[second] = -1;
        secondFork.release();
        owner[first] = -1;
        meals_[who]++;
      } finally {
        firstFork.release();
        seats.release();
      }
    }
  };

  await Promise.all(Array.from({ length: n }, (_, p) => philosopher(p)));
  return { deadlocked, cycleFormed, meals: meals_, violations };
}
