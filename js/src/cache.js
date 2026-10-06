/**
 * LRU cache where concurrent misses on one key share a single load (one promise). A Map keeps
 * insertion order, so re-inserting on access makes its first key the least recently used.
 * @template K, V
 */
export class SingleFlightCache {
  /** @param {number} capacity @param {(key: K) => Promise<V>} loader */
  constructor(capacity, loader) {
    if (!Number.isInteger(capacity) || capacity < 1) throw new RangeError('capacity must be at least 1');
    this.capacity = capacity;
    this.loader = loader;
    /** @type {Map<K, V>} */
    this.values = new Map();
    /** @type {Map<K, Promise<V>>} */
    this.inFlight = new Map();
    this.loads = 0;
  }

  /** Cached value, or the result of the one load shared by all concurrent callers. @param {K} key */
  async get(key) {
    if (this.values.has(key)) {
      const value = /** @type {V} */ (this.values.get(key));
      this.values.delete(key);
      this.values.set(key, value);
      return value;
    }
    let flight = this.inFlight.get(key);
    if (!flight) {
      this.loads++;
      flight = this.#load(key);
      this.inFlight.set(key, flight);
    }
    return flight;
  }

  /** @param {K} key @returns {Promise<V>} */
  async #load(key) {
    try {
      const value = await this.loader(key);
      if (value === undefined || value === null) throw new TypeError('loader returned no value');
      this.values.set(key, value);
      while (this.values.size > this.capacity) {
        this.values.delete(/** @type {K} */ (this.values.keys().next().value));
      }
      return value;
    } finally {
      this.inFlight.delete(key);
    }
  }

  /** Cached keys, least recently used first. */
  keys() {
    return [...this.values.keys()];
  }
}
