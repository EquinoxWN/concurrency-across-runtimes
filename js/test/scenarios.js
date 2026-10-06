// Shared scenario parameters (spec/scenarios.json), the same file Java and Python read.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

/** @type {any} */
export const scenarios = JSON.parse(readFileSync(new URL('../../spec/scenarios.json', import.meta.url), 'utf8'));

/** Every item exactly once, and each consumer sees each producer's items in order. */
export function checkDelivery(/** @type {number[][]} */ received, /** @type {number} */ producers, /** @type {number} */ perProducer, base = 1_000_000) {
  const flat = received.flat();
  assert.equal(flat.length, producers * perProducer, 'no item lost or duplicated');
  assert.equal(new Set(flat).size, flat.length);
  for (const mine of received) {
    const last = new Array(producers).fill(-1);
    for (const x of mine) {
      const p = Math.floor(x / base);
      const s = x % base;
      assert.ok(s > last[p], `producer ${p} out of order`);
      last[p] = s;
    }
  }
}
