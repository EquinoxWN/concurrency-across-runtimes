import { AsyncBoundedBuffer, ClosedError } from './buffer.js';

/** A stage failed; names the stage and the index of the failing input. */
export class PipelineError extends Error {
  /** @param {string} stage @param {number} index @param {unknown} cause */
  constructor(stage, index, cause) {
    super(`stage '${stage}' failed on item ${index}: ${cause instanceof Error ? cause.message : String(cause)}`, {
      cause,
    });
    this.name = 'PipelineError';
    this.stage = stage;
    this.index = index;
  }
}

/** @typedef {{name: string, workers: number, fn: (value: any) => unknown}} Stage */
/** @typedef {{index: number, value: unknown}} Item */

/**
 * Stages joined by bounded buffers on one event loop. Concurrency here means overlapping
 * waits (I/O, timers), not parallel computation; a failure aborts every buffer.
 */
export class Pipeline {
  /** @param {Stage[]} stages @param {number} capacity @param {{ordered: boolean}} options */
  constructor(stages, capacity, { ordered }) {
    if (stages.length === 0) throw new RangeError('a pipeline needs at least one stage');
    if (stages.some((s) => !Number.isInteger(s.workers) || s.workers < 1)) {
      throw new RangeError('every stage needs at least one worker');
    }
    this.stages = stages;
    this.capacity = capacity;
    this.ordered = ordered;
    /** @type {number[]} */
    this.lastMaxOccupancy = [];
    this.lastRunTerminated = false;
  }

  /** Push every input through all stages and collect the outputs. @param {unknown[]} inputs */
  async run(inputs) {
    /** @type {AsyncBoundedBuffer<Item>[]} */
    const buffers = Array.from({ length: this.stages.length + 1 }, () => new AsyncBoundedBuffer(this.capacity));
    /** @type {PipelineError | undefined} */
    let failure;
    const fail = (/** @type {PipelineError} */ error) => {
      if (failure) return;
      failure = error;
      for (const b of buffers) b.abort();
    };
    this.lastRunTerminated = false;
    const first = /** @type {AsyncBoundedBuffer<Item>} */ (buffers[0]);
    const last = /** @type {AsyncBoundedBuffer<Item>} */ (buffers[buffers.length - 1]);

    const feed = async () => {
      try {
        for (let i = 0; i < inputs.length; i++) await first.put({ index: i, value: inputs[i] });
        first.close();
      } catch (e) {
        if (!(e instanceof ClosedError)) throw e;
      }
    };

    const tasks = [feed()];
    this.stages.forEach((stage, s) => {
      const src = /** @type {AsyncBoundedBuffer<Item>} */ (buffers[s]);
      const dst = /** @type {AsyncBoundedBuffer<Item>} */ (buffers[s + 1]);
      let remaining = stage.workers;
      const work = async () => {
        try {
          for (let item = await src.take(); item !== undefined; item = await src.take()) {
            let value;
            try {
              value = await stage.fn(item.value);
            } catch (e) {
              fail(new PipelineError(stage.name, item.index, e));
              return;
            }
            await dst.put({ index: item.index, value });
          }
        } catch (e) {
          if (!(e instanceof ClosedError)) throw e;
        } finally {
          if (--remaining === 0) dst.close();
        }
      };
      for (let w = 0; w < stage.workers; w++) tasks.push(work());
    });

    /** @type {Item[]} */
    const results = [];
    for (let item = await last.take(); item !== undefined; item = await last.take()) results.push(item);
    await Promise.all(tasks);
    this.lastRunTerminated = true;
    this.lastMaxOccupancy = buffers.map((b) => b.maxObserved);
    if (failure) throw failure;
    if (this.ordered) results.sort((a, b) => a.index - b.index);
    return results.map((r) => r.value);
  }
}
