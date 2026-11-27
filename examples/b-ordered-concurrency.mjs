import { addAbortListener, EventEmitter, once } from 'node:events';

async function* mapInOrder(records, transform, { signal, stats, onWait }) {
  const maxActive = 2;
  const maxWindow = 3;
  const stop = new AbortController();
  const workSignal = AbortSignal.any([signal, stop.signal]);
  const source = records[Symbol.iterator]();
  const window = new Map();
  let changed = Promise.withResolvers();
  const listener = addAbortListener(workSignal, () => changed.resolve());
  let sourceEnded = false;
  let nextInput = 0;
  let nextOutput = 0;

  async function run(record, slot) {
    try {
      slot.result = await transform(record, workSignal);
      slot.ready = true;
    } catch (error) {
      stop.abort(error);
    } finally {
      stats.active -= 1;
      changed.resolve();
    }
  }

  try {
    while (true) {
      while (!sourceEnded && stats.active < maxActive && window.size < maxWindow) {
        workSignal.throwIfAborted();
        const record = source.next();
        if (record.done) {
          sourceEnded = true;
          break;
        }
        const slot = { ready: false };
        window.set(nextInput, slot);
        nextInput += 1;
        stats.active += 1;
        stats.peakActive = Math.max(stats.peakActive, stats.active);
        stats.peakWindow = Math.max(stats.peakWindow, window.size);
        slot.task = run(record.value, slot);
      }

      workSignal.throwIfAborted();
      const head = window.get(nextOutput);
      if (head?.ready) {
        yield head.result;
        // A consumer that has not asked for the next item still occupies this slot.
        window.delete(nextOutput);
        nextOutput += 1;
      } else if (sourceEnded && window.size === 0) {
        return;
      } else {
        onWait(window.size, [...window.values()].filter((slot) => slot.ready).length);
        await changed.promise;
        changed = Promise.withResolvers();
      }
    }
  } finally {
    listener[Symbol.dispose]();
    stop.abort();
    try {
      if (!sourceEnded) source.return?.();
    } finally {
      await Promise.all([...window.values()].map((slot) => slot.task));
    }
  }
}

async function demo(mode) {
  const gate = new EventEmitter();
  const caller = new AbortController();
  const stats = { active: 0, peakActive: 0, peakWindow: 0 };
  const output = [];
  let pulled = 0;
  let sourceClosed = false;
  let released = false;

  function* records() {
    try {
      for (let id = 1; id <= 5; id += 1) {
        pulled += 1;
        yield id;
      }
    } finally {
      sourceClosed = true;
    }
  }

  async function transform(id, signal) {
    if (id === 1) await once(gate, 'release', { signal });
    if (mode === 'early-exit' && id === 4) await once(gate, 'unreleased', { signal });
    if (mode === 'error' && id === 3) throw new Error('record 3 failed');
    return id * 10;
  }

  function onWait(windowSize, ready) {
    if (released || windowSize !== 3 || ready !== 2) return;
    released = true;
    console.log(`blocked pulled=${pulled} active=${stats.active} ready=${ready} window=${windowSize}`);
    if (mode === 'abort') caller.abort(new Error('cancelled by caller'));
    else gate.emit('release');
  }

  try {
    for await (const result of mapInOrder(records(), transform, { signal: caller.signal, stats, onWait })) {
      output.push(result);
      if (mode === 'early-exit' && output.length === 2) {
        console.log(`earlyExit active=${stats.active}`);
        break;
      }
    }
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  } finally {
    console.log(`output=${output.join(',') || 'none'}`);
    console.log(`peakActive=${stats.peakActive} peakWindow=${stats.peakWindow} active=${stats.active} pulled=${pulled} sourceClosed=${sourceClosed}`);
  }
}

const mode = process.argv[2] ?? 'ok';
if (['ok', 'abort', 'error', 'early-exit'].includes(mode)) {
  await demo(mode);
} else {
  console.error('Usage: node examples/b-ordered-concurrency.mjs [ok|abort|error|early-exit]');
  process.exitCode = 1;
}
