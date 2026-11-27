import { EventEmitter, once } from 'node:events';
import { Readable } from 'node:stream';

function optionalProgress(write) {
  const connection = new AbortController();
  let accepting = true;
  let pending;
  let pump;
  let replaced = 0;
  let failure;

  async function drain() {
    try {
      while (pending !== undefined) {
        const update = pending;
        pending = undefined;
        await write(update, connection.signal);
      }
    } catch (error) {
      accepting = false;
      pending = undefined;
      if (!connection.signal.aborted) failure = error;
    } finally {
      pump = undefined;
    }
  }

  return {
    offer(snapshot) {
      if (!accepting) return;
      if (pending !== undefined) replaced += 1;
      pending = snapshot;
      pump ??= Promise.resolve().then(drain);
    },

    async stop({ discard = false } = {}) {
      accepting = false;
      if (discard) {
        pending = undefined;
        connection.abort();
      }
      await pump;
    },

    state() {
      return { pending: Number(pending !== undefined), replaced, failure };
    },
  };
}

async function demo(mode) {
  const gate = new EventEmitter();
  const shown = [];
  const progress = optionalProgress(async (snapshot, signal) => {
    if (snapshot.processed === 1) {
      await once(gate, 'release', { signal });
      if (mode === 'sink-error') throw new Error('display write failed');
    }
    shown.push(snapshot.processed);
  });

  let sum = 0;
  let processed = 0;
  let completed = false;
  try {
    for await (const value of Readable.from([1, 2, 3, 4, 5])) {
      sum += value;
      processed += 1;
      progress.offer({ processed });
    }
    // The result is available while the first display write is still paused.
    const { pending, replaced } = progress.state();
    console.log(`result=${sum} records=${processed} progressPending=${pending} replaced=${replaced}`);

    completed = true;
  } finally {
    const discard = !completed || mode === 'disconnect';
    if (!discard) gate.emit('release');
    await progress.stop({ discard });
  }

  const { pending, replaced, failure } = progress.state();
  console.log(`shown=${shown.join(',') || 'none'} progressPending=${pending} replaced=${replaced}`);
  if (failure) console.log(`progress error: ${failure.message}`);
  if (mode === 'disconnect') console.log('progress stopped: viewer disconnected');
}

const mode = process.argv[2] ?? 'ok';
if (['ok', 'sink-error', 'disconnect'].includes(mode)) {
  await demo(mode);
} else {
  console.error('Usage: node examples/b-optional-progress.mjs [ok|sink-error|disconnect]');
  process.exitCode = 1;
}
