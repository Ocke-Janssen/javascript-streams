import assert from 'node:assert/strict';

const controller = new AbortController();
const { promise: gate, resolve: release } = Promise.withResolvers();
const effects = [];
const oldRun = 'export-1';
let view = { runId: oldRun, result: 'pending' };
const task = gate.then(() => {
  effects.push('old run wrote');
  if (view.runId === oldRun) view.result = 'old result';
});
const cancelled = new Promise((_, reject) => {
  controller.signal.addEventListener('abort', () => reject(controller.signal.reason), { once: true });
});

const waiting = Promise.race([task, cancelled]);
controller.abort(new Error('caller stopped waiting'));
await assert.rejects(waiting, /caller stopped waiting/);
assert.deepEqual(effects, []);
console.log('wait=aborted task=pending');

view = { runId: 'export-2', result: 'new result' };
release();
await task;
assert.deepEqual(effects, ['old run wrote']);
assert.deepEqual(view, { runId: 'export-2', result: 'new result' });
console.log('lateEffect=true ownerWaited=true');
console.log(`active=${view.runId} result=${view.result}`);

async function firstOutcome(writeFirst) {
  const controller = new AbortController();
  const { promise: write, reject: failWrite } = Promise.withResolvers();
  const abort = new Promise((_, reject) => {
    controller.signal.addEventListener('abort', () => reject(controller.signal.reason), { once: true });
  });

  // Model two notifications delivered after admission and before publication.
  const decision = Promise.race([write, abort]);
  const failure = new Error('write rejected');
  const stop = new Error('manual abort');
  if (writeFirst) {
    failWrite(failure);
    controller.abort(stop);
  } else {
    controller.abort(stop);
    failWrite(failure);
  }
  const primary = await decision.catch((error) => error);
  await Promise.allSettled([write, abort]);
  assert.equal(primary, writeFirst ? failure : stop);
  console.log(`first=${writeFirst ? 'write' : 'abort'} primary=${primary.message} bothSettled=true`);
}

await firstOutcome(true);
await firstOutcome(false);
