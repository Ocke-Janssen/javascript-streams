import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';

function run(example, mode) {
  const script = fileURLToPath(new URL(`../examples/${example}.mjs`, import.meta.url));
  const result = spawnSync(process.execPath, [script, mode], {
    encoding: 'utf8',
    timeout: 5000,
  });
  assert.ifError(result.error);
  assert.equal(result.signal, null);
  return {
    status: result.status,
    output: result.stdout.trimEnd().split(/\r?\n/),
    error: result.stderr.trimEnd(),
  };
}

const blocked = 'blocked pulled=3 active=1 ready=2 window=3';

test('ordered mapping refills a free slot, bounds the window and preserves order', () => {
  assert.deepEqual(run('b-ordered-concurrency', 'ok'), {
    status: 0,
    output: [
      blocked,
      'output=10,20,30,40,50',
      'peakActive=2 peakWindow=3 active=0 pulled=5 sourceClosed=true',
    ],
    error: '',
  });
});

test('ordered mapping cancels the held task and closes input on caller abort or task failure', () => {
  const summary = 'peakActive=2 peakWindow=3 active=0 pulled=3 sourceClosed=true';
  assert.deepEqual(run('b-ordered-concurrency', 'abort'), {
    status: 1,
    output: [blocked, 'output=none', summary],
    error: 'cancelled by caller',
  });
  assert.deepEqual(run('b-ordered-concurrency', 'error'), {
    status: 1,
    output: ['output=none', summary],
    error: 'record 3 failed',
  });
});

test('leaving the ordered iterator early closes the source and awaits active work', () => {
  assert.deepEqual(run('b-ordered-concurrency', 'early-exit'), {
    status: 0,
    output: [
      blocked,
      'earlyExit active=1',
      'output=10,20',
      'peakActive=2 peakWindow=3 active=0 pulled=4 sourceClosed=true',
    ],
    error: '',
  });
});

const mainResult = 'result=15 records=5 progressPending=1 replaced=3';

test('optional progress keeps the latest snapshot and does not delay the main result', () => {
  assert.deepEqual(run('b-optional-progress', 'ok'), {
    status: 0,
    output: [mainResult, 'shown=1,5 progressPending=0 replaced=3'],
    error: '',
  });
});

test('progress failure and disconnect leave the main result successful', () => {
  const empty = 'shown=none progressPending=0 replaced=3';
  assert.deepEqual(run('b-optional-progress', 'sink-error'), {
    status: 0,
    output: [mainResult, empty, 'progress error: display write failed'],
    error: '',
  });
  assert.deepEqual(run('b-optional-progress', 'disconnect'), {
    status: 0,
    output: [mainResult, empty, 'progress stopped: viewer disconnected'],
    error: '',
  });
});
