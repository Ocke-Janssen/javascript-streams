import assert from 'node:assert/strict';
import { test } from 'node:test';
import { readProgress } from '../public/progress.mjs';

const runId = '10500d63-052e-47cb-9c72-7b05de9b6934';
const start = { event: 'run.start', runId, label: 'Grüße' };
const partial = { event: 'run.progress', runId, recordsRead: 1, kept: 1, dropped: 0 };
const complete = {
  event: 'run.end', runId, recordsRead: 12, kept: 8, dropped: 4, status: 'ok',
  outputPath: '/output/' + runId + '.ndjson.gz',
  outputSha256: 'fc127eb395903d16763493bfc297ccdf57f00bd91dde9c5c41876261ec5e26ef',
};

function body(events, chunkBytes = 32) {
  const bytes = new TextEncoder().encode(events.map((event) => JSON.stringify(event) + '\n').join(''));
  let offset = 0;
  return new ReadableStream({
    pull(controller) {
      if (offset === bytes.length) {
        controller.close();
        return;
      }
      controller.enqueue(bytes.subarray(offset, offset + chunkBytes));
      offset = Math.min(bytes.length, offset + chunkBytes);
    },
  });
}

test('progress and completion survive different byte cuts', async () => {
  const events = [start, partial, complete];
  for (const size of [1, 7, 4096]) {
    assert.deepEqual(await Array.fromAsync(readProgress(body(events, size))), events);
  }
  const handedOff = readProgress(body(events));
  (await handedOff.next()).value.runId = 'local display label';
  (await handedOff.next()).value.kept = 1000;
  assert.deepEqual(await handedOff.next(), { value: complete, done: false });
  assert.equal((await handedOff.next()).done, true);
});

test('an orderly EOF without run.end leaves the outcome unknown', async () => {
  const observed = [];
  await assert.rejects(async () => {
    for await (const event of readProgress(body([start, partial]))) observed.push(event);
  }, { message: 'Missing end event' });
  assert.deepEqual(observed, [start, partial]);
});

test('invalid progress JSON reports a category without reflecting input', async () => {
  const invalidBody = new Response('SYNTHETIC_BODY_FRAGMENT\n').body;
  await assert.rejects(Array.fromAsync(readProgress(invalidBody)), {
    message: 'Invalid progress JSON',
  });
});

test('a matching run.end is withheld when another event follows', async () => {
  const observed = [];
  await assert.rejects(async () => {
    for await (const event of readProgress(body([start, partial, complete, partial]))) observed.push(event);
  }, { message: 'Event after run.end' });
  assert.deepEqual(observed, [start, partial]);
});

test('a counter regression or a different run ID invalidates the response', async () => {
  await assert.rejects(Array.fromAsync(readProgress(body([
    start, partial, { ...complete, recordsRead: 1, kept: 0, dropped: 1 },
  ]))), { message: 'Invalid record counters' });
  await assert.rejects(Array.fromAsync(readProgress(body([
    start, partial, { ...complete, runId: '20500d63-052e-47cb-9c72-7b05de9b6934' },
  ]))), { message: 'Unexpected run ID' });
});

test('a complete protocol response can report a failed application run', async () => {
  const failure = {
    event: 'run.end', runId, recordsRead: 3, kept: 2, dropped: 1, status: 'error',
    reason: 'invalid_json', line: 4,
  };
  assert.deepEqual(await Array.fromAsync(readProgress(body([start, partial, failure]))),
    [start, partial, failure]);
});
