import assert from 'node:assert/strict';
import { parseDemoSse, readDemoFrames } from './frames.mjs';

const port = Number(process.env.PORT ?? 3002);
const base = `http://127.0.0.1:${port}`;
const endId = 5;

function get(path, headers = {}) {
  return fetch(base + path, { headers, signal: AbortSignal.timeout(5000) });
}

async function replayAfter(cursor) {
  const response = await get('/sse?window=bounded', { 'Last-Event-ID': String(cursor) });
  if (response.status === 409) {
    const gap = await response.json();
    assert.deepEqual(gap, { reason: 'replay_gap', earliestId: 3 });
    console.log(`cursor=${cursor} status=${response.status} ${gap.reason} earliestId=${gap.earliestId}`);
    return null;
  }

  assert.equal(response.status, 200);
  assert.match(response.headers.get('content-type') ?? '', /^text\/event-stream\b/);
  const ids = [];
  let nextId = cursor + 1;
  let complete = false;
  await readDemoFrames(response.body, '\n\n', (frame) => {
    const event = parseDemoSse(frame);
    if (!event) return;
    assert.ok(!complete, 'unexpected event after the end event');
    assert.equal(event.id, String(nextId));
    if (nextId === endId) {
      assert.equal(event.type, 'end');
      assert.equal(event.data, '{}');
      complete = true;
    } else {
      assert.equal(event.type, 'progress');
      assert.deepEqual(JSON.parse(event.data), { id: nextId, percent: nextId * 25 });
    }
    ids.push(nextId);
    nextId += 1;
  });
  assert.ok(complete, 'missing SSE end event');
  console.log(`cursor=${cursor} status=${response.status} ids=${ids.join(',')} end=${ids.at(-1)}`);
  return ids;
}

assert.deepEqual(await replayAfter(2), [3, 4, 5]);
assert.equal(await replayAfter(1), null);

const response = await get('/snapshot');
assert.equal(response.status, 200);
const snapshot = await response.json();
assert.deepEqual(snapshot, { percent: 100, throughId: 4 });
console.log(`snapshot status=${response.status} percent=${snapshot.percent} throughId=${snapshot.throughId}`);
assert.deepEqual(await replayAfter(snapshot.throughId), [5]);
