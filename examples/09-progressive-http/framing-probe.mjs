import assert from 'node:assert/strict';
import { parseDemoSse, readDemoFrames } from './frames.mjs';

const fixture = new TextEncoder().encode([
  ': keepalive\n\n',
  'id: 1\nevent: note\ndata: Grüße\ndata: 🧪\n\n',
  'id: 2\nevent: note\ndata: Ende\n\n',
].join(''));
const expected = [
  { id: '1', type: 'note', data: 'Grüße\n🧪' },
  { id: '2', type: 'note', data: 'Ende' },
];

function stream(bytes, width) {
  let offset = 0;
  return new ReadableStream({
    pull(controller) {
      if (offset === bytes.length) {
        controller.close();
        return;
      }
      controller.enqueue(bytes.subarray(offset, offset + width));
      offset += Math.min(width, bytes.length - offset);
    },
  });
}

async function collect(bytes, width) {
  const events = [];
  await readDemoFrames(stream(bytes, width), '\n\n', (frame) => {
    const event = parseDemoSse(frame);
    if (event) events.push(event);
  });
  return events;
}

for (const width of [1, 7, fixture.length]) {
  assert.deepEqual(await collect(fixture, width), expected);
  console.log(`width=${width} ids=1,2 sameEvents=true`);
}
await assert.rejects(collect(fixture.subarray(0, -1), 1), /incomplete frame/);
console.log('missing final blank line: incomplete frame');

const encoder = new TextEncoder();
const exact = 'data: ' + 'é'.repeat(4090);
const complete = encoder.encode(exact + '\n\n');
for (const width of [1, 7, complete.length]) {
  assert.deepEqual(await collect(complete, width), [{ id: undefined, type: 'message', data: 'é'.repeat(4090) }]);
  await assert.rejects(collect(encoder.encode(exact + 'x\n\n'), width), /frame exceeds 4096/);
  await assert.rejects(collect(encoder.encode(exact + 'x'), width), /frame exceeds 4096/);
}
console.log('frame limit: exact accepted, oversized rejected with or without delimiter');

const line = '{"data":"' + 'é'.repeat(4085) + '"}';
const lines = [];
await readDemoFrames(stream(encoder.encode(line + '\n'), 7), '\n', (frame) => lines.push(frame));
assert.deepEqual(lines, [line]);
await assert.rejects(
  readDemoFrames(stream(encoder.encode(' ' + line + '\n'), 7), '\n', () => {}),
  /frame exceeds 4096/,
);
console.log('NDJSON line limit: exact accepted, oversized rejected');

await assert.rejects(collect(new Uint8Array([0xc3, 0x28, 0x0a, 0x0a]), 1), TypeError);
console.log('invalid UTF-8: TypeError');
