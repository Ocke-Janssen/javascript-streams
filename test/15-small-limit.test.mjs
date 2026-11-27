import assert from 'node:assert/strict';
import { Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { test } from 'node:test';

async function* bounded(source, maxBytes) {
  let seen = 0;
  for await (const chunk of source) {
    seen += chunk.length;
    if (seen > maxBytes) throw new Error('input_limit');
    yield chunk;
  }
}

async function copy(maxBytes) {
  const received = [];
  await pipeline(
    [Buffer.from('ab'), Buffer.from('cde')],
    (source) => bounded(source, maxBytes),
    new Writable({
      write(chunk, _encoding, done) {
        received.push(chunk);
        done();
      },
    }),
  );
  return Buffer.concat(received);
}

test('five bytes fit below the limit', async () => {
  assert.deepEqual(await copy(6), Buffer.from('abcde'));
});

test('five bytes fit at the exact limit', async () => {
  assert.deepEqual(await copy(5), Buffer.from('abcde'));
});

test('five bytes exceed a four-byte limit', async () => {
  await assert.rejects(copy(4), { message: 'input_limit' });
});
