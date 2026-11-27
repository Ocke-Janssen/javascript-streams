import assert from 'node:assert/strict';
import { test } from 'node:test';
import { ndjsonRecords } from '../examples/11-fetch-browser/records.mjs';

const line = (id) => JSON.stringify({ id, message: 'Grüße' }) + '\n';

function deliver(chunks, received) {
  const records = ndjsonRecords(ReadableStream.from(chunks));
  return records.pipeTo(new WritableStream({
    write(record) {
      received.push(record);
    },
  }));
}

test('the sink sees the valid prefix at either text chunk boundary', async () => {
  const lines = [line(1), line(2), line(4)];
  const expected = [{ id: 1, message: 'Grüße' }, { id: 2, message: 'Grüße' }];
  for (const chunks of [lines, [lines.join('')]]) {
    const received = [];
    await assert.rejects(deliver(chunks, received), { message: 'unexpected record ID' });
    assert.deepEqual(received, expected);
  }
});

test('a coalesced body preserves the difference between clean EOF and a partial line', async () => {
  const prefix = line(1) + line(2);
  const partial = [];
  const complete = [];
  await deliver([prefix], complete);
  await assert.rejects(deliver([prefix + '{"id":3'], partial), { message: 'incomplete NDJSON line' });
  assert.deepEqual(complete.map((record) => record.id), [1, 2]);
  assert.deepEqual(partial, complete);
});

test('malformed JSON reports a fixed category and preserves earlier records', async () => {
  const received = [];
  const malformed = '{"message":"synthetic marker",]\n';
  await assert.rejects(deliver([line(1) + malformed], received), {
    name: 'SyntaxError', message: 'invalid NDJSON record',
  });
  assert.deepEqual(received.map((record) => record.id), [1]);
});

test('only one carriage return is allowed immediately before LF', async () => {
  const prefix = line(1).replace('\n', '\r\n');
  const escaped = JSON.stringify({ id: 2, message: 'A\rB' }) + '\n';
  const accepted = [];
  await deliver([prefix.slice(0, -1), prefix.slice(-1), escaped], accepted);
  assert.deepEqual(accepted, [{ id: 1, message: 'Grüße' }, { id: 2, message: 'A\rB' }]);

  const malformedLines = [
    '{"id":2,\r"message":"Reading"}\n',
    line(2).replace('\n', '\r\r\n'),
  ];
  for (const invalid of malformedLines) {
    const received = [];
    await assert.rejects(deliver([prefix + invalid], received), {
      name: 'SyntaxError', message: 'invalid NDJSON record',
    });
    assert.deepEqual(received.map((record) => record.id), [1]);
  }
});

test('fatal UTF-8 decoding can reject valid text in the same byte chunk', async () => {
  const prefix = new TextEncoder().encode(line(1) + line(2));
  const invalid = Uint8Array.of(0xff);
  const together = Uint8Array.from([...prefix, ...invalid]);
  const cases = [
    { chunks: [together], expected: [] },
    { chunks: [prefix, invalid], expected: [1, 2] },
  ];

  for (const { chunks, expected } of cases) {
    const text = ReadableStream.from(chunks).pipeThrough(new TextDecoderStream('utf-8', { fatal: true }));
    const received = [];
    await assert.rejects(deliver(text, received), { name: 'TypeError' });
    assert.deepEqual(received.map((record) => record.id), expected);
  }
});
