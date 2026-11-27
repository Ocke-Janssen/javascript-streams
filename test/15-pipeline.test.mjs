import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { EventEmitter, once } from 'node:events';
import { link, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { test } from 'node:test';
import { createGunzip, gzipSync, gunzipSync } from 'node:zlib';
import { buildOutput, limitBytes, limits } from '../projects/event-pipeline/pipeline.mjs';

const first = '{"id":1,"level":"info","event":"Grüße"}\n';
const debug = '{"id":2,"level":"debug","event":"test"}\n';
const input = Buffer.from(first + debug);

async function fixture(t) {
  const dir = await mkdtemp(join(tmpdir(), 'streams-ch15-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const output = join(dir, 'result.ndjson.gz');
  async function run(bytes, { chunkBytes = 7, splitAt, ...options } = {}) {
    return buildOutput({
      openSource: () => (async function* chunks() {
        if (splitAt !== undefined) {
          yield bytes.subarray(0, splitAt);
          yield bytes.subarray(splitAt);
          return;
        }
        for (let offset = 0; offset < bytes.length; offset += chunkBytes) {
          yield bytes.subarray(offset, offset + chunkBytes);
        }
      })(),
      output, ...options,
    });
  }
  return { dir, output, run };
}

test('preserves UTF-8 across byte boundaries and filters debug', async (t) => {
  for (const chunkBytes of [1, 7, 64]) {
    const { output, run } = await fixture(t);
    const result = await run(input, { chunkBytes });
    assert.deepEqual(gunzipSync(await readFile(output)), Buffer.from(first));
    assert.deepEqual([result.recordsRead, result.kept, result.dropped], [2, 1, 1]);
  }
});

test('every single split of one UTF-8 record produces the expected bytes', async (t) => {
  const { dir, run } = await fixture(t);
  const bytes = Buffer.from(first);
  for (let splitAt = 1; splitAt < bytes.length; splitAt += 1) {
    const output = join(dir, 'split-' + splitAt + '.ndjson.gz');
    await run(bytes, { output, splitAt });
    assert.deepEqual(gunzipSync(await readFile(output)), bytes, 'split after byte ' + splitAt);
  }
});

test('invalid UTF-8 identifies the line without echoing the input', async (t) => {
  const { dir, run } = await fixture(t);
  const bytes = Buffer.concat([Buffer.from(first + '{"event":"'), Buffer.from([0xc3, 0x28]),
    Buffer.from('"}\n')]);
  await assert.rejects(run(bytes), { code: 'invalid_utf8', line: 2, message: 'invalid_utf8' });
  assert.deepEqual(await readdir(dir), []);
});

test('missing final LF stops publication', async (t) => {
  const { dir, run } = await fixture(t);
  await assert.rejects(run(input.subarray(0, -1)), { code: 'incomplete_line', line: 2 });
  assert.deepEqual(await readdir(dir), []);
});

test('input accepts its exact byte limit', async (t) => {
  const { output, run } = await fixture(t);
  const result = await run(input, { policy: { ...limits, inputBytes: input.length } });
  assert.equal(result.readBytes, input.length);
  assert.deepEqual(gunzipSync(await readFile(output)), Buffer.from(first));
});

test('input below its byte limit is unchanged', async (t) => {
  const { output, run } = await fixture(t);
  const result = await run(input, { policy: { ...limits, inputBytes: input.length + 1 } });
  assert.equal(result.readBytes, input.length);
  assert.deepEqual(gunzipSync(await readFile(output)), Buffer.from(first));
});

test('input rejects the first byte beyond the limit', async (t) => {
  const { dir, run } = await fixture(t);
  await assert.rejects(
    run(input, { policy: { ...limits, inputBytes: input.length - 1 } }),
    { code: 'input_limit', message: 'input_limit' },
  );
  assert.deepEqual(await readdir(dir), []);
});

test('competing byte and format errors publish nothing at either chunk size', async (t) => {
  const { dir, run } = await fixture(t);
  const twoLines = Buffer.from('{\n{}\n');
  const policy = { ...limits, inputBytes: 3 };
  await assert.rejects(run(twoLines, { chunkBytes: 2, policy }), { code: 'invalid_json', line: 1 });
  await assert.rejects(run(twoLines, { chunkBytes: 5, policy }), { code: 'input_limit' });
  assert.deepEqual(await readdir(dir), []);
});

test('line limit counts bytes including CR and excludes LF', async (t) => {
  const line = Buffer.from(first.trimEnd() + '\r\n');
  const { output, run } = await fixture(t);
  const lineBytes = line.length - 1;
  await assert.rejects(run(line, { policy: { ...limits, lineBytes: lineBytes - 1 } }),
    { code: 'line_limit', line: 1 });
  await assert.rejects(run(Buffer.from(first.replace(',"level"', ',\r"level"'))),
    { code: 'invalid_line_break', line: 1 });
  await run(line, { policy: { ...limits, lineBytes } });
  assert.deepEqual(gunzipSync(await readFile(output)), Buffer.from(first));
});

test('record limit counts debug entries before filtering', async (t) => {
  const { dir, run } = await fixture(t);
  await assert.rejects(run(Buffer.from(debug + first), { policy: { ...limits, records: 1 } }),
    { code: 'record_limit', line: 2 });
  assert.deepEqual(await readdir(dir), []);
});

test('decompression enforces a separate expanded-byte limit', async () => {
  const compressed = gzipSync(Buffer.alloc(4096, 65));
  assert.ok(compressed.length < 1024);
  const sink = new Writable({
    write(_chunk, _encoding, done) { done(); },
  });
  await assert.rejects(pipeline(
    [compressed],
    (source) => limitBytes(source, 1024, 'compressed_limit'),
    createGunzip(),
    (source) => limitBytes(source, 1024, 'expanded_limit'),
    sink,
  ), { code: 'expanded_limit' });
});

test('write failure leaves the prior result and the next job can publish', async (t) => {
  const { dir, output, run } = await fixture(t);
  const original = gzipSync(Buffer.from('already published\n'));
  await writeFile(output, original);
  let writes = 0;
  await assert.rejects(run(input, {
    beforeWrite: async () => {
      writes += 1;
      throw new Error('simulated write failure');
    },
  }), { message: 'simulated write failure' });
  assert.equal(writes, 1);
  assert.deepEqual(await readFile(output), original);
  await assert.rejects(run(input), { code: 'EEXIST' });
  assert.deepEqual(await readdir(dir), ['result.ndjson.gz']);
  const next = join(dir, 'next.ndjson.gz');
  await run(input, { output: next });
  assert.deepEqual(gunzipSync(await readFile(next)), Buffer.from(first));
  assert.deepEqual(await readFile(output), original);
});

test('concurrent jobs can publish only once to the same name', { timeout: 5000 }, async (t) => {
  const { dir, output, run } = await fixture(t);
  const second = '{"id":3,"level":"warn","event":"second job"}\n';
  const release = Promise.withResolvers();
  const bothReady = Promise.withResolvers();
  let waiting = 0;
  async function publish(temporary, destination) {
    waiting += 1;
    if (waiting === 2) bothReady.resolve();
    await release.promise;
    await link(temporary, destination);
  }
  const results = Promise.allSettled([
    run(Buffer.from(first), { publish }),
    run(Buffer.from(second), { publish }),
  ]);
  try {
    await Promise.race([
      bothReady.promise,
      results.then(() => assert.fail('Both jobs must reach publication')),
    ]);
    await assert.rejects(readFile(output), { code: 'ENOENT' });
  } finally {
    release.resolve();
  }
  const attempts = await results;
  const winner = attempts.findIndex(({ status }) => status === 'fulfilled');
  assert.notEqual(winner, -1);
  assert.equal(attempts[1 - winner].status, 'rejected');
  assert.equal(attempts[1 - winner].reason.code, 'EEXIST');
  assert.deepEqual(gunzipSync(await readFile(output)), Buffer.from([first, second][winner]));
  assert.deepEqual(await readdir(dir), ['result.ndjson.gz']);
});

test('aborts at a known write boundary and removes the partial file', { timeout: 5000 }, async (t) => {
  const { dir, run } = await fixture(t);
  const controller = new AbortController();
  const entered = Promise.withResolvers();
  const gate = new EventEmitter();
  const result = run(input, {
    signal: controller.signal,
    beforeWrite: async (signal) => {
      entered.resolve();
      await once(gate, 'release', { signal });
    },
  });
  const rejected = assert.rejects(result, { name: 'AbortError' });
  try {
    await Promise.race([entered.promise, result]);
  } finally {
    controller.abort();
  }
  await rejected;
  assert.deepEqual(await readdir(dir), []);
});

test('the async write gate handles one gzip chunk at a time', { timeout: 5000 }, async (t) => {
  const { run } = await fixture(t);
  const records = Buffer.from(Array.from({ length: 4096 }, (_, id) => {
    const event = createHash('sha256').update(String(id)).digest('hex');
    return JSON.stringify({ id: id + 1, level: 'info', event }) + '\n';
  }).join(''));
  const firstWrite = Promise.withResolvers();
  const release = Promise.withResolvers();
  let writes = 0;
  let inFlight = 0;
  let peak = 0;
  const result = run(records, {
    chunkBytes: 16 * 1024,
    beforeWrite: async () => {
      writes += 1;
      inFlight += 1;
      peak = Math.max(peak, inFlight);
      firstWrite.resolve();
      await release.promise;
      inFlight -= 1;
    },
  });
  try {
    await Promise.race([firstWrite.promise, result]);
    assert.equal(writes, 1);
  } finally {
    release.resolve();
  }
  await result;
  assert.ok(writes > 1);
  assert.equal(peak, 1);
});

test('abort with publication pending preserves a successful link', { timeout: 5000 }, async (t) => {
  const { dir, output, run } = await fixture(t);
  const controller = new AbortController();
  const entered = Promise.withResolvers();
  const release = Promise.withResolvers();
  const result = run(input, {
    signal: controller.signal,
    async publish(partial, target) {
      entered.resolve();
      await release.promise;
      await link(partial, target);
    },
  });
  try {
    await Promise.race([entered.promise, result]);
    await assert.rejects(readFile(output), { code: 'ENOENT' });
    controller.abort();
  } finally {
    release.resolve();
  }
  assert.equal((await result).cleanupPending, false);
  assert.deepEqual(await readdir(dir), ['result.ndjson.gz']);
  assert.deepEqual(gunzipSync(await readFile(output)), Buffer.from(first));
});

test('cleanup failure after publication does not revoke the result', async (t) => {
  const { dir, output, run } = await fixture(t);
  let temporary;
  const result = await run(input, {
    async removeTemporary(directory) {
      temporary = directory;
      throw new Error('simulated cleanup failure');
    },
  });
  assert.equal(result.cleanupPending, true);
  assert.equal((await readdir(dir)).length, 2);
  const finalBytes = await readFile(output);
  assert.deepEqual(await readFile(join(temporary, 'output.ndjson.gz')), finalBytes);
  assert.deepEqual(gunzipSync(finalBytes), Buffer.from(first));
});
