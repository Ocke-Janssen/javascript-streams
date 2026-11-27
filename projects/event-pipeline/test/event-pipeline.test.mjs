import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { EventEmitter, once } from 'node:events';
import { link, mkdtemp, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import { test } from 'node:test';
import { gunzipSync } from 'node:zlib';
import { buildOutput } from '../pipeline.mjs';
import { createDemoServer } from '../server.mjs';

async function fixture(t, options = {}) {
  const workspace = await mkdtemp(join(tmpdir(), 'streams-ch16-'));
  const entries = [];
  const gate = new EventEmitter();
  const logEvents = new EventEmitter();
  let released = false;
  const server = await createDemoServer({
    workspace,
    log(entry) {
      entries.push(entry);
      logEvents.emit(entry.event, entry);
    },
    async slowWrite(signal) {
      if (!released) await once(gate, 'release', { signal });
    },
    ...options,
  });
  const release = () => { released = true; gate.emit('release'); };
  t.after(async () => {
    const closed = new Promise((resolve) => server.close(resolve));
    server.closeAllConnections();
    await closed;
    await rm(workspace, { recursive: true, force: true });
  });

  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const base = 'http://127.0.0.1:' + server.address().port;
  async function run(query, signal) {
    return fetch(base + '/run?' + query, {
      method: 'POST', headers: { 'X-Stream-Demo': '1' }, signal,
    });
  }
  return { base, entries, logEvents, release, run, server, workspace, output: join(workspace, 'output') };
}

async function* events(response) {
  assert.equal(response.status, 200);
  assert.match(response.headers.get('content-type'), /^application\/x-ndjson/);
  let pending = '';
  for await (const chunk of response.body.pipeThrough(new TextDecoderStream())) {
    pending += chunk;
    let lf;
    while ((lf = pending.indexOf('\n')) !== -1) {
      yield JSON.parse(pending.slice(0, lf));
      pending = pending.slice(lf + 1);
    }
  }
  assert.equal(pending, '');
}

async function readAll(response) {
  return Array.fromAsync(events(response));
}

test('progress precedes publication and the download matches the file and digest',
  { timeout: 8000 }, async (t) => {
    const f = await fixture(t);
    assert.match(await (await fetch(f.base)).text(), /Event Pipeline/);
    const stream = events(await f.run('mode=slow'));
    assert.equal((await stream.next()).value.event, 'run.start');
    const progress = (await stream.next()).value;
    assert.equal(progress.event, 'run.progress');
    assert.equal(progress.recordsRead, 1);
    assert.equal(f.entries.some((entry) => entry.event === 'run.end'), false);
    assert.deepEqual((await readdir(f.output)).filter((name) => name.endsWith('.gz')), []);
    f.release();
    const rest = await Array.fromAsync(stream);
    assert.equal(rest.length, 1);
    const [end] = rest;
    assert.equal(end.status, 'ok');
    assert.equal(end.runId, progress.runId);
    assert.deepEqual([end.recordsRead, end.kept, end.dropped], [12, 8, 4]);
    assert.deepEqual(end, f.entries.at(-1));
    const download = await fetch(f.base + end.outputPath);
    assert.equal(download.status, 200);
    assert.equal(download.headers.get('content-type'), 'application/gzip');
    const gzip = Buffer.from(await download.arrayBuffer());
    const [name] = await readdir(f.output);
    assert.deepEqual(gzip, await readFile(join(f.output, name)));
    assert.equal(end.gzipBytes, gzip.length);
    const raw = gunzipSync(gzip);
    const ids = [1, 3, 4, 6, 7, 9, 10, 12];
    const expected = ids.map((id) => ({
      id, level: id % 3 === 0 ? 'warn' : 'info', event: 'Grüße ' + id,
    }));
    assert.deepEqual(raw, Buffer.from(expected.map((record) => JSON.stringify(record) + '\n').join('')));
    assert.equal(end.outputSha256, createHash('sha256').update(raw).digest('hex'));
  });

test('the malformed record reaches a terminal error with no partial output',
  { timeout: 8000 }, async (t) => {
    const f = await fixture(t);
    const entries = await readAll(await f.run('mode=bad'));
    assert.deepEqual(entries.map(({ event }) => event), ['run.start', 'run.progress', 'run.end']);
    const end = entries.at(-1);
    assert.equal(end.status, 'error');
    assert.equal(end.reason, 'invalid_json');
    assert.equal(end.line, 4);
    assert.equal(end.recordsRead, 3);
    assert.equal(end.outputPath, undefined);
    assert.deepEqual(end, f.entries.at(-1));
    assert.deepEqual(await readdir(f.output), []);
    assert.doesNotMatch(JSON.stringify(f.entries), /Grüße|SyntaxError/);
  });

test('disconnect aborts a gated job, frees the slot and allows the next run',
  { timeout: 8000 }, async (t) => {
    const f = await fixture(t);
    const controller = new AbortController();
    const stream = events(await f.run('mode=slow', controller.signal));
    const start = (await stream.next()).value;
    assert.equal((await stream.next()).value.event, 'run.progress');
    const busy = await f.run('mode=ok');
    assert.equal(busy.status, 409);
    assert.deepEqual(await busy.json(), { reason: 'busy' });
    const terminal = once(f.logEvents, 'run.end', { signal: AbortSignal.timeout(5000) });
    controller.abort();
    await assert.rejects(stream.next(), { name: 'AbortError' });
    const [end] = await terminal;
    assert.deepEqual([end.runId, end.status, end.reason], [start.runId, 'aborted', 'client_disconnect']);
    assert.deepEqual(await readdir(f.output), []);
    const next = (await readAll(await f.run('mode=ok'))).at(-1);
    assert.equal(next.status, 'ok');
    assert.notEqual(next.runId, start.runId);
    assert.deepEqual(await readdir(f.output), [next.outputPath.split('/').at(-1)]);
  });

test('the server processes the generated-file route with the same filter',
  { timeout: 8000 }, async (t) => {
    const f = await fixture(t);
    const fixtureBytes = await readFile(new URL('../fixtures/demo.ndjson', import.meta.url));
    await writeFile(join(f.workspace, 'input.ndjson'), fixtureBytes);
    const large = (await readAll(await f.run('mode=ok&input=large'))).at(-1);
    const demo = (await readAll(await f.run('mode=ok&input=demo'))).at(-1);
    assert.equal(large.status, 'ok');
    assert.equal(large.outputSha256, demo.outputSha256);
    assert.deepEqual([large.recordsRead, large.kept, large.dropped], [12, 8, 4]);
  });

test('a client can lose the final message while publication still succeeds',
  { timeout: 8000 }, async (t) => {
    const entered = Promise.withResolvers();
    const releasePublication = Promise.withResolvers();
    const responseClosed = Promise.withResolvers();
    const f = await fixture(t, {
      async publish(partial, target) {
        entered.resolve();
        await releasePublication.promise;
        await link(partial, target);
      },
    });
    f.server.once('request', (_req, res) => res.once('close', responseClosed.resolve));
    const controller = new AbortController();
    const terminal = once(f.logEvents, 'run.end', { signal: AbortSignal.timeout(5000) });
    const stream = events(await f.run('mode=ok', controller.signal));
    const start = (await stream.next()).value;
    assert.equal((await stream.next()).value.event, 'run.progress');
    try {
      await Promise.race([entered.promise, terminal]);
      assert.deepEqual((await readdir(f.output)).filter((name) => name.endsWith('.gz')), []);
      controller.abort();
      await assert.rejects(stream.next(), { name: 'AbortError' });
      await responseClosed.promise;
    } finally {
      releasePublication.resolve();
    }
    const [end] = await terminal;
    assert.deepEqual([end.runId, end.status, end.cleanupPending], [start.runId, 'ok', false]);
    const result = gunzipSync(await readFile(join(f.output, end.outputPath.split('/').at(-1))));
    assert.equal(result.length, 356);
    assert.equal(createHash('sha256').update(result).digest('hex'),
      'fc127eb395903d16763493bfc297ccdf57f00bd91dde9c5c41876261ec5e26ef');
  });

test('the deadline stops a gated job and leaves no output',
  { timeout: 8000 }, async (t) => {
    const f = await fixture(t, { deadlineMs: 500 });
    const entries = await readAll(await f.run('mode=slow'));
    const end = entries.at(-1);
    assert.deepEqual([end.status, end.reason], ['error', 'deadline']);
    assert.equal(end.outputPath, undefined);
    assert.deepEqual(await readdir(f.output), []);
  });

test('a downstream failure aborts waiting progress and removes the temporary file',
  { timeout: 8000 }, async (t) => {
    const directory = await mkdtemp(join(tmpdir(), 'streams-ch16-progress-'));
    t.after(() => rm(directory, { recursive: true, force: true }));
    const gate = new EventEmitter();
    const progressWaiting = Promise.withResolvers();
    const failure = new Error('simulated write failure');
    const input = Buffer.from('{"id":1,"level":"info","event":"a"}\n'.repeat(5000));
    let progressAborted = false;

    const run = buildOutput({
      openSource: () => Readable.from([input]),
      output: join(directory, 'result.gz'),
      async progress({ recordsRead }, signal) {
        if (recordsRead !== 5000) return;
        progressWaiting.resolve();
        try {
          await once(gate, 'release', { signal: AbortSignal.any([signal, t.signal]) });
        } finally {
          progressAborted = signal?.aborted;
        }
      },
      async beforeWrite() {
        await progressWaiting.promise;
        throw failure;
      },
    });

    await assert.rejects(run, (error) => error === failure);
    assert.equal(progressAborted, true);
    assert.deepEqual(await readdir(directory), []);
  });
