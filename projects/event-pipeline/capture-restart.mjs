import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { EventEmitter, once } from 'node:events';
import { link, mkdir, mkdtemp, readFile, readdir, realpath, rm, writeFile } from 'node:fs/promises';
import { request } from 'node:http';
import { tmpdir } from 'node:os';
import { basename, join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { gunzipSync } from 'node:zlib';
import { readProgress } from './public/progress.mjs';
import { createDemoServer } from './server.mjs';

const expectedDigest = 'fc127eb395903d16763493bfc297ccdf57f00bd91dde9c5c41876261ec5e26ef';
const scenarios = ['within-grace', 'before-publication', 'during-publication'];

async function bounded(promise) {
  const timeout = new AbortController();
  try {
    return await Promise.race([
      promise,
      delay(12_000, undefined, { signal: timeout.signal }).then(() => {
        throw new Error('The expected restart transition was not observed');
      }),
    ]);
  } finally {
    timeout.abort();
  }
}

async function listen(server, port) {
  server.listen(port, '127.0.0.1');
  await once(server, 'listening');
  return server.address().port;
}

function run(base, mode, signal) {
  return fetch(base + '/run?mode=' + mode, {
    method: 'POST', headers: { 'X-Stream-Demo': '1' }, signal,
  });
}

// agent:false proves admission refusal on a new connection before listener closure.
function requestWhileDraining(base) {
  return new Promise((resolve, reject) => {
    const req = request(base + '/run?mode=ok', {
      method: 'POST', agent: false,
      headers: { 'X-Stream-Demo': '1', 'Content-Length': '0' },
    }, (res) => {
      let body = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => { body += chunk; });
      res.once('error', reject);
      res.once('end', () => {
        try {
          resolve({ status: res.statusCode, body: JSON.parse(body), complete: res.complete });
        } catch (error) {
          reject(error);
        }
      });
    });
    req.once('error', reject);
    req.end();
  });
}

export function verifyDemoGzip(bytes) {
  const raw = gunzipSync(bytes);
  const digest = createHash('sha256').update(raw).digest('hex');
  assert.equal(raw.length, 356);
  assert.equal(digest, expectedDigest);
  return { bytes: raw.length, records: raw.toString('utf8').split('\n').length - 1, match: true };
}

async function verifyFile(file) {
  return verifyDemoGzip(await readFile(file));
}

async function writeEvents(file, entries) {
  await writeFile(file, entries.map((entry) => JSON.stringify(entry) + '\n').join(''));
}

async function captureCase(directory, scenario) {
  await mkdir(directory, { recursive: true });
  const output = join(directory, 'output');
  const originalEvents = [];
  const replacementEvents = [];
  const boundary = Promise.withResolvers();
  const deadline = Promise.withResolvers();
  const publication = Promise.withResolvers();
  const writeGate = new EventEmitter();
  let writeReleased = false;
  let replacement;
  let shutdown;
  let clientEvents;
  let clientCapture;
  const client = new AbortController();
  const server = await createDemoServer({
    workspace: directory,
    log(entry) {
      originalEvents.push(entry);
      if (entry.event === 'server.shutdown_deadline') deadline.resolve();
    },
    async slowWrite(signal) {
      boundary.resolve();
      if (!writeReleased) await once(writeGate, 'release', { signal });
    },
    async publish(partial, target) {
      if (scenario === 'during-publication') {
        boundary.resolve();
        await publication.promise;
      }
      await link(partial, target);
    },
  });
  function releaseWrite() {
    writeReleased = true;
    writeGate.emit('release');
  }
  try {
    const port = await bounded(listen(server, 0));
    const base = 'http://127.0.0.1:' + port;
    const response = await bounded(run(base,
      scenario === 'during-publication' ? 'ok' : 'slow', client.signal));
    assert.equal(response.status, 200);
    clientCapture = Array.fromAsync(readProgress(response.body));
    // Observe rejection immediately as well as at the eventual await.
    clientCapture.catch(() => {});
    await bounded(boundary.promise);
    assert.equal(originalEvents.some((entry) => entry.event === 'run.end'), false);

    const graceMs = scenario === 'within-grace' ? 5000 : 1000;
    shutdown = server.shutdown({ graceMs });
    assert.equal(server.shutdown({ graceMs }), shutdown);
    const refused = await bounded(requestWhileDraining(base));
    assert.deepEqual(refused, { status: 503, body: { reason: 'shutting_down' }, complete: true });
    assert.equal(originalEvents.filter((entry) => entry.event === 'run.start').length, 1);
    let shutdownPendingAtDeadline;
    if (scenario === 'within-grace') {
      releaseWrite();
    } else {
      await bounded(deadline.promise);
      if (scenario === 'during-publication') {
        let stopped = false;
        shutdown.then(() => { stopped = true; });
        await Promise.resolve();
        shutdownPendingAtDeadline = !stopped;
        assert.equal(shutdownPendingAtDeadline, true);
        publication.resolve();
      }
    }

    clientEvents = await bounded(clientCapture);
    const stopResult = await bounded(shutdown);
    const end = clientEvents.at(-1);
    assert.equal(end.event, 'run.end');
    assert.equal(server.listening, false);
    assert.equal(server.address(), null);
    assert.equal(stopResult.graceExpired, scenario !== 'within-grace');
    assert.deepEqual([end.status, end.reason], scenario === 'before-publication'
      ? ['aborted', 'shutdown_deadline'] : ['ok', 'completed']);
    const originals = await readdir(output);
    assert.equal(originals.length, scenario === 'before-publication' ? 0 : 1);
    assert.equal(originals.every((name) => name.endsWith('.ndjson.gz')), true);
    const originalResult = originals.length ? await verifyFile(join(output, originals[0])) : null;

    // This is a new server instance in THIS process, using the same address and files.
    replacement = await createDemoServer({
      workspace: directory, log: (entry) => replacementEvents.push(entry),
    });
    assert.equal(await bounded(listen(replacement, port)), port);
    assert.deepEqual(await readdir(output), originals);
    const previousPath = '/output/' + end.runId + '.ndjson.gz';
    const download = await bounded(fetch(base + previousPath));
    let previousOutput;
    if (originals.length) {
      assert.equal(download.status, 200);
      assert.deepEqual(Buffer.from(await bounded(download.arrayBuffer())),
        await readFile(join(output, basename(previousPath))));
      previousOutput = { status: download.status, match: true };
    } else {
      assert.equal(download.status, 404);
      const missing = await bounded(download.json());
      assert.deepEqual(missing, { reason: 'missing_output' });
      previousOutput = { status: download.status, reason: missing.reason };
    }
    const nextResponse = await bounded(run(base, 'ok', client.signal));
    assert.equal(nextResponse.status, 200);
    const restartedClient = await bounded(Array.fromAsync(readProgress(nextResponse.body)));
    const next = restartedClient.at(-1);
    assert.equal(next.status, 'ok');
    assert.notEqual(next.runId, end.runId);
    const restartedResult = await verifyFile(join(output, basename(next.outputPath)));
    const files = await readdir(output);
    assert.equal(files.length, originals.length + 1);
    assert.equal(files.every((name) => name.endsWith('.ndjson.gz')), true);
    await bounded(replacement.shutdown({ graceMs: 1000 }));

    const summary = {
      scenario, restart: 'new-server-same-process', pid: process.pid, port,
      graceMs, refused, graceExpired: stopResult.graceExpired,
      ...(shutdownPendingAtDeadline === undefined ? {} : { shutdownPendingAtDeadline }),
      original: {
        httpStatus: response.status, body: 'complete', runId: end.runId,
        status: end.status, reason: end.reason, result: originalResult,
        temporaryDirectories: 0, serverClosed: !server.listening,
      },
      replacement: {
        samePort: true, sameWorkspace: true, preservedResults: originals.length,
        previousOutput,
        runId: next.runId, status: next.status, result: restartedResult,
        finalFiles: files.length, serverClosed: !replacement.listening,
      },
    };
    await writeEvents(join(directory, 'server.ndjson'), originalEvents);
    await writeEvents(join(directory, 'client.ndjson'), clientEvents);
    await writeEvents(join(directory, 'replacement-server.ndjson'), replacementEvents);
    await writeEvents(join(directory, 'replacement-client.ndjson'), restartedClient);
    await writeFile(join(directory, 'summary.json'), JSON.stringify(summary, null, 2) + '\n');
    return summary;
  } finally {
    releaseWrite();
    publication.resolve();
    client.abort();
    await clientCapture?.catch(() => {});
    await server.shutdown({ graceMs: 0 });
    await replacement?.shutdown({ graceMs: 0 });
  }
}

export async function captureRestart() {
  const directory = await mkdtemp(join(tmpdir(), 'streams-restart-'));
  try {
    const cases = [];
    for (const [index, scenario] of scenarios.entries()) {
      cases.push(await captureCase(join(directory, 'case-' + (index + 1)), scenario));
    }
    return { directory, cases };
  } catch (error) {
    await rm(directory, { recursive: true, force: true });
    throw error;
  }
}

const entry = process.argv[1];
const modulePath = fileURLToPath(import.meta.url);
if (entry && (resolve(entry) === modulePath ||
    await realpath(entry).catch(() => null) === modulePath)) {
  const { directory, cases } = await captureRestart();
  for (const summary of cases) console.log(JSON.stringify(summary));
  console.log(JSON.stringify({ evidenceDirectory: directory }));
}
