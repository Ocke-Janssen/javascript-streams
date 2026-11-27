import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import { request } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';

const storage = await mkdtemp(join(tmpdir(), 'streams-upload-'));
const target = join(storage, 'received.bin');
const headers = { 'Content-Type': 'application/octet-stream' };
const server = spawn(process.execPath, [
  fileURLToPath(new URL('./08-http-files.mjs', import.meta.url)),
], {
  env: { ...process.env, PORT: '0', UPLOAD_DIR: storage },
  stdio: ['ignore', 'pipe', 'pipe'],
});
const closed = once(server, 'close');
const stdout = createInterface({ input: server.stdout });
const stderr = createInterface({ input: server.stderr });
const pending = [];

for (const channel of [stdout, stderr]) {
  channel.on('line', (line) => pending.push(line));
}

async function waitForLine(predicate) {
  const timer = AbortSignal.timeout(5_000);
  // A line can arrive before we start waiting for it.
  while (true) {
    const index = pending.findIndex(predicate);
    if (index !== -1) return pending.splice(index, 1)[0];
    if (server.exitCode !== null || server.signalCode !== null) {
      throw new Error(`server exited: ${pending.join('\n')}`);
    }
    await delay(10, undefined, { signal: timer });
  }
}

let stalled;
try {
  const listening = await waitForLine((line) => line.startsWith('Listening on '));
  const url = listening.slice('Listening on '.length) + '/upload';
  const previous = Buffer.from([0, 65, 255, 10, 0]);
  const newer = Buffer.from([0, 88, 10]);
  function send(body) {
    return fetch(url, { method: 'POST', headers, body, signal: AbortSignal.timeout(5_000) });
  }

  const first = await send(previous);
  assert.equal(first.status, 201);
  assert.deepEqual(await first.json(), { bytes: 5 });
  await waitForLine((line) => line === 'upload received first bytes');
  await waitForLine((line) => line === 'upload slot released');

  // Supply one byte, then leave the request body open.
  let stalledResult;
  stalled = request(url, { method: 'POST', headers }, (response) => {
    stalledResult = response.statusCode;
    response.resume();
  });
  stalled.on('error', (error) => { stalledResult = error.code; });
  stalled.setTimeout(5_000, () => stalled.destroy(new Error('stalled client timed out')));
  const stalledClosed = new Promise((resolve) => stalled.once('close', resolve));
  stalled.write(Buffer.from([1]));
  await waitForLine((line) => line === 'upload received first bytes');

  const competing = await send(newer);
  assert.equal(competing.status, 503);
  await competing.arrayBuffer();
  assert.deepEqual(await readFile(target), previous);
  console.log('competing status=503 previousBytesIntact=true');

  const oversizedWhileBusy = await send(Buffer.alloc(65_537));
  assert.equal(oversizedWhileBusy.status, 413);
  await oversizedWhileBusy.arrayBuffer();
  const health = await fetch(new URL('/health', url), { signal: AbortSignal.timeout(5_000) });
  assert.equal(health.status, 200);
  assert.deepEqual(await health.json(), { ok: true });
  // A second busy response confirms the stalled upload still holds the slot.
  const stillBusy = await send(newer);
  assert.equal(stillBusy.status, 503);
  await stillBusy.arrayBuffer();
  assert.deepEqual(await readFile(target), previous);
  console.log('busy oversized=413 health=200 previousBytesIntact=true');

  const stopped = await waitForLine((line) => line.startsWith('upload stopped:'));
  assert.match(stopped, /deadline=true/);
  await stalledClosed;
  assert.ok(stalledResult === 408 || typeof stalledResult === 'string', String(stalledResult));
  await waitForLine((line) => line === 'upload slot released');
  assert.deepEqual(await readdir(storage), ['received.bin']);
  assert.deepEqual(await readFile(target), previous);
  console.log('stalled deadline=true temporaryRemoved=true previousBytesIntact=true');
  console.log(`stalled client=${stalledResult}`);

  const next = await send(newer);
  assert.equal(next.status, 201);
  assert.deepEqual(await next.json(), { bytes: 3 });
  await waitForLine((line) => line === 'upload slot released');
  assert.deepEqual(await readFile(target), newer);
  console.log('follow-up status=201 sameBytes=true');

  for (const size of [65_535, 65_536]) {
    const body = Buffer.alloc(size, 0x61);
    const accepted = await send(body);
    assert.equal(accepted.status, 201);
    assert.deepEqual(await accepted.json(), { bytes: size });
    await waitForLine((line) => line === 'upload slot released');
    assert.deepEqual(await readFile(target), body);
    console.log(`size=${size} status=201 sameBytes=true`);
  }
  const oversized = await send(Buffer.alloc(65_537));
  assert.equal(oversized.status, 413);
  await oversized.arrayBuffer();
  assert.deepEqual(await readFile(target), Buffer.alloc(65_536, 0x61));
  console.log('size=65537 status=413 previousBytesIntact=true');

  const chunkedResult = await new Promise((resolve, reject) => {
    const upload = request(url, { method: 'POST', headers }, (response) => {
      response.resume();
      response.once('end', () => resolve(response.statusCode));
    });
    upload.once('error', (error) => resolve(error.code));
    upload.setTimeout(5_000, () => upload.destroy(new Error('chunked upload timed out')));
    upload.write(Buffer.alloc(65_536));
    upload.end(Buffer.from([0x62]));
    upload.once('close', () => reject(new Error('chunked upload closed without result')));
  });
  assert.ok(chunkedResult === 413 || ['ECONNRESET', 'EPIPE'].includes(chunkedResult));
  const overflow = await waitForLine((line) => line.startsWith('upload stopped:'));
  assert.match(overflow, /UPLOAD_LIMIT/);
  await waitForLine((line) => line === 'upload slot released');
  assert.deepEqual(await readdir(storage), ['received.bin']);
  assert.deepEqual(await readFile(target), Buffer.alloc(65_536, 0x61));
  console.log('chunked size=65537 limitDetected=true previousBytesIntact=true');
} finally {
  stalled?.destroy();
  server.kill();
  await closed;
  await rm(storage, { recursive: true, force: true });
}
