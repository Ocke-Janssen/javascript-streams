import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { get } from 'node:http';
import { join } from 'node:path';

const [mode = 'echo'] = process.argv.slice(2);
const port = Number(process.env.PORT ?? (mode === 'echo' ? 3000 : 3001));
const base = `http://127.0.0.1:${port}`;
const sample = Buffer.from([0, 65, 255, 10, 0]);

async function send(path, options) {
  return fetch(base + path, { ...options, signal: AbortSignal.timeout(5_000) });
}

switch (mode) {
  case 'echo': {
    const response = await send('/echo', { method: 'POST', body: sample });
    assert.equal(response.status, 200);
    assert.deepEqual(Buffer.from(await response.arrayBuffer()), sample);
    console.log('status=200 sameBytes=true');
    break;
  }
  case 'download': {
    const response = await send('/download');
    assert.equal(response.status, 200);
    const expected = await readFile(new URL('../fixtures/events.ndjson', import.meta.url));
    assert.deepEqual(Buffer.from(await response.arrayBuffer()), expected);
    console.log(`status=200 bytes=${expected.length} sameBytes=true`);
    break;
  }
  case 'download-changed': {
    const response = await send('/download-changed');
    assert.equal(response.status, 200);
    const expected = await readFile(new URL('../fixtures/events.ndjson', import.meta.url));
    const actual = Buffer.from(await response.arrayBuffer());
    assert.equal(Number(response.headers.get('content-length')), expected.length);
    assert.equal(actual.length, expected.length);
    assert.equal(actual.equals(expected), false);
    const records = actual.toString('utf8').trimEnd().split('\n').map((line) => JSON.parse(line));
    assert.equal(records.length, 3);
    assert.equal(records[0].requestId, 'demo-10');
    console.log(`status=200 bytes=${actual.length} complete=true records=3 sameBytes=false`);
    break;
  }
  case 'download-fail': {
    await new Promise((resolve, reject) => {
      const request = get(base + '/download-fail', (response) => {
        let received = 0;
        let bodyError;
        response.on('data', (chunk) => { received += chunk.length; });
        response.on('error', (error) => { bodyError = error; });
        response.on('close', () => {
          try {
            assert.equal(response.statusCode, 200);
            assert.equal(Number(response.headers['content-length']), 131_072);
            assert.equal(received, 16_384);
            assert.equal(response.complete, false);
            assert.ok(bodyError);
            console.log(`status=200 bytes=${received} complete=false`);
            resolve();
          } catch (error) {
            reject(error);
          }
        });
      });
      request.on('error', reject);
      request.setTimeout(5_000, () => request.destroy(new Error('download timed out')));
    });
    break;
  }
  case 'upload': {
    const response = await send('/upload', {
      method: 'POST',
      headers: { 'Content-Type': 'application/octet-stream' },
      body: sample,
    });
    assert.equal(response.status, 201);
    assert.deepEqual(await response.json(), { bytes: 5 });
    const target = join(process.env.UPLOAD_DIR ?? 'scratch/08-upload', 'received.bin');
    assert.deepEqual(await readFile(target), sample);
    console.log('status=201 bytes=5 sameBytes=true');
    break;
  }
  default:
    console.error('Usage: node examples/08-http-client.mjs [echo|download|download-changed|download-fail|upload]');
    process.exitCode = 1;
}
