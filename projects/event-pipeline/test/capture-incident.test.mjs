import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFile, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { gunzipSync } from 'node:zlib';

const exec = promisify(execFile);
const capture = fileURLToPath(new URL('../capture-incident.mjs', import.meta.url));
const demoDigest = 'fc127eb395903d16763493bfc297ccdf57f00bd91dde9c5c41876261ec5e26ef';

async function readEvents(directory, file) {
  return (await readFile(join(directory, file), 'utf8'))
    .trimEnd().split('\n').map((line) => JSON.parse(line));
}

test('incident evidence separates lost confirmation, changed content, and early cancellation',
  { timeout: 15000 }, async (t) => {
    const { stdout } = await exec(process.execPath, [capture], { timeout: 12000 });
    const root = stdout.trimEnd();
    assert.equal(dirname(root), resolve(tmpdir()));
    assert.match(basename(root), /^streams-incident-[A-Za-z0-9]{6}$/);
    t.after(() => rm(root, { recursive: true, force: true }));
    for (const [name, body, status, match] of [
      ['case-1', 'aborted', 'ok', true],
      ['case-2', 'complete', 'ok', false],
      ['case-3', 'aborted', 'aborted', null],
    ]) {
      const directory = join(root, name);
      const actual = JSON.parse(await readFile(join(directory, 'transfer.json'), 'utf8'));
      assert.deepEqual(actual, { httpStatus: 200, body });
      const client = await readEvents(directory, 'client.ndjson');
      const server = await readEvents(directory, 'server.ndjson');
      const end = server.at(-1);
      assert.deepEqual(client.map((entry) => entry.event), body === 'complete'
        ? ['run.start', 'run.progress', 'run.end'] : ['run.start', 'run.progress']);
      assert.equal(client[1].recordsRead, 1);
      assert.equal(client[0].runId, end.runId);
      assert.equal(end.status, status);
      const output = join(directory, 'output');
      if (match === null) {
        assert.equal(end.reason, 'client_disconnect');
        assert.deepEqual(await readdir(output), []);
        continue;
      }
      assert.equal(end.outputSha256, demoDigest);
      const bytes = gunzipSync(await readFile(join(output, basename(end.outputPath))));
      assert.equal(bytes.length, 356);
      assert.equal(bytes.toString('utf8').split('\n').filter(Boolean).length, 8);
      assert.equal(createHash('sha256').update(bytes).digest('hex') === demoDigest, match);
    }
  });
