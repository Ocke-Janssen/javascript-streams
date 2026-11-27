import assert from 'node:assert/strict';
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { gunzipSync } from 'node:zlib';
import { compressFile } from './03-gzip-file.mjs';

const input = fileURLToPath(new URL('../fixtures/events.ndjson', import.meta.url));
const directory = await mkdtemp(join(tmpdir(), 'streams-rename-'));
const target = join(directory, 'blocked.gz');
const marker = join(target, 'marker.txt');

try {
  // The compressor can create and write its temporary file beside this directory.
  // Renaming a file onto an existing directory is the operation that must fail.
  await mkdir(target);
  await writeFile(marker, 'previous data\n');
  let verifiedBytes = 0;
  await assert.rejects(compressFile(input, target, {
    async beforeRename(partial) {
      const decoded = gunzipSync(await readFile(partial));
      assert.deepEqual(decoded, await readFile(input));
      verifiedBytes = decoded.length;
    },
  }), (error) => {
    assert.equal(error.syscall, 'rename');
    console.log(`operation=rename code=${error.code}`);
    return true;
  });
  assert.equal(verifiedBytes, 170);
  assert.equal(await readFile(marker, 'utf8'), 'previous data\n');
  assert.deepEqual(await readdir(directory), ['blocked.gz']);
  console.log(`verifiedBytes=${verifiedBytes} markerUnchanged=true temporaryDirectories=0`);
} finally {
  await rm(directory, { recursive: true, force: true });
}
