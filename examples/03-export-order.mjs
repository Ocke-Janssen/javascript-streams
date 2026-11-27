import assert from 'node:assert/strict';
import { mkdtemp, readFile, rename, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { gunzipSync } from 'node:zlib';
import { compressFile } from './03-gzip-file.mjs';

async function prepare(directory, version, permission) {
  const input = join(directory, `events-${version}.ndjson`);
  const archive = join(directory, `events-${version}.gz`);
  const bytes = Buffer.from(JSON.stringify({ version }) + '\n');
  await writeFile(input, bytes);
  await compressFile(input, archive, {
    async beforeRename(partial) {
      assert.deepEqual(gunzipSync(await readFile(partial)), bytes);
      // Hold the verified archive before returning it for publication.
      await permission;
    },
  });
  return archive;
}

async function readVersion(target) {
  const bytes = gunzipSync(await readFile(target));
  return JSON.parse(bytes.toString('utf8')).version;
}

function coordinatedPublisher(target) {
  let queue = Promise.resolve();
  return (archive, version) => {
    const result = queue.then(async () => {
      const currentVersion = await readVersion(target);
      if (version <= currentVersion) return 'skipped';
      await rename(archive, target);
      return 'published';
    });
    // The caller sees this failure. Later jobs still get a turn.
    queue = result.catch(() => {});
    return result;
  };
}

async function runCase(mode, expectedVersion) {
  const directory = await mkdtemp(join(tmpdir(), 'streams-export-order-'));
  const target = join(directory, 'events.gz');
  try {
    await rename(await prepare(directory, 41), target);

    const publish = mode === 'coordinated' ? coordinatedPublisher(target) : async (archive) => {
      await rename(archive, target);
      return 'published';
    };

    async function exportVersion(version, permission) {
      console.log(`${mode} start=${version}`);
      const archive = await prepare(directory, version, permission);
      const action = await publish(archive, version);
      const targetVersion = await readVersion(target);
      console.log(`${mode} ${action}=${version} target=${targetVersion}`);
      return { action, targetVersion };
    }

    const older = Promise.withResolvers();
    const [first, second] = await Promise.allSettled([
      exportVersion(42, older.promise),
      exportVersion(43).finally(older.resolve),
    ]);
    if (second.status === 'rejected') throw second.reason;
    if (first.status === 'rejected') throw first.reason;
    assert.deepEqual(second.value, { action: 'published', targetVersion: 43 });
    const expectedAction = mode === 'coordinated' ? 'skipped' : 'published';
    assert.deepEqual(first.value, { action: expectedAction, targetVersion: expectedVersion });
    const finalVersion = await readVersion(target);
    assert.equal(finalVersion, expectedVersion);
    console.log(`${mode} final=${finalVersion}`);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

await runCase('unconditional', 42);
await runCase('coordinated', 43);
