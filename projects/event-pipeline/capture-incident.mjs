import assert from 'node:assert/strict';
import { EventEmitter, once } from 'node:events';
import { link, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { gunzipSync, gzipSync } from 'node:zlib';
import { readProgress } from './public/progress.mjs';
import { createDemoServer } from './server.mjs';

async function bounded(promise) {
  const timeout = new AbortController();
  try {
    return await Promise.race([
      promise,
      delay(8000, undefined, { signal: timeout.signal }).then(() => {
        throw new Error('The expected transition was not observed');
      }),
    ]);
  } finally {
    timeout.abort();
  }
}

async function capture(directory, scenario) {
  const serverEvents = [];
  const runEnded = Promise.withResolvers();
  const reachedBoundary = Promise.withResolvers();
  const closeObserved = Promise.withResolvers();
  const releasePublication = Promise.withResolvers();
  const heldWrite = new EventEmitter();
  const client = new AbortController();
  const server = await createDemoServer({
    workspace: directory,
    log(entry) {
      serverEvents.push(entry);
      if (entry.event === 'run.end') runEnded.resolve(entry);
    },
    async slowWrite(signal) {
      reachedBoundary.resolve();
      await once(heldWrite, 'release', { signal });
    },
    async publish(temporary, target) {
      if (scenario === 'during-publication') {
        reachedBoundary.resolve();
        await releasePublication.promise;
      }
      await link(temporary, target);
    },
  });
  server.once('request', (_req, res) => {
    res.once('close', closeObserved.resolve);
  });

  try {
    server.listen(0, '127.0.0.1');
    await once(server, 'listening');
    const mode = scenario === 'before-publication' ? 'slow' : 'ok';
    const response = await bounded(fetch('http://127.0.0.1:' + server.address().port + '/run?mode=' + mode, {
      method: 'POST',
      headers: { 'X-Stream-Demo': '1' },
      signal: client.signal,
    }));
    assert.equal(response.status, 200);
    const streamed = readProgress(response.body);
    const observed = [
      (await bounded(streamed.next())).value,
      (await bounded(streamed.next())).value,
    ];
    assert.equal(observed[1]?.event, 'run.progress');
    let body = 'complete';
    if (scenario === 'changed-file') {
      observed.push(...await bounded(Array.fromAsync(streamed)));
    } else {
      await bounded(reachedBoundary.promise);
      client.abort();
      await assert.rejects(bounded(streamed.next()), { name: 'AbortError' });
      await bounded(closeObserved.promise);
      releasePublication.resolve();
      body = 'aborted';
    }

    const end = await bounded(runEnded.promise);
    if (scenario === 'changed-file') {
      const output = join(directory, 'output', basename(end.outputPath));
      const original = gunzipSync(await readFile(output));
      const modified = Buffer.from(original.toString('utf8').replace('Grüße 1"', 'Grüße 9"'));
      assert.notDeepEqual(modified, original);
      assert.equal(modified.length, original.length);
      await writeFile(output, gzipSync(modified));
    }
    for (const [name, entries] of [['client', observed], ['server', serverEvents]]) {
      await writeFile(join(directory, name + '.ndjson'),
        entries.map((entry) => JSON.stringify(entry) + '\n').join(''));
    }
    await writeFile(join(directory, 'transfer.json'),
      JSON.stringify({ httpStatus: response.status, body }) + '\n');
  } finally {
    client.abort();
    releasePublication.resolve();
    const closed = new Promise((resolve) => server.close(resolve));
    server.closeAllConnections();
    await closed;
    if (serverEvents.length > 0 && serverEvents.at(-1).event !== 'run.end') {
      await bounded(runEnded.promise).catch(() => {});
    }
  }
}

const root = await mkdtemp(join(tmpdir(), 'streams-incident-'));
try {
  await capture(join(root, 'case-1'), 'during-publication');
  await capture(join(root, 'case-2'), 'changed-file');
  await capture(join(root, 'case-3'), 'before-publication');
  console.log(root);
} catch (error) {
  await rm(root, { recursive: true, force: true });
  throw error;
}
