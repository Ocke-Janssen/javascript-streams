import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { EventEmitter, once } from 'node:events';
import { mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { verifyDemoGzip } from './capture-restart.mjs';
import { readProgress } from './public/progress.mjs';

async function withDeadline(promise, description) {
  const timer = new AbortController();
  try {
    return await Promise.race([
      promise,
      delay(8000, undefined, { signal: timer.signal }).then(() => {
        throw new Error('Timed out waiting for ' + description);
      }),
    ]);
  } finally {
    timer.abort();
  }
}

function startServer(workspace, scenario, port = 0) {
  const worker = new URL('./capture-process-restart-worker.mjs', import.meta.url);
  const child = fork(worker, [workspace, scenario, String(port)], {
    execArgv: [], stdio: ['ignore', 'ignore', 'inherit', 'ipc'],
  });
  const entries = [];
  const messages = new EventEmitter();
  child.on('message', (entry) => {
    entries.push(entry);
    messages.emit(entry.event, entry);
  });
  const closed = once(child, 'close').then(([code, signal]) => ({ code, signal }));
  closed.catch(() => {});

  async function waitFor(event) {
    const existing = entries.find((entry) => entry.event === event);
    if (existing) return existing;
    const listener = new AbortController();
    try {
      return await withDeadline(Promise.race([
        once(messages, event, { signal: listener.signal }).then(([entry]) => entry),
        closed.then((status) => { throw new Error('Worker closed: ' + JSON.stringify(status)); }),
      ]), event);
    } finally {
      listener.abort();
    }
  }

  return { child, closed, entries, waitFor };
}

async function checkDownload(response, output, completed) {
  if (!completed) {
    assert.equal(response.status, 404);
    assert.deepEqual(await response.json(), { reason: 'missing_output' });
    return { status: 404, reason: 'missing_output' };
  }
  assert.equal(response.status, 200);
  const bytes = Buffer.from(await response.arrayBuffer());
  assert.deepEqual(bytes, await readFile(output));
  return { status: 200, ...verifyDemoGzip(bytes) };
}

async function writeEvents(file, entries) {
  await writeFile(file, entries.map((entry) => JSON.stringify(entry) + '\n').join(''));
}

async function captureCase(workspace, scenario) {
  const original = startServer(workspace, scenario);
  const servers = [original];
  const cancelRequests = new AbortController();
  const signal = AbortSignal.any([cancelRequests.signal, AbortSignal.timeout(12_000)]);
  const completed = scenario === 'within-grace';
  try {
    const { port, pid } = await original.waitFor('probe.ready');
    const base = 'http://127.0.0.1:' + port;
    const response = await fetch(base + '/run?mode=slow', {
      method: 'POST', headers: { 'X-Stream-Demo': '1' }, signal,
    });
    assert.equal(response.status, 200);
    const capture = Array.fromAsync(readProgress(response.body));
    capture.catch(() => {});
    await original.waitFor('probe.write_waiting');
    assert.equal(original.entries.some((entry) => entry.event === 'run.end'), false);

    assert.equal(original.child.kill('SIGTERM'), true);
    const [clientEvents, oldExit] = await withDeadline(Promise.all([capture, original.closed]),
      'the final response and original exit');
    assert.deepEqual(oldExit, { code: 0, signal: null });
    assert.equal((await original.waitFor('probe.stopping')).trigger, 'SIGTERM');
    assert.equal((await original.waitFor('server.draining')).activeRun, true);
    assert.equal((await original.waitFor('server.stopped')).graceExpired, !completed);
    const end = clientEvents.at(-1);
    assert.deepEqual([end.status, end.reason], completed
      ? ['ok', 'completed'] : ['aborted', 'shutdown_deadline']);
    const filename = end.runId + '.ndjson.gz';
    assert.deepEqual(await readdir(join(workspace, 'output')), completed ? [filename] : []);

    const replacement = startServer(workspace, 'replacement', port);
    servers.push(replacement);
    const ready = await replacement.waitFor('probe.ready');
    assert.equal(ready.port, port);
    assert.notEqual(ready.pid, pid);
    const download = await fetch(base + '/output/' + filename, { signal });
    const oldDownload = await checkDownload(download, join(workspace, 'output', filename), completed);
    const status = await fetch(base + '/run/' + end.runId, { signal });
    const statusLookup = { status: status.status, ...await status.json() };
    assert.deepEqual(statusLookup, { status: 404, reason: 'not_found' });
    assert.equal(replacement.child.kill('SIGTERM'), true);
    const newExit = await withDeadline(replacement.closed, 'the replacement exit');
    assert.deepEqual(newExit, { code: 0, signal: null });

    const summary = {
      scenario, port,
      original: { pid, received: 'SIGTERM', exit: oldExit, runId: end.runId,
        outcome: end.status + '/' + end.reason },
      replacement: { pid: ready.pid, exit: newExit, oldDownload, statusLookup },
    };
    await writeEvents(join(workspace, 'server.ndjson'), original.entries);
    await writeEvents(join(workspace, 'replacement-server.ndjson'), replacement.entries);
    await writeEvents(join(workspace, 'client.ndjson'), clientEvents);
    await writeFile(join(workspace, 'summary.json'), JSON.stringify(summary, null, 2) + '\n');
    return summary;
  } finally {
    cancelRequests.abort();
    await Promise.all(servers.map(async ({ child, closed }) => {
      if (child.pid !== undefined && child.exitCode === null && child.signalCode === null) {
        child.kill('SIGKILL');
      }
      await withDeadline(closed, 'worker cleanup');
    }));
  }
}

if (process.platform === 'win32') {
  throw new Error('This SIGTERM example requires POSIX signal semantics');
}

const directory = await mkdtemp(join(tmpdir(), 'streams-process-restart-'));
try {
  for (const scenario of ['within-grace', 'before-publication']) {
    const { original, replacement } = await captureCase(join(directory, scenario), scenario);
    const { oldDownload } = replacement;
    console.log(scenario);
    console.log('  old: SIGTERM, exit=' + original.exit.code + ', ' + original.outcome);
    const result = oldDownload.match ? 'match=true' : oldDownload.reason;
    console.log('  new: differentPid=true, download=' + oldDownload.status + ', ' + result);
  }
  console.log(JSON.stringify({ evidenceDirectory: directory }));
} catch (error) {
  await rm(directory, { recursive: true, force: true });
  throw error;
}
