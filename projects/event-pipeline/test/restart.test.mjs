import assert from 'node:assert/strict';
import { readFile, readdir, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { test } from 'node:test';
import { captureRestart } from '../capture-restart.mjs';

test('shutdown drains or cancels before publication and respects a publication already started',
  { timeout: 20_000 }, async (t) => {
    const { directory, cases } = await captureRestart();
    t.after(() => rm(directory, { recursive: true, force: true }));
    assert.equal(cases.length, 3);
    for (const [index, summary] of cases.entries()) {
      assert.equal(summary.restart, 'new-server-same-process');
      assert.equal(summary.pid, process.pid);
      assert.deepEqual(summary.refused, {
        status: 503, body: { reason: 'shutting_down' }, complete: true,
      });
      assert.equal(summary.original.body, 'complete');
      assert.equal(summary.original.serverClosed, true);
      assert.equal(summary.replacement.serverClosed, true);
      assert.equal(summary.replacement.samePort, true);
      assert.equal(summary.replacement.sameWorkspace, true);
      assert.notEqual(summary.original.runId, summary.replacement.runId);
      assert.deepEqual(summary.replacement.result, { bytes: 356, records: 8, match: true });
      const caseDir = join(directory, 'case-' + (index + 1));
      const entries = (await readFile(join(caseDir, 'server.ndjson'), 'utf8'))
        .trimEnd().split('\n').map((line) => JSON.parse(line));
      const eventNames = entries.map((entry) => entry.event);
      assert.equal(eventNames.filter((name) => name === 'run.start').length, 1);
      assert.equal(eventNames.filter((name) => name === 'run.end').length, 1);
      assert.ok(eventNames.indexOf('server.draining') < eventNames.indexOf('run.end'));
      assert.ok(eventNames.indexOf('run.end') < eventNames.indexOf('server.stopped'));
      assert.equal((await readdir(join(caseDir, 'output'))).every((name) => name.endsWith('.gz')), true);
      assert.deepEqual(JSON.parse(await readFile(join(caseDir, 'summary.json'), 'utf8')), summary);
    }
    assert.deepEqual(cases.map(({ graceExpired }) => graceExpired), [false, true, true]);
    assert.deepEqual(cases.map(({ original }) => original.status), ['ok', 'aborted', 'ok']);
    assert.deepEqual(cases.map(({ replacement }) => replacement.preservedResults), [1, 0, 1]);
    assert.deepEqual(cases.map(({ replacement }) => replacement.finalFiles), [2, 1, 2]);
    assert.deepEqual(cases.map(({ replacement }) => replacement.previousOutput), [
      { status: 200, match: true },
      { status: 404, reason: 'missing_output' },
      { status: 200, match: true },
    ]);
    assert.equal(cases[1].original.reason, 'shutdown_deadline');
    assert.equal(cases[1].original.result, null);
    assert.equal(cases[2].shutdownPendingAtDeadline, true);
    assert.deepEqual(cases[2].original.result, { bytes: 356, records: 8, match: true });
  });
