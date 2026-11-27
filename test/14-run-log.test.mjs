import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { copyFileSync, existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { summarizeRunLog } from '../examples/14-run-log.mjs';

const summarize = fileURLToPath(new URL('../examples/14-summarize.mjs', import.meta.url));
const prepare = new URL('../examples/14-prepare-diagnosis.mjs', import.meta.url);
const parser = new URL('../examples/14-run-log.mjs', import.meta.url);

function records(runId = 'demo-1', end = {}) {
  return [
    { runId, event: 'run.start', elapsedMs: 0 },
    {
      runId, event: 'run.end', elapsedMs: 10, status: 'ok', reason: 'completed',
      readBytes: 16, recordsRead: 2, processed: 1, dropped: 1, rejected: 0,
      peakSinkPendingRecords: 0, sourceWaitMs: 8, sinkWriteMs: 8, ...end,
    },
  ];
}

function ndjson(rows) {
  return Buffer.from(rows.map(row => JSON.stringify(row)).join('\n') + '\n');
}

function temporary(t) {
  const directory = mkdtempSync(join(tmpdir(), 'streams-run-log-test-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  return directory;
}

test('accepts a blind run with equal timestamps, overlapping phases, and a zero sampled peak', () => {
  const rows = records();
  rows.splice(1, 0, { runId: 'demo-1', event: 'sink.first', elapsedMs: 10, processed: 1 });
  const summary = summarizeRunLog(ndjson(rows), 'valid.ndjson');
  assert.equal(summary.runId, 'demo-1');
  assert.equal(summary.mode, undefined);
  assert.equal(summary.peakSinkPendingRecords, 0);
  assert.equal(summary.sourceWaitMs + summary.sinkWriteMs, 16);

  const aborted = records('demo-2', {
    status: 'aborted', reason: 'client_disconnect', recordsRead: 3,
  });
  assert.equal(summarizeRunLog(ndjson(aborted), 'abort.ndjson').recordsRead, 3);
});

test('rejects missing measurements, contradictory outcomes, and impossible progress', () => {
  for (const [fields, message] of [
    [{ processed: undefined }, /missing or invalid measurements/],
    [{ reason: 'client_disconnect' }, /invalid status or reason/],
    [{ processed: 3 }, /inconsistent measurements/],
    [{ sourceWaitMs: 11 }, /inconsistent measurements/],
  ]) {
    assert.throws(() => summarizeRunLog(ndjson(records('demo-1', fields)), 'bad.ndjson'), message);
  }

  const rows = records();
  rows.splice(1, 0, { runId: 'demo-1', elapsedMs: 5, pendingRecords: 1 });
  assert.throws(() => summarizeRunLog(ndjson(rows), 'unnamed.ndjson'), /complete, ordered run/);
  rows[1].event = 'sink.queue';
  assert.throws(() => summarizeRunLog(ndjson(rows), 'wrong-peak.ndjson'), /contradicts run.end/);
});

test('distinguishes missing terminal records, broken JSON, and invalid UTF-8 without exposing data', () => {
  const [start] = records();
  assert.throws(() => summarizeRunLog(ndjson([start]), 'missing.ndjson'), /complete, ordered run/);
  const damaged = Buffer.concat([ndjson([start]), Buffer.from('{"private":"synthetic data')]);
  assert.throws(() => summarizeRunLog(damaged, 'truncated.ndjson'), {
    message: 'truncated.ndjson: invalid JSON on line 2',
  });
  assert.throws(() => summarizeRunLog(Buffer.of(0xff), 'encoding.ndjson'), /invalid UTF-8 in log/);
  const completeWithoutLF = ndjson(records()).subarray(0, -1);
  assert.equal(summarizeRunLog(completeWithoutLF, 'no-final-lf.ndjson').status, 'ok');
});

test('emits no summaries for a multi-file input with an invalid or repeated run', t => {
  const directory = temporary(t);
  const valid = join(directory, 'valid.ndjson');
  const another = join(directory, 'another.ndjson');
  writeFileSync(valid, ndjson(records()));
  writeFileSync(another, ndjson(records('demo-2', { status: 'unknown' })));

  const invalid = spawnSync(process.execPath, [summarize, valid, another], { encoding: 'utf8' });
  assert.equal(invalid.status, 1, invalid.stderr);
  assert.equal(invalid.stdout, '');
  assert.match(invalid.stderr, /invalid status or reason/);

  writeFileSync(another, ndjson(records()));
  const duplicate = spawnSync(process.execPath, [summarize, valid, another], { encoding: 'utf8' });
  assert.equal(duplicate.status, 1, duplicate.stderr);
  assert.equal(duplicate.stdout, '');
  assert.match(duplicate.stderr, /duplicate run ID/);
});

test('the preparation helper cannot pass an empty exit-1 log off as an invalid JSON case', t => {
  const directory = temporary(t);
  const preparation = join(directory, '14-prepare-diagnosis.mjs');
  copyFileSync(prepare, preparation);
  copyFileSync(parser, join(directory, '14-run-log.mjs'));
  const successLogs = Object.fromEntries(['slow-source', 'slow-sink'].map(mode =>
    [mode, ndjson(records(mode)).toString()]));
  writeFileSync(join(directory, '14-observe-pipeline.mjs'), `
const mode = process.argv[2];
if (mode === 'bad-record') process.exitCode = 1;
else process.stdout.write(${JSON.stringify(successLogs)}[mode]);
`);
  const result = spawnSync(process.execPath, [preparation], { cwd: directory, encoding: 'utf8' });
  assert.equal(result.status, 1, result.stderr);
  assert.equal(result.stdout, '');
  assert.match(result.stderr, /bad-record: invalid JSON on line 1/);
  assert.equal(existsSync(join(directory, 'scratch')), false);
});
