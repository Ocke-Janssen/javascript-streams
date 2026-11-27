import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

const summarizer = fileURLToPath(new URL('../benchmarks/13-summarize.mjs', import.meta.url));
const environment = {
  type: 'environment', runId: 'series-1', inputMiB: [1], sinkDelayMs: [4],
  blockBytes: 65536, highWaterMark: 262144, trials: 1,
};
const samples = ['buffered', 'stream', 'unpaced'].map(mode => ({
  type: 'sample', runId: 'series-1', sizeMiB: 1, delayMs: 4, trial: 1, mode,
  bytes: 1048576, writes: 16, sha256: 'a'.repeat(64), firstMs: 20,
  totalMs: 1000, throughputMiBps: 1, peakRssKiB: 32768, peakPendingBytes: 262144,
}));

function summarize(t, manifest, rows, ...options) {
  const directory = mkdtempSync(join(tmpdir(), 'streams-bench-summary-test-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const path = join(directory, 'samples.ndjson');
  writeFileSync(path, [manifest, ...rows].map(entry => JSON.stringify(entry)).join('\n') + '\n');
  return spawnSync(process.execPath, [summarizer, path, ...options], { encoding: 'utf8' });
}

test('summarizes a complete series and older rows without run IDs', t => {
  const current = summarize(t, environment, samples, '--pairs');
  const { runId: _runId, ...legacyEnvironment } = environment;
  const legacySamples = samples.map(({ runId: _runId, ...fields }) => fields);
  const legacy = summarize(t, legacyEnvironment, legacySamples, '--pairs');
  assert.equal(current.status, 0, current.stderr);
  assert.equal(legacy.status, 0, legacy.stderr);
  assert.match(current.stdout, /pair=unpaced-minus-stream deltaMs=0\.0/);
  assert.equal(current.stdout, legacy.stdout);
});

test('fails before reporting a summary for invalid or mixed benchmark rows', t => {
  const invalidRows = [
    [{ totalMs: 0 }, /invalid benchmark duration/],
    [{ firstMs: 1001 }, /invalid benchmark duration/],
    [{ throughputMiBps: 100 }, /inconsistent benchmark throughput/],
    [{ peakPendingBytes: 262145 }, /invalid benchmark memory measurements/],
    [{ peakPendingBytes: 327680 }, /invalid benchmark memory measurements/],
    [{ cpuMs: -1 }, /invalid benchmark CPU time/],
    [{ runId: 'series-2' }, /belongs to a different run/],
  ];
  for (const [fields, message] of invalidRows) {
    const result = summarize(t, environment, [{ ...samples[0], ...fields }, ...samples.slice(1)]);
    assert.equal(result.status, 1, result.stdout);
    assert.equal(result.stdout, '');
    assert.match(result.stderr, message);
  }
});

test('rejects a second input file instead of silently ignoring it', t => {
  const result = summarize(t, environment, samples, 'another.ndjson');
  assert.equal(result.status, 1, result.stdout);
  assert.equal(result.stdout, '');
  assert.match(result.stderr, /Usage:/);
});
