import { readFile } from 'node:fs/promises';

const args = process.argv.slice(2);
const pairFlags = args.filter((argument) => argument === '--pairs');
const paths = args.filter((argument) => argument !== '--pairs');
if (pairFlags.length > 1 || paths.length > 1 || paths.some((argument) => argument.startsWith('--'))) {
  throw new Error('Usage: node benchmarks/13-summarize.mjs [--pairs] [input.ndjson]');
}
const showPairs = pairFlags.length === 1;
const path = paths[0] ?? 'scratch/bench-13.ndjson';
const [environment, ...samples] = (await readFile(path, 'utf8')).trim().split('\n').map(JSON.parse);
const modes = ['buffered', 'stream', 'unpaced'];
const MiB = 1024 * 1024;

function isPositiveInteger(value) {
  return Number.isSafeInteger(value) && value > 0;
}

function hasDistinctPositiveIntegers(values) {
  return Array.isArray(values) && values.length > 0 &&
    values.every(isPositiveInteger) && new Set(values).size === values.length;
}

function isNonNegativeInteger(value) {
  return Number.isSafeInteger(value) && value >= 0;
}

function isNonEmptyString(value) {
  return typeof value === 'string' && value.trim().length > 0;
}

if (environment?.type !== 'environment' || !hasDistinctPositiveIntegers(environment.inputMiB) ||
    !hasDistinctPositiveIntegers(environment.sinkDelayMs) ||
    !isPositiveInteger(environment.blockBytes) || !isPositiveInteger(environment.trials) ||
    environment.trials > 8 ||
    (environment.node !== undefined && !isNonEmptyString(environment.node)) ||
    (environment.runId !== undefined && !isNonEmptyString(environment.runId)) ||
    (environment.cpuRounds !== undefined && !isNonNegativeInteger(environment.cpuRounds)) ||
    (environment.highWaterMark !== undefined &&
      (!isPositiveInteger(environment.highWaterMark) ||
       environment.highWaterMark % environment.blockBytes !== 0))) {
  throw new Error('invalid benchmark environment');
}

function validateMeasurements(entry, identity) {
  const { firstMs, totalMs, throughputMiBps, peakRssKiB, peakPendingBytes, cpuMs } = entry;
  if (!Number.isFinite(firstMs) || firstMs <= 0 ||
      !Number.isFinite(totalMs) || totalMs < firstMs) {
    throw new Error('invalid benchmark duration: ' + identity);
  }

  const expectedThroughput = (entry.bytes / MiB) / (totalMs / 1000);
  if (!Number.isFinite(expectedThroughput) || !Number.isFinite(throughputMiBps) ||
      Math.abs(throughputMiBps / expectedThroughput - 1) > 1e-9) {
    throw new Error('inconsistent benchmark throughput: ' + identity);
  }

  const queueLimit = entry.mode === 'unpaced'
    ? entry.bytes
    : Math.min(entry.bytes, environment.highWaterMark ?? entry.bytes);
  if (!isNonNegativeInteger(peakRssKiB) || !isPositiveInteger(peakPendingBytes) ||
      peakPendingBytes % environment.blockBytes !== 0 || peakPendingBytes > queueLimit) {
    throw new Error('invalid benchmark memory measurements: ' + identity);
  }

  if (cpuMs != null && (!Number.isFinite(cpuMs) || cpuMs < 0)) {
    throw new Error('invalid benchmark CPU time: ' + identity);
  }
}

const groups = new Map();
const seen = new Set();
const digests = new Map();
const failures = [];
function groupKey(size, delay, mode) {
  return `size=${size}MiB delay=${delay}ms mode=${mode}`;
}

for (const entry of samples) {
  if (!['sample', 'failure'].includes(entry?.type) || !environment.inputMiB.includes(entry.sizeMiB) ||
      !environment.sinkDelayMs.includes(entry.delayMs) || !modes.includes(entry.mode) ||
      !Number.isSafeInteger(entry.trial) || entry.trial < 0 || entry.trial > environment.trials) {
    throw new Error('invalid benchmark sample');
  }

  const key = groupKey(entry.sizeMiB, entry.delayMs, entry.mode);
  const identity = key + ' trial=' + entry.trial;
  if (entry.runId !== environment.runId) {
    throw new Error('benchmark sample belongs to a different run: ' + identity);
  }
  if (entry.type === 'failure') {
    failures.push(`${identity} (${entry.reason})`);
    continue;
  }
  if (entry.trial === 0) {
    throw new Error('invalid benchmark sample: ' + identity);
  }
  if (seen.has(identity)) throw new Error('duplicate benchmark sample: ' + identity);
  seen.add(identity);

  const bytes = entry.sizeMiB * MiB;
  if (!isPositiveInteger(bytes) || entry.bytes !== bytes || !isPositiveInteger(entry.writes) ||
      entry.writes !== bytes / environment.blockBytes ||
      typeof entry.sha256 !== 'string' || !/^[a-f0-9]{64}$/.test(entry.sha256) ||
      (digests.has(entry.sizeMiB) && digests.get(entry.sizeMiB) !== entry.sha256)) {
    throw new Error('incomparable benchmark sample: ' + identity);
  }
  validateMeasurements(entry, identity);
  digests.set(entry.sizeMiB, entry.sha256);
  if (!groups.has(key)) groups.set(key, []);
  groups.get(key).push(entry);
}

const expected = environment.inputMiB.length * environment.sinkDelayMs.length * modes.length * environment.trials;
if (seen.size !== expected || failures.length !== 0) {
  for (const failure of failures) console.error('failed: ' + failure);
  for (const size of environment.inputMiB) {
    for (const delay of environment.sinkDelayMs) {
      for (const mode of modes) {
        const key = groupKey(size, delay, mode);
        const missing = Array.from({ length: environment.trials }, (_, i) => i + 1)
          .filter((trial) => !seen.has(key + ' trial=' + trial));
        if (missing.length !== 0) console.error(`no valid sample: ${key} trials=${missing.join(',')}`);
      }
    }
  }
  throw new Error(`incomplete benchmark: expected ${expected} samples, found ${seen.size}`);
}

function median(rows, field) {
  const values = rows.map((row) => row[field]).sort((a, b) => a - b);
  const mid = Math.floor(values.length / 2);
  return values.length % 2 ? values[mid] : (values[mid - 1] + values[mid]) / 2;
}

for (const [key, rows] of groups) {
  const durations = rows.map((row) => row.totalMs);
  const hasCpu = rows.every((row) => Number.isFinite(row.cpuMs));
  console.log([
    key, `n=${rows.length}`,
    `firstMs=${median(rows, 'firstMs').toFixed(1)}`,
    `totalMs=${median(rows, 'totalMs').toFixed(1)}`,
    `minTotalMs=${Math.min(...durations).toFixed(1)}`,
    `maxTotalMs=${Math.max(...durations).toFixed(1)}`,
    `MiBps=${median(rows, 'throughputMiBps').toFixed(1)}`,
    `peakRssKiB=${median(rows, 'peakRssKiB').toFixed(0)}`,
    `pendingKiB=${(median(rows, 'peakPendingBytes') / 1024).toFixed(0)}`,
    ...(hasCpu ? [`cpuMs=${median(rows, 'cpuMs').toFixed(1)}`] : []),
  ].join(' '));
}

if (showPairs) {
  for (const size of environment.inputMiB) {
    for (const delay of environment.sinkDelayMs) {
      const streamRows = groups.get(groupKey(size, delay, 'stream'));
      const unpacedByTrial = new Map(
        groups.get(groupKey(size, delay, 'unpaced')).map((row) => [row.trial, row]),
      );
      const deltas = streamRows
        .map((row) => ({
          trial: row.trial,
          deltaMs: unpacedByTrial.get(row.trial).totalMs - row.totalMs,
        }))
        .sort((a, b) => a.trial - b.trial);
      console.log([
        `size=${size}MiB delay=${delay}ms pair=unpaced-minus-stream`,
        `deltaMs=${deltas.map((row) => row.deltaMs.toFixed(1)).join(',')}`,
        `medianDeltaMs=${median(deltas, 'deltaMs').toFixed(1)}`,
      ].join(' '));
    }
  }
}
