import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { cpus, release, tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { fileURLToPath } from 'node:url';

const jobCount = 6;
const inputBytes = 4 * 1024 * 1024;
const blockBytes = 256 * 1024;
const delayMs = 4;
const trials = 3;
const modes = [
  { name: 'all-at-once', slots: jobCount },
  { name: 'two-at-once', slots: 2 },
];

function fixture() {
  const input = Buffer.allocUnsafe(inputBytes);
  for (let offset = 0; offset < input.length; offset += blockBytes) {
    input.fill(offset / blockBytes, offset, offset + blockBytes);
  }
  return input;
}

function* blocks(input) {
  for (let offset = 0; offset < input.length; offset += blockBytes) {
    yield input.subarray(offset, offset + blockBytes);
  }
}

async function importBuffer(input) {
  const digest = createHash('sha256');
  let bytes = 0;
  const sink = new Writable({
    highWaterMark: blockBytes,
    write(chunk, _encoding, done) {
      setTimeout(() => {
        digest.update(chunk);
        bytes += chunk.length;
        done();
      }, delayMs);
    },
  });
  const source = Readable.from(blocks(input), {
    objectMode: false,
    highWaterMark: blockBytes,
  });
  await pipeline(source, sink);
  return { bytes, sha256: digest.digest('hex') };
}

async function measure(mode, inputPath) {
  const file = await stat(inputPath);
  assert.ok(file.isFile() && file.size === inputBytes, 'expected the generated input file');

  const jobs = [];
  let nextJob = 0;
  let activeJobs = 0;
  let peakActiveJobs = 0;
  let heldInputBytes = 0;
  let peakHeldInputBytes = 0;
  const submittedAt = performance.now();

  async function takeJobs() {
    while (nextJob < jobCount) {
      const job = nextJob++;
      const queueWaitMs = performance.now() - submittedAt;
      activeJobs += 1;
      peakActiveJobs = Math.max(peakActiveJobs, activeJobs);
      let heldByJob = 0;
      try {
        const input = await readFile(inputPath);
        assert.equal(input.length, inputBytes);
        heldByJob = input.length;
        heldInputBytes += heldByJob;
        peakHeldInputBytes = Math.max(peakHeldInputBytes, heldInputBytes);
        const result = await importBuffer(input);
        jobs[job] = { job: job + 1, queueWaitMs, elapsedMs: performance.now() - submittedAt, ...result };
      } finally {
        heldInputBytes -= heldByJob;
        activeJobs -= 1;
      }
    }
  }

  const workers = await Promise.allSettled(Array.from({ length: mode.slots }, takeJobs));
  const failed = workers.find((worker) => worker.status === 'rejected');
  if (failed) throw failed.reason;
  const batchMs = performance.now() - submittedAt;
  return {
    jobs, peakActiveJobs, peakHeldInputBytes, batchMs,
    maxQueueWaitMs: Math.max(...jobs.map((job) => job.queueWaitMs)),
    peakRssKiB: process.resourceUsage().maxRSS,
  };
}

function emit(record) {
  console.log(JSON.stringify(record));
}

function middleSample(samples, field) {
  const values = samples.map((sample) => sample[field]).sort((a, b) => a - b);
  return values[Math.floor(values.length / 2)];
}

async function run() {
  const folder = await mkdtemp(join(tmpdir(), 'streams-concurrent-imports-'));
  try {
    const inputPath = join(folder, 'input.bin');
    const bytes = fixture();
    const expectedSha256 = createHash('sha256').update(bytes).digest('hex');
    await writeFile(inputPath, bytes);
    emit({
      type: 'environment', node: process.version, platform: process.platform,
      release: release(), arch: process.arch, cpu: cpus()[0]?.model,
      jobCount, inputBytes, blockBytes, delayMs, trials, warmupsPerMode: 1, expectedSha256,
    });

    const samples = new Map(modes.map((mode) => [mode.name, []]));
    for (let trial = 0; trial <= trials; trial += 1) {
      const order = trial % 2 ? [...modes].reverse() : modes;
      for (const mode of order) {
        const result = JSON.parse(execFileSync(process.execPath, [
          fileURLToPath(import.meta.url), '--worker', mode.name, inputPath,
        ], { encoding: 'utf8', timeout: 15_000 }));
        assert.equal(result.jobs.length, jobCount);
        for (const job of result.jobs) {
          assert.equal(job.bytes, inputBytes);
          assert.equal(job.sha256, expectedSha256);
        }
        assert.equal(result.peakActiveJobs, mode.slots);
        assert.ok(result.peakHeldInputBytes <= mode.slots * inputBytes);
        if (trial !== 0) {
          samples.get(mode.name).push(result);
          emit({ type: 'sample', trial, mode: mode.name, ...result });
        }
      }
    }

    for (const [mode, runs] of samples) {
      emit({
        type: 'summary', mode, verifiedJobsPerBatch: jobCount,
        peakActiveJobs: middleSample(runs, 'peakActiveJobs'),
        medianPeakHeldInputBytes: middleSample(runs, 'peakHeldInputBytes'),
        medianMaxQueueWaitMs: middleSample(runs, 'maxQueueWaitMs'),
        medianBatchMs: middleSample(runs, 'batchMs'),
        medianPeakRssKiB: middleSample(runs, 'peakRssKiB'),
      });
    }
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
}

const [command, modeName, inputPath, ...extra] = process.argv.slice(2);
if (command === undefined) {
  await run();
} else if (command === '--worker' && inputPath && extra.length === 0) {
  const mode = modes.find(({ name }) => name === modeName);
  assert.ok(mode, 'unknown worker mode');
  emit(await measure(mode, inputPath));
} else {
  console.error('Usage: node benchmarks/13-concurrent-imports.mjs');
  process.exitCode = 1;
}
