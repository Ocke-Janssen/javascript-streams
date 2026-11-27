import { execFileSync } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { createReadStream } from 'node:fs';
import { mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { cpus, release, tmpdir } from 'node:os';
import { join } from 'node:path';
import { monitorEventLoopDelay } from 'node:perf_hooks';
import { Writable } from 'node:stream';
import { finished } from 'node:stream/promises';
import { fileURLToPath } from 'node:url';

const MiB = 1024 * 1024;
const blockBytes = 64 * 1024;
const seed = 0x13579bdf;
const sizesMiB = [1, 8];
const delaysMs = [1, 4];
const modes = ['buffered', 'stream', 'unpaced'];

function seededBytes(length) {
  const output = Buffer.allocUnsafe(length);
  let state = seed;
  for (let offset = 0; offset < length; offset += 4) {
    state = (Math.imul(1_664_525, state) + 1_013_904_223) >>> 0;
    output.writeUInt32LE(state, offset);
  }
  return output;
}

async function* blocks(input) {
  let block = Buffer.allocUnsafe(blockBytes);
  let used = 0;
  for await (const chunk of input) {
    let offset = 0;
    while (offset < chunk.length) {
      const amount = Math.min(blockBytes - used, chunk.length - offset);
      chunk.copy(block, used, offset, offset + amount);
      used += amount;
      offset += amount;
      if (used === blockBytes) {
        yield block;
        block = Buffer.allocUnsafe(blockBytes);
        used = 0;
      }
    }
  }
  if (used !== 0) yield block.subarray(0, used);
}

async function measure(mode, inputPath, delayMs, sampleLoop, queueBlocks, cpuRounds) {
  const info = await stat(inputPath);
  if (!modes.includes(mode) || !delaysMs.includes(delayMs) ||
      !info.isFile() || !sizesMiB.includes(info.size / MiB) ||
      !validControls(queueBlocks, cpuRounds)) {
    throw new Error('worker requires a generated input and supported mode and delay');
  }

  const loop = sampleLoop ? monitorEventLoopDelay({ resolution: 10 }) : undefined;
  const digest = createHash('sha256');
  let bytes = 0;
  let writes = 0;
  let waits = 0;
  let firstMs;
  let peakPendingBytes = 0;
  let cpuMarker = 0;

  const sink = new Writable({
    highWaterMark: queueBlocks * blockBytes,
    write(chunk, _encoding, done) {
      setTimeout(() => {
        digest.update(chunk);
        for (let round = 0; round < cpuRounds; round += 1) {
          cpuMarker ^= createHash('sha256').update(chunk).digest()[0];
        }
        bytes += chunk.length;
        writes += 1;
        firstMs ??= performance.now() - started;
        done();
      }, delayMs);
    },
  });

  loop?.enable();
  const cpuStart = process.cpuUsage();
  const started = performance.now();
  try {
    const input = mode === 'buffered'
      ? [await readFile(inputPath)]
      : createReadStream(inputPath, { highWaterMark: blockBytes });

    for await (const block of blocks(input)) {
      const canContinue = sink.write(block);
      peakPendingBytes = Math.max(peakPendingBytes, sink.writableLength);
      if (!canContinue && mode !== 'unpaced') {
        waits += 1;
        await once(sink, 'drain');
      }
    }
    sink.end();
    await finished(sink, { cleanup: true });
  } catch (error) {
    sink.destroy();
    throw error;
  } finally {
    loop?.disable();
  }

  const totalMs = performance.now() - started;
  const cpuUsed = process.cpuUsage(cpuStart);
  const eventLoopSamples = loop?.count ?? 0;
  return {
    bytes, writes, waits, firstMs, totalMs,
    throughputMiBps: (bytes / MiB) / (totalMs / 1000),
    peakRssKiB: process.resourceUsage().maxRSS,
    peakPendingBytes,
    eventLoopSamples,
    eventLoopP99Ms: eventLoopSamples ? loop.percentile(99) / 1e6 : null,
    sha256: digest.digest('hex'),
    cpuMs: (cpuUsed.user + cpuUsed.system) / 1000,
    cpuMarker,
  };
}

function emit(record) {
  console.log(JSON.stringify(record));
}

function validControls(queueBlocks, cpuRounds) {
  return Number.isInteger(queueBlocks) && queueBlocks >= 1 && queueBlocks <= 16 &&
    Number.isInteger(cpuRounds) && cpuRounds >= 0 && cpuRounds <= 64;
}

async function run(trials, sampleLoop, queueBlocks, cpuRounds) {
  const runId = randomUUID();
  const dir = await mkdtemp(join(tmpdir(), 'streams-bench-'));
  try {
    const fixtures = new Map();
    for (const sizeMiB of sizesMiB) {
      const bytes = seededBytes(sizeMiB * MiB);
      const path = join(dir, `input-${sizeMiB}.bin`);
      await writeFile(path, bytes);
      fixtures.set(sizeMiB, { path, sha256: createHash('sha256').update(bytes).digest('hex') });
    }

    emit({
      type: 'environment', runId, node: process.version, platform: process.platform,
      release: release(), arch: process.arch, cpu: cpus()[0]?.model, logicalCpus: cpus().length,
      inputMiB: sizesMiB, sinkDelayMs: delaysMs, blockBytes,
      highWaterMark: queueBlocks * blockBytes, cpuRounds,
      trials, warmupsPerMode: 1, seed, eventLoopDelay: sampleLoop,
    });

    for (const sizeMiB of sizesMiB) {
      const { path, sha256 } = fixtures.get(sizeMiB);
      for (const delayMs of delaysMs) {
        for (let trial = 0; trial <= trials; trial += 1) {
          for (let position = 0; position < modes.length; position += 1) {
            const mode = modes[(trial + position) % modes.length];
            const identity = { runId, sizeMiB, delayMs, trial, mode };
            try {
              const result = JSON.parse(execFileSync(process.execPath, [
                fileURLToPath(import.meta.url), '--worker', mode, path, String(delayMs),
                sampleLoop ? 'loop' : 'no-loop', String(queueBlocks), String(cpuRounds),
              ], { encoding: 'utf8', timeout: 15_000 }));
              if (result.bytes !== sizeMiB * MiB || result.writes !== result.bytes / blockBytes ||
                  result.sha256 !== sha256) {
                throw new Error('incomparable work: length, block count or checksum differs');
              }
              if (trial !== 0) emit({ type: 'sample', ...identity, ...result });
            } catch (error) {
              emit({ type: 'failure', ...identity, reason: error.code ?? error.message });
              throw error;
            }
          }
        }
      }
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

const [arg = '5', ...options] = process.argv.slice(2);
if (arg === '--worker') {
  const [mode, inputPath, delayText, loopMode, queueBlocks, cpuRounds] = options;
  emit(await measure(mode, inputPath, Number(delayText), loopMode === 'loop',
    Number(queueBlocks), Number(cpuRounds)));
} else {
  const trials = Number(arg);
  const queueOption = options.find((option) => option.startsWith('--queue-blocks='));
  const cpuOption = options.find((option) => option.startsWith('--cpu-rounds='));
  const queueBlocks = Number(queueOption?.split('=')[1] ?? 4);
  const cpuRounds = Number(cpuOption?.split('=')[1] ?? 0);
  const sampleLoop = options.includes('--loop-delay');
  if (!Number.isInteger(trials) || trials < 1 || trials > 8 ||
      !validControls(queueBlocks, cpuRounds) ||
      options.length !== Number(sampleLoop) + Number(Boolean(queueOption)) + Number(Boolean(cpuOption))) {
    console.error('Usage: node benchmarks/13-stream-baselines.mjs [trials: 1–8] [--loop-delay] [--queue-blocks=1–16] [--cpu-rounds=0–64]');
    process.exitCode = 1;
  } else {
    await run(trials, sampleLoop, queueBlocks, cpuRounds);
  }
}
