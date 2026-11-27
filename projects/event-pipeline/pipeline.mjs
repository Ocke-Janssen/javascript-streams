import { createHash } from 'node:crypto';
import { createWriteStream } from 'node:fs';
import { link, mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { pipeline } from 'node:stream/promises';
import { createGzip } from 'node:zlib';

export const limits = Object.freeze({
  inputBytes: 8 * 1024 * 1024,
  lineBytes: 512,
  records: 100_000,
  deadlineMs: 15_000,
});

export class PipelineError extends Error {
  constructor(code, line) {
    super(code);
    this.code = code;
    if (line !== undefined) this.line = line;
  }
}

export async function* limitBytes(source, maxBytes, code, accepted = () => {}) {
  let bytes = 0;
  for await (const chunk of source) {
    bytes += chunk.length;
    if (bytes > maxBytes) throw new PipelineError(code);
    accepted(bytes);
    yield chunk;
  }
}

function readRecord(bytes, line, decoder) {
  if (bytes.at(-1) === 0x0d) bytes = bytes.subarray(0, -1);
  if (bytes.includes(0x0d)) throw new PipelineError('invalid_line_break', line);
  let text;
  try {
    text = decoder.decode(bytes);
  } catch {
    throw new PipelineError('invalid_utf8', line);
  }
  let record;
  try {
    record = JSON.parse(text);
  } catch {
    throw new PipelineError('invalid_json', line);
  }
  if (!record || !Number.isSafeInteger(record.id) || record.id < 1 ||
      !['info', 'debug', 'warn'].includes(record.level) ||
      typeof record.event !== 'string' || record.event.length > 80) {
    throw new PipelineError('invalid_record', line);
  }
  return { id: record.id, level: record.level, event: record.event };
}

async function* readRecords(source, maxLineBytes) {
  const pending = Buffer.alloc(maxLineBytes);
  const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
  let used = 0;
  let line = 1;
  for await (const chunk of source) {
    let offset = 0;
    while (offset < chunk.length) {
      const lf = chunk.indexOf(0x0a, offset);
      const end = lf === -1 ? chunk.length : lf;
      const length = end - offset;
      if (used + length > pending.length) {
        throw new PipelineError('line_limit', line);
      }
      chunk.copy(pending, used, offset, end);
      used += length;
      if (lf === -1) break;

      yield readRecord(pending.subarray(0, used), line, decoder);
      used = 0;
      line += 1;
      offset = lf + 1;
    }
  }
  if (used > 0) throw new PipelineError('incomplete_line', line);
}

export function newCounters() {
  return { readBytes: 0, recordsRead: 0, kept: 0, dropped: 0, gzipBytes: 0, sinkWaitMs: 0 };
}

async function removeDirectory(directory) {
  await rm(directory, { recursive: true, force: true });
}

export async function buildOutput({
  openSource, output, signal, policy = limits, counters = newCounters(),
  progress = async () => {}, beforeWrite, publish = link, removeTemporary = removeDirectory,
}) {
  signal?.throwIfAborted();
  const directory = await mkdtemp(join(dirname(output), '.event-pipeline-'));
  const partial = join(directory, 'output.ndjson.gz');
  const digest = createHash('sha256');
  try {
    await pipeline(
      openSource(),
      (source) => limitBytes(source, policy.inputBytes, 'input_limit', (bytes) => {
        counters.readBytes = bytes;
      }),
      async function* normalize(source, { signal: pipeSignal }) {
        for await (const record of readRecords(source, policy.lineBytes)) {
          if (counters.recordsRead >= policy.records) {
            throw new PipelineError('record_limit', counters.recordsRead + 1);
          }
          counters.recordsRead += 1;
          const keep = record.level !== 'debug';
          if (keep) counters.kept += 1;
          else counters.dropped += 1;
          if (counters.recordsRead === 1 || counters.recordsRead % 5000 === 0) {
            await progress({ ...counters }, pipeSignal);
          }
          if (keep) {
            const line = Buffer.from(JSON.stringify(record) + '\n');
            digest.update(line);
            yield line;
          }
        }
      },
      createGzip(),
      async function* writeGate(source, { signal: pipeSignal }) {
        for await (const chunk of source) {
          if (beforeWrite) {
            const started = performance.now();
            try {
              await beforeWrite(pipeSignal);
            } finally {
              counters.sinkWaitMs += performance.now() - started;
            }
          }
          counters.gzipBytes += chunk.length;
          yield chunk;
        }
      },
      createWriteStream(partial, { flags: 'wx' }),
      { signal },
    );
    signal?.throwIfAborted();
    await publish(partial, output);
  } finally {
    try {
      await removeTemporary(directory);
    } catch {
      counters.cleanupPending = true;
    }
  }
  return {
    ...counters, outputSha256: digest.digest('hex'), cleanupPending: counters.cleanupPending ?? false,
  };
}
