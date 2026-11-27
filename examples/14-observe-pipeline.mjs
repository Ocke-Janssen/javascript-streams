import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { Transform, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { setTimeout as delay } from 'node:timers/promises';

const [mode = 'ok', ...options] = process.argv.slice(2);
const blind = options.includes('--blind');
const delayOption = options.find((option) => option.startsWith('--sink-ms='));
const delayText = delayOption?.slice('--sink-ms='.length);
const sinkMs = delayOption === undefined ? (mode === 'slow-sink' ? 90 : 6) : Number(delayText);
if (!['ok', 'slow-sink', 'slow-source', 'bad-record', 'abort'].includes(mode) ||
    options.length !== new Set(options).size ||
    options.some((option) => option !== '--blind' && option !== delayOption) ||
    (delayOption !== undefined && (!/^\d+$/.test(delayText) || !Number.isSafeInteger(sinkMs) || sinkMs > 1000))) {
  console.error('Usage: node examples/14-observe-pipeline.mjs [ok|slow-sink|slow-source|bad-record|abort] [--blind] [--sink-ms=0..1000]');
  process.exit(1);
}

const levels = ['info', 'debug', 'warn', 'info', 'warn'];
const operation = new AbortController();
const runId = 'demo-' + randomUUID();
const started = performance.now();
const metrics = {
  readBytes: 0, recordsRead: 0, processed: 0, dropped: 0, rejected: 0,
  peakSinkPendingRecords: 0, sourceWaitMs: 0, sinkWriteMs: 0,
};

function log(event, fields = {}) {
  console.log(JSON.stringify({
    runId, event, elapsedMs: Math.round(performance.now() - started), ...fields,
  }));
}

function recordChunk(chunk) {
  metrics.readBytes += chunk.length;
  metrics.recordsRead += 1;
  if (metrics.recordsRead === 1) log('source.first', { bytes: chunk.length });
  return { bytes: chunk, number: metrics.recordsRead };
}

async function* source({ signal }) {
  try {
    for (const [index, level] of levels.entries()) {
      if (mode === 'abort' && index === 1) {
        await delay(60_000, undefined, { signal });
      }
      if (mode === 'slow-source') {
        const since = performance.now();
        await delay(70, undefined, { signal });
        metrics.sourceWaitMs += performance.now() - since;
      }
      if (mode === 'bad-record' && index === 2) {
        if (metrics.processed === 0) await once(sink, 'first-processed', { signal });
        yield recordChunk(Buffer.from('{"level":\n'));
        return;
      }
      const line = JSON.stringify({ level, message: 'synthetic payload' }) + '\n';
      yield recordChunk(Buffer.from(line));
    }
  } finally {
    log('source.released');
  }
}

const decode = new Transform({
  objectMode: true,
  transform({ bytes, number }, _encoding, done) {
    let record;
    try {
      record = JSON.parse(bytes.toString('utf8'));
    } catch (cause) {
      metrics.rejected += 1;
      log('record.rejected', { stage: 'decode', record: number, reason: 'invalid_json' });
      const error = new Error('invalid record', { cause });
      error.code = 'INVALID_JSON';
      done(error);
      return;
    }

    if (record.level === 'debug') {
      metrics.dropped += 1;
      done();
    } else {
      done(null, record);
    }
  },
});

const sink = new Writable({
  objectMode: true,
  highWaterMark: 4,
  write(_record, _encoding, done) {
    const since = performance.now();
    delay(sinkMs, undefined, { signal: operation.signal })
      .then(() => {
        metrics.sinkWriteMs += performance.now() - since;
        metrics.processed += 1;
        const first = metrics.processed === 1;
        if (first) {
          log('sink.first', { processed: 1 });
          sink.emit('first-processed');
        }
        done();
        if (first && mode === 'abort') operation.abort();
      }, done);
  },
});

let status = 'ok';
let reason = 'completed';
log('run.start', blind ? {} : { mode });
const samples = setInterval(() => {
  const pending = sink.writableLength;
  if (pending > metrics.peakSinkPendingRecords) {
    metrics.peakSinkPendingRecords = pending;
    log('sink.queue', { pendingRecords: pending });
  }
}, 5);
try {
  await pipeline(source, decode, sink, { signal: operation.signal });
} catch (error) {
  if (operation.signal.aborted && error.code === 'ABORT_ERR') {
    status = 'aborted';
    reason = 'client_disconnect';
  } else {
    status = 'error';
    reason = error.code === 'INVALID_JSON' ? 'invalid_json' : 'unexpected';
  }
  process.exitCode = 1;
} finally {
  clearInterval(samples);
  log('run.end', {
    status, reason, ...metrics,
    sourceWaitMs: Math.round(metrics.sourceWaitMs),
    sinkWriteMs: Math.round(metrics.sinkWriteMs),
  });
}
