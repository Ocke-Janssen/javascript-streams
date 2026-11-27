import { randomUUID } from 'node:crypto';
import { Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';

const runId = randomUUID();
const started = performance.now();
function log(event, fields = {}) {
  const elapsedMs = Math.round(performance.now() - started);
  console.log(JSON.stringify({ runId, event, elapsedMs, ...fields }));
}

async function* source() {
  const bytes = Buffer.from('first\n');
  try {
    log('source.first', { bytes: bytes.length });
    yield bytes;
  } finally {
    log('source.released');
  }
}

const sink = new Writable({
  write(_chunk, _encoding, done) {
    setTimeout(() => {
      log('sink.first', { processed: 1 });
      done();
    }, 20);
  },
});

log('run.start');
let status = 'error';
try {
  await pipeline(source, sink);
  status = 'ok';
} catch {
  process.exitCode = 1;
} finally {
  log('run.end', { status });
}
