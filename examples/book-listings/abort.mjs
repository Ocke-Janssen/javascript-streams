import { Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { setTimeout as delay } from 'node:timers/promises';

const controller = new AbortController();

async function* records({ signal }) {
  try {
    yield 'first\n';
    console.log('source waiting');
    await delay(60_000, undefined, { signal });
    yield 'second\n';
  } finally {
    console.log('source cleanup');
  }
}

const sink = new Writable({
  write(chunk, _encoding, done) {
    console.log('written=' + chunk.toString().trimEnd());
    done();
    setImmediate(() => controller.abort());
  },
});

try {
  await pipeline(records, sink, {
    signal: controller.signal,
  });
} catch (error) {
  if (error.code !== 'ABORT_ERR') throw error;
  console.log('pipeline=' + error.code);
}
console.log('sinkDestroyed=' + sink.destroyed);
