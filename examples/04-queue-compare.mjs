import { once } from 'node:events';
import { Writable } from 'node:stream';
import { finished } from 'node:stream/promises';

const mode = process.argv[2] ?? 'respect';
const delayMs = Number(process.argv[3] ?? 10);
if (!['respect', 'ignore'].includes(mode) ||
    !Number.isInteger(delayMs) || delayMs < 0 || delayMs > 250) {
  console.error('Usage: node examples/04-queue-compare.mjs [respect|ignore] [0–250 ms]');
  process.exit(1);
}

let processed = 0;
const sink = new Writable({
  highWaterMark: 8,
  write(chunk, _encoding, done) {
    setTimeout(() => {
      processed += chunk.length;
      done();
    }, delayMs);
  },
});

let peakBytes = 0;
let waits = 0;
const started = performance.now();
for (let i = 0; i < 8; i += 1) {
  const canContinue = sink.write(Buffer.alloc(4));
  peakBytes = Math.max(peakBytes, sink.writableLength);

  if (!canContinue && mode === 'respect') {
    waits += 1;
    await once(sink, 'drain');
  }
}
sink.end();
await finished(sink, { cleanup: true });
const elapsedMs = Math.round(performance.now() - started);

console.log(`mode=${mode} processed=${processed}B peak=${peakBytes}B waits=${waits}`);
console.log(`elapsedMs=${elapsedMs}`);
