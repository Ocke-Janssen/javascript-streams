import { PassThrough, Readable, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';

const delayMs = Number(process.argv[2] ?? 10);
if (!Number.isInteger(delayMs) || delayMs < 0 || delayMs > 250) {
  console.error('Usage: node examples/04-readable-pipeline.mjs [0–250 ms]');
  process.exit(1);
}

let next = 1;
const source = new Readable({
  objectMode: true,
  highWaterMark: 2,
  read() {
    while (next <= 5) {
      const record = { id: next++ };
      const canContinue = this.push(record);
      console.log(`push ${record.id} length=${this.readableLength} ready=${canContinue}`);
      if (!canContinue) return;
    }
    this.push(null);
  },
});

const stage = new PassThrough({
  objectMode: true,
  writableHighWaterMark: 2,
  readableHighWaterMark: 2,
});

const completed = [];
const sink = new Writable({
  objectMode: true,
  highWaterMark: 1,
  write(record, _encoding, done) {
    console.log(
      `consume ${record.id} source.r=${source.readableLength} ` +
      `stage.w=${stage.writableLength} stage.r=${stage.readableLength} ` +
      `sink.w=${this.writableLength}`
    );
    setTimeout(() => {
      completed.push(record.id);
      done();
    }, delayMs);
  },
});

const started = performance.now();
await pipeline(source, stage, sink);
const elapsedMs = Math.round(performance.now() - started);
console.log(`completed=${completed.join(',')}`);
console.log(`elapsedMs=${elapsedMs}`);
