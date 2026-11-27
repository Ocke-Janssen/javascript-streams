import { Readable, Transform, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { setTimeout as delay } from 'node:timers/promises';

const slow = process.argv.includes('--slow');
const maxBatchSize = 2;

const expand = new Transform({
  objectMode: true,
  ...(slow ? { readableHighWaterMark: 1 } : {}),
  transform(batch, _encoding, done) {
    if (!Array.isArray(batch) || batch.length > maxBatchSize) {
      done(new Error('expected a batch with at most two IDs'));
      return;
    }
    for (const id of batch) {
      const ready = this.push({ id });
      if (slow) console.error(`queued=${this.readableLength} ready=${ready}`);
    }
    done();
  },
});

const sink = new Writable({
  objectMode: true,
  ...(slow ? { highWaterMark: 1 } : {}),
  write(record, _encoding, done) {
    function confirm() {
      console.log(`id=${record.id}`);
      done();
    }
    if (slow) delay(20).then(confirm, done);
    else confirm();
  },
});

const groups = process.argv.includes('--oversize') ? [['a', 'b', 'c']] : [['a', 'b'], [], ['c']];
try {
  await pipeline(Readable.from(groups), expand, sink);
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
}
