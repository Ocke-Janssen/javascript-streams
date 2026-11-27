import { Readable, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';

const mode = process.argv[2] ?? 'complete';
if (!['complete', 'stall'].includes(mode)) {
  console.error('Usage: node examples/c-pipeline-stall.mjs [complete|stall]');
  process.exit(1);
}

const controller = new AbortController();
const entered = Promise.withResolvers();
let acknowledged = 0;
const sink = new Writable({
  objectMode: true,
  highWaterMark: 1,
  write(record, _encoding, done) {
    console.log('sink.enter=' + record);
    if (mode === 'stall') {
      // Intentional fault: this write never acknowledges the record.
      entered.resolve();
      return;
    }
    acknowledged += 1;
    done();
  },
});

const completed = pipeline(Readable.from([1]), sink, { signal: controller.signal });
try {
  if (mode === 'stall') {
    await Promise.race([entered.promise, completed]);
    console.log('acknowledged=' + acknowledged + ' pending=' + sink.writableLength +
      ' finished=' + sink.writableFinished);
    controller.abort();
  }
  await completed;
  console.log('acknowledged=' + acknowledged + ' finished=' + sink.writableFinished);
} catch (error) {
  if (mode !== 'stall' || error.code !== 'ABORT_ERR') throw error;
  console.error('pipeline=' + error.code + ' destroyed=' + sink.destroyed);
  process.exitCode = 1;
}
