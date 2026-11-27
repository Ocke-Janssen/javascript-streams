import { Readable, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';

const mode = process.argv[2] ?? 'ok';
if (!['ok', 'error', 'abort'].includes(mode)) {
  console.error('Usage: node examples/12-adapter-lifecycle.mjs [ok|error|abort]');
  process.exit(1);
}

const operation = new AbortController();
const records = [{ value: 'alpha' }, { value: 'beta' }];
let index = 0;

const nodeSource = new Readable({
  objectMode: true,
  highWaterMark: 1,
  read() {
    if (index === 0 || (index === 1 && mode !== 'abort')) {
      this.push(records[index++]);
    } else if (mode === 'ok') {
      this.push(null);
    }
  },
  destroy(error, done) {
    console.log('node source released');
    done(error);
  },
});
const released = new Promise((resolve) => nodeSource.once('close', resolve));

const webSource = Readable.toWeb(nodeSource, { strategy: { highWaterMark: 1 } });
const modeled = webSource.pipeThrough(new TransformStream({
  transform(record, controller) {
    if (mode === 'error' && record.value === 'beta') {
      throw new Error('web transform rejected beta');
    }
    controller.enqueue(record);
  },
}));
console.log(`source locked after pipeThrough=${webSource.locked}`);

const nodeInput = Readable.fromWeb(modeled, { objectMode: true, highWaterMark: 1 });
const webSink = new WritableStream({
  write(record) {
    console.log(`web sink received=${record.value}`);
    if (mode === 'abort') operation.abort();
  },
  close() {
    console.log('web sink closed');
  },
  abort(reason) {
    console.log('web sink aborted:', reason.code ?? reason.message);
  },
});

try {
  await pipeline(nodeInput, Writable.fromWeb(webSink, { objectMode: true }),
    { signal: operation.signal });
  console.log('result=ok');
} catch (error) {
  console.error('result=' + (error.code ?? error.message));
  process.exitCode = 1;
} finally {
  await released;
  console.log(`node source closed=${nodeSource.closed}`);
}
