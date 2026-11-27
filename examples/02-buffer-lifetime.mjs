import { Writable } from 'node:stream';
import { finished } from 'node:stream/promises';

const [mode = 'view'] = process.argv.slice(2);
if (!['view', 'copy'].includes(mode)) {
  console.error('Usage: node examples/02-buffer-lifetime.mjs [view|copy]');
  process.exit(1);
}

const reusable = Buffer.alloc(64 * 1024);
reusable.write('record A');
const view = reusable.subarray(0, 8);
let submitted = view;
if (mode === 'copy') {
  submitted = Buffer.alloc(view.length);
  view.copy(submitted);
}

const { promise: mayRead, resolve: releaseRead } = Promise.withResolvers();
const sink = new Writable({
  write(chunk, _encoding, done) {
    mayRead.then(() => {
      console.log(`received=${JSON.stringify(chunk.toString())}`);
      done();
    }, done);
  },
});

console.log(`submitted=${JSON.stringify(submitted.toString())}`);
console.log(`chunkBytes=${submitted.byteLength} backingBytes=${submitted.buffer.byteLength}`);
sink.write(submitted);
reusable.write('record B');
releaseRead();
sink.end();
await finished(sink, { cleanup: true });
