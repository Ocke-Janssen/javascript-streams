import { Readable, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';

const chunks = [Buffer.from('ab'), Buffer.from('cd')];
const source = Readable.from(chunks, { objectMode: false });
let bytes = 0;
let firstMs;
const sink = new Writable({
  write(chunk, _encoding, done) {
    setTimeout(() => {
      bytes += chunk.byteLength;
      firstMs ??= performance.now() - started;
      done();
    }, 8);
  },
});

const started = performance.now();
await pipeline(source, sink);
const totalMs = performance.now() - started;
console.log('bytes=' + bytes);
console.log('firstMs=' + firstMs.toFixed(1));
console.log('totalMs=' + totalMs.toFixed(1));
