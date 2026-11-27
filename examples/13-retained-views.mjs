import { Readable, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';

const [mode = 'view'] = process.argv.slice(2);
if (!['view', 'copy'].includes(mode)) {
  console.error('Usage: node examples/13-retained-views.mjs [view|copy]');
  process.exit(1);
}

function* chunks() {
  for (let record = 1; record <= 16; record += 1) {
    yield Buffer.alloc(64 * 1024, record);
  }
}

const retained = [];
const sink = new Writable({
  write(chunk, _encoding, done) {
    const view = chunk.subarray(0, 8);
    retained.push(mode === 'copy' ? Uint8Array.from(view) : view);
    done();
  },
});

await pipeline(Readable.from(chunks(), { objectMode: false }), sink);

const backingStores = new Set(retained.map((view) => view.buffer));
const backingBytes = [...backingStores].reduce((sum, buffer) => sum + buffer.byteLength, 0);
const visibleBytes = retained.reduce((sum, view) => sum + view.byteLength, 0);
const byteSum = retained.reduce((sum, view) =>
  sum + view.reduce((total, byte) => total + byte, 0), 0);
console.log(`mode=${mode} records=${retained.length} pendingBytes=${sink.writableLength}`);
console.log(`visibleBytes=${visibleBytes} backingBytes=${backingBytes} byteSum=${byteSum}`);
