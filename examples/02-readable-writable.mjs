import { createReadStream } from 'node:fs';
import { Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';

const inputPath = process.argv[2] ?? 'fixtures/events.ndjson';
const source = createReadStream(inputPath, { highWaterMark: 64 });
let bytes = 0;
let chunks = 0;

const sink = new Writable({
  write(chunk, _encoding, done) {
    bytes += chunk.length;
    chunks += 1;
    console.log(`sink write bytes=${chunk.length} buffer=${Buffer.isBuffer(chunk)}`);
    done();
  },
});

source.on('end', () => console.log('source end'));
sink.on('finish', () => console.log('sink finish'));

for (const [name, stream] of [['source', source], ['sink', sink]]) {
  stream.on('close', () => console.log(`${name} close`));
  stream.on('error', (error) => console.log(`${name} error ${error.code ?? error.name}`));
}

try {
  await pipeline(source, sink);
  console.log(`completed chunks=${chunks} bytes=${bytes}`);
} catch (error) {
  console.error(`failed ${error.code ?? error.name}`);
  process.exitCode = 1;
}
