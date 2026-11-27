import { once } from 'node:events';
import { Readable, Writable } from 'node:stream';
import { finished } from 'node:stream/promises';

const source = Readable.from(['ab', 'cd']);
const sink = new Writable({
  write(chunk, _encoding, done) {
    console.log('sink write', chunk.toString());
    source.unpipe(sink);
    done();
  },
});

for (const event of ['pipe', 'unpipe']) {
  sink.on(event, (input) => console.log('sink', event, input === source));
}

const detached = once(sink, 'unpipe');
source.pipe(sink);
await detached;

sink.end();
for await (const chunk of source) console.log('remaining', chunk);
await finished(sink, { cleanup: true });
