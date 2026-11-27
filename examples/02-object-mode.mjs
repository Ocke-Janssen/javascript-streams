import { Readable, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';

const events = [
  { event: 'opened', bytes: 0 },
  { event: 'finished', bytes: 32 },
];

let index = 0;
const source = new Readable({
  objectMode: true,
  read() {
    if (index === events.length) {
      this.push(null);
      return;
    }

    const record = events[index];
    index += 1;
    this.push(record);
  },
});

let records = 0;
const sink = new Writable({
  objectMode: true,
  write(record, _encoding, done) {
    records += 1;
    console.log(`event=${record.event} bytesField=${record.bytes}`);
    done();
  },
});

await pipeline(source, sink);
console.log(`records=${records}`);
