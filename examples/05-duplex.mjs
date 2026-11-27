import { once } from 'node:events';
import { Duplex } from 'node:stream';
import { finished } from 'node:stream/promises';

const connection = new Duplex({
  allowHalfOpen: true,

  read() {
    this.push(Buffer.from('server reply'));
    this.push(null);
  },

  write(chunk, _encoding, done) {
    console.log(`write side got: ${chunk.toString('utf8')}`);
    done();
  },
});

const readableEnd = once(connection, 'end');
connection.on('data', (chunk) => {
  console.log(`read side got: ${chunk.toString('utf8')}`);
});

await readableEnd;
console.log(`readable end writableEnded=${connection.writableEnded}`);

const completed = finished(connection, { cleanup: true });
connection.end('client message');
await completed;
console.log(`closed=${connection.closed}`);
