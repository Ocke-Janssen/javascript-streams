import { Readable, Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';

const records = [
  { id: 'a', level: 'warn' },
  { id: 'b', level: 'info' },
  { id: 'c', level: 'error' },
];
const rank = new Map([
  ['info', 0], ['warn', 1], ['error', 2],
]);
const minimum = rank.get('warn');

const filter = new Transform({
  writableObjectMode: true,
  readableObjectMode: false,

  transform(record, _encoding, done) {
    const level = rank.get(record?.level);
    if (level === undefined) {
      return done(new Error('unknown event level'));
    }
    if (level < minimum) return done();

    let line;
    try {
      const json = JSON.stringify(record);
      if (json === undefined) {
        throw new Error('event has no JSON representation');
      }
      line = Buffer.from(json + '\n', 'utf8');
    } catch (error) {
      return done(error);
    }
    done(null, line);
  },
});

await pipeline(Readable.from(records), filter, process.stdout);
