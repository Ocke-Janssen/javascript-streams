import { Readable, Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';

const minLevel = process.argv[2] ?? 'warn';
const rank = new Map([['info', 0], ['warn', 1], ['error', 2]]);
if (!rank.has(minLevel)) {
  console.error('Usage: node examples/05-transform-objects.mjs [info|warn|error] [--invalid|--empty]');
  process.exit(1);
}

const records = process.argv.includes('--empty') ? [] : [
  { id: 'a', level: 'warn' },
  { id: 'b', level: 'info' },
  { id: 'c', level: process.argv.includes('--invalid') ? 'unknown' : 'error' },
];

let seen = 0;
let kept = 0;
const filter = new Transform({
  writableObjectMode: true,

  transform(record, _encoding, done) {
    seen += 1;
    if (!rank.has(record?.level)) {
      done(new Error(`record ${seen}: unknown level`));
      return;
    }
    if (rank.get(record.level) < rank.get(minLevel)) {
      done();
      return;
    }

    let encoded;
    try {
      encoded = Buffer.from(JSON.stringify(record) + '\n', 'utf8');
    } catch (error) {
      done(error);
      return;
    }
    kept += 1;
    done(null, encoded);
  },

  flush(done) {
    console.error(`seen=${seen} kept=${kept} dropped=${seen - kept}`);
    done();
  },
});

try {
  await pipeline(Readable.from(records), filter, process.stdout);
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
}
