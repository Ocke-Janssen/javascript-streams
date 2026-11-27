import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { parse } from 'csv-parse';
import { stringify } from 'csv-stringify';

const input = Readable.from([
  Buffer.from('id,message\n001,"first\n'),
  Buffer.from('second"\n002,done\n'),
], { objectMode: false });

await pipeline(
  input,
  parse({ columns: true, max_record_size: 1024 }),
  stringify({ header: true, columns: ['id', 'message'] }),
  process.stdout,
);
