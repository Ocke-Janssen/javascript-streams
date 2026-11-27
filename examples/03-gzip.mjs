import { createReadStream } from 'node:fs';
import { pipeline } from 'node:stream/promises';
import { createGzip } from 'node:zlib';

const inputPath = process.argv[2];
if (!inputPath) {
  console.error('Usage: node examples/03-gzip.mjs <input-file>');
  process.exitCode = 1;
} else {
  await pipeline(createReadStream(inputPath), createGzip(), process.stdout);
}
