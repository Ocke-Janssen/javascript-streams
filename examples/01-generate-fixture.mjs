import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';

const records = Number(process.argv[2] ?? 1000);
if (!Number.isInteger(records) || records < 0 || records > 5_000_000) {
  console.error('Usage: node examples/01-generate-fixture.mjs [records: 0–5000000]');
  process.exit(1);
}

const line = Buffer.from('{"event":"sample","message":"Grüße"}\n', 'utf8');
const linesPerBlock = 1024;
const block = Buffer.concat(Array(linesPerBlock).fill(line));

function* blocks() {
  for (let left = records; left > 0; left -= linesPerBlock) {
    const lines = Math.min(left, linesPerBlock);
    yield block.subarray(0, lines * line.length);
  }
}

await pipeline(Readable.from(blocks(), { objectMode: false }), process.stdout);
