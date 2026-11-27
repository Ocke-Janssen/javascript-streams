import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { streamArray } from 'stream-json/streamers/stream-array.js';

const input = Readable.from([
  Buffer.from('[{"id":"001"},\n'),
  Buffer.from(' {"id":"002"}]\n'),
], { objectMode: false });

await pipeline(
  input,
  streamArray.withParserAsStream({
    maxDepth: 32,
    maxPackedLength: 1024,
  }),
  async function* encode(values) {
    for await (const { value } of values) {
      yield JSON.stringify(value) + '\n';
    }
  },
  process.stdout,
);
